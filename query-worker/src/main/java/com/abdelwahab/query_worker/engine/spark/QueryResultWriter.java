package com.abdelwahab.query_worker.engine.spark;

import com.abdelwahab.query_worker.dto.query.QueryResult;
import com.abdelwahab.query_worker.storage.StorageConfig;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Writes a query-result {@link Dataset} to MinIO as a Parquet file, or returns
 * the rows inline when the result set is small enough to skip the MinIO write.
 *
 * <p><b>Responsibility (SRP):</b> only handles result persistence.
 * Query building and status updates live elsewhere.
 *
 * <p><b>Inline vs external strategy:</b>
 * <ul>
 *   <li>{@code rowCount <= INLINE_THRESHOLD} → rows embedded in Redis directly;
 *       no Parquet file written, {@code resultPath} is {@code null}.</li>
 *   <li>{@code rowCount > INLINE_THRESHOLD} → Parquet written to MinIO at
 *       {@code wh/{projectId}/queries/query_{timestamp}/result.parquet}.</li>
 * </ul>
 *
 * <p><b>Caching strategy:</b> the Dataset is cached before any action so that
 * the inline-row collection and (when needed) the Parquet write share the same
 * computed partitions instead of triggering two independent Spark jobs.
 *
 * @see SparkService — the orchestrator that calls this writer
 */
public class QueryResultWriter {

    private static final Logger log = LoggerFactory.getLogger(QueryResultWriter.class);

    private static final DateTimeFormatter TIMESTAMP_FMT =
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    /**
     * Row-count threshold below which results are returned inline (embedded in
     * Redis) and the MinIO Parquet write is skipped entirely.
     * Saves ~500 ms of Parquet serialisation + S3 PUT overhead per small query.
     */
    private static final int INLINE_THRESHOLD = 1_000;

    private final SparkSession spark;
    private final StorageConfig storageConfig;

    public QueryResultWriter(SparkSession spark, StorageConfig storageConfig) {
        this.spark         = spark;
        this.storageConfig = storageConfig;
    }

    /**
     * Persists the result Dataset and returns metadata.
     *
     * <p>For small results ({@code rowCount <= } {@value #INLINE_THRESHOLD}) the rows
     * are embedded directly — no MinIO write, {@code resultPath} is {@code null}.
     * For large results a Parquet file is written to MinIO as before.
     *
     * @param result    computed result Dataset (already filtered / aggregated / limited)
     * @param projectId project identifier used to construct the output path
     * @return {@link QueryResult} with path, row count, file size, and inline rows
     */
    public QueryResult write(Dataset<Row> result, String projectId) {

        // Cache before any action so collect + write share the same partitions
        result.cache();

        List<Row> inlineRows = result.collectAsList();
        String[]  columns    = result.columns();
        long      rowCount   = inlineRows.size();

        // ── Optimisation #5: skip MinIO write for small results ───────────────
        // Rows already collected above; if the set is small enough, embed them
        // directly in Redis and return immediately without touching MinIO.
        if (rowCount <= INLINE_THRESHOLD) {
            result.unpersist();
            log.info("Inline result — rowCount={} (≤ {}), skipping MinIO write",
                    rowCount, INLINE_THRESHOLD);
            return QueryResult.builder()
                    .resultPath(null)
                    .rowCount(rowCount)
                    .fileSizeBytes(0L)
                    .resultData(toRowMaps(inlineRows, columns))
                    .build();
        }

        // ── Large result: write Parquet to MinIO ──────────────────────────────
        String timestamp    = LocalDateTime.now().format(TIMESTAMP_FMT);
        String relativePath = "wh/" + projectId + "/queries/query_" + timestamp + "/result.parquet";
        String s3aPath      = "s3a://" + storageConfig.getWarehouseBucket() + "/" + relativePath;

        log.info("Large result (rowCount={}) — writing Parquet to: {}", rowCount, s3aPath);

        result.coalesce(1)
              .write()
              .mode(SaveMode.Overwrite)
              .parquet(s3aPath);

        result.unpersist();

        long fileSizeBytes = resolveFileSize(s3aPath);

        log.info("Result written — rowCount={}, sizeBytes={}, path={}",
                rowCount, fileSizeBytes, relativePath);

        return QueryResult.builder()
                .resultPath(relativePath)
                .rowCount(rowCount)
                .fileSizeBytes(fileSizeBytes)
                .resultData(toRowMaps(inlineRows, columns))
                .build();
    }

    // ── Private helpers ────────────────────────────────────────────────────────

    /**
     * Converts Spark {@link Row} objects to plain Java maps for JSON serialisation.
     * Column order is preserved via {@link LinkedHashMap}.
     */
    private List<Map<String, Object>> toRowMaps(List<Row> rows, String[] columns) {
        return rows.stream()
                .map(row -> {
                    Map<String, Object> map = new LinkedHashMap<>();
                    for (int i = 0; i < columns.length; i++) {
                        map.put(columns[i], row.get(i));
                    }
                    return map;
                })
                .collect(Collectors.toList());
    }

    /**
     * Resolves the total byte size of the written path using the Hadoop
     * {@link FileSystem} API (works for both S3A and local FS).
     * Returns {@code 0} on any error rather than failing the job.
     */
    private long resolveFileSize(String s3aPath) {
        try {
            Path         hadoopPath = new Path(s3aPath);
            FileSystem   fs         = hadoopPath.getFileSystem(
                    spark.sparkContext().hadoopConfiguration());
            ContentSummary summary  = fs.getContentSummary(hadoopPath);
            return summary.getLength();
        } catch (Exception e) {
            log.warn("Could not resolve file size for '{}': {}", s3aPath, e.getMessage());
            return 0L;
        }
    }
}
