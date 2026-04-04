package com.abdelwahab.cdc_worker.maintenance;

import com.abdelwahab.cdc_worker.status.redis.AsyncRedisJobStatusService;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compacts small data files in an Iceberg table using {@code RewriteDataFiles}.
 *
 * <p><b>Strategy:</b> Bin-pack (default) — merges small files without re-ordering
 * data. This is the fastest compaction method and suitable for CDC workloads
 * where data ordering is less important than file consolidation.
 *
 * <p><b>Concurrency safety:</b> Iceberg's compaction is conflict-safe.
 * It creates a new snapshot atomically, so concurrent CDC writes are not blocked.
 * The compaction will retry on commit conflicts.
 *
 * @see org.apache.iceberg.actions.RewriteDataFiles
 */
public class CompactionService {

    private static final Logger log = LoggerFactory.getLogger(CompactionService.class);

    private final SparkSession spark;
    private final AsyncRedisJobStatusService statusService;

    public CompactionService(SparkSession spark, AsyncRedisJobStatusService statusService) {
        this.spark         = spark;
        this.statusService = statusService;
    }

    /**
     * Runs bin-pack compaction on the specified Iceberg table.
     *
     * @param tableName           fully-qualified table name (e.g. {@code iceberg.cdc_namespace.customers})
     * @param targetFileSizeBytes target file size in bytes (default 128 MiB)
     */
    public void compact(String tableName, long targetFileSizeBytes) {
        String jobId = "compaction:" + tableName;
        log.info("Starting compaction — table={}, targetFileSize={}MB",
                tableName, targetFileSizeBytes / (1024 * 1024));

        statusService.writeStatus(jobId, "PROCESSING",
                "Compacting " + tableName);

        try {
            // Load the Iceberg table via Spark SQL catalog
            Table table = loadTable(tableName);

            RewriteDataFiles.Result result = SparkActions.get(spark)
                    .rewriteDataFiles(table)
                    .option("target-file-size-bytes", String.valueOf(targetFileSizeBytes))
                    .option("min-input-files", "2")
                    .execute();

            int rewrittenGroups = result.rewriteResults().size();
            // Count total files
            int addedFiles = result.rewriteResults().stream()
                    .mapToInt(r -> r.addedDataFilesCount())
                    .sum();
            int rewrittenFiles = result.rewriteResults().stream()
                    .mapToInt(r -> r.rewrittenDataFilesCount())
                    .sum();

            log.info("Compaction completed — table={}, groups={}, rewritten={}, added={}",
                    tableName, rewrittenGroups, rewrittenFiles, addedFiles);

            statusService.writeStatus(jobId, "COMPLETED",
                    String.format("Compacted: %d files rewritten → %d files", rewrittenFiles, addedFiles));

        } catch (Exception e) {
            log.error("Compaction failed — table={}", tableName, e);
            statusService.writeStatus(jobId, "FAILED",
                    "Compaction error: " + e.getMessage());
            throw new RuntimeException("Compaction failed for " + tableName, e);
        }
    }

    /**
     * Loads an Iceberg Table object from the Spark catalog using Spark3Util.
     */
    private Table loadTable(String fullyQualifiedName) {
        try {
            return org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, fullyQualifiedName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load table: " + fullyQualifiedName, e);
        }
    }
}
