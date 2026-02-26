package com.abdelwahab.ingestion_worker.engine.spark;

import com.abdelwahab.ingestion_worker.cache.SchemaCacheService;
import com.abdelwahab.ingestion_worker.dto.IngestionMessage;
import com.abdelwahab.ingestion_worker.engine.IngestionService;
import com.abdelwahab.ingestion_worker.status.JobStatusService;
import com.abdelwahab.ingestion_worker.storage.StorageConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Spark implementation of {@link IngestionService}.
 *
 * <p><b>Responsibility:</b> reads an uploaded file from object storage (MinIO / S3),
 * infers its schema, and appends the data to an Apache Iceberg table in the
 * warehouse. Job progress is reported to Redis via {@link JobStatusService}.
 *
 * <p><b>Data flow:</b>
 * <pre>
 *   RabbitMQ message
 *        │
 *        ▼
 *   SparkService.ingest(message)
 *        │
 *        ├─ 1. Redis  ← PROCESSING
 *        ├─ 2. s3a://{uploadsBucket}/{storedPath}  ──► Spark DataFrame
 *        ├─ 3. Resolve target table: iceberg.{projectId}.{tableName}
 *        ├─ 4. DataFrame.writeTo(icebergTable).append()
 *        └─ 5. Redis  ← COMPLETED | FAILED
 * </pre>
 *
 * <p><b>Supported file formats</b> (detected from {@code contentType} or extension):
 * CSV, JSON, Parquet, Avro.
 *
 * <p><b>Iceberg table naming:</b> {@code iceberg.<projectId>.<tableName>}
 * — if {@code tableName} is absent it is derived from the filename.
 *
 * @see SparkEngine            — creates the SparkSession and instantiates this class
 * @see SparkStorageConfigurer — injects S3/MinIO credentials into the session
 */
public class SparkService implements IngestionService {

    private static final Logger log = LoggerFactory.getLogger(SparkService.class);

    /** Iceberg catalog name as registered in {@code spark.sql.catalog.<name>} */
    private static final String ICEBERG_CATALOG = "iceberg";

    /**
     * Target size per Spark partition when reading — 64 MiB.
     * Used to derive a partition count proportional to the actual file size,
     * avoiding both under-parallelism (1 tiny task) and over-parallelism
     * (200 empty shuffle tasks from the Spark default).
     */
    private static final long PARTITION_SIZE_BYTES   = 64L  * 1024 * 1024; // 64 MiB

    /**
     * Target size for each Iceberg output data file — 128 MiB.
     * Passed to the Iceberg writer so it rolls to a new file at this threshold,
     * keeping the table's file count healthy for downstream query engines.
     */
    private static final long TARGET_FILE_SIZE_BYTES = 128L * 1024 * 1024; // 128 MiB

    /** Lower bound: always produce at least one partition/file. */
    private static final int  MIN_PARTITIONS         = 1;

    /**
     * Upper bound for an ingestion worker processing a single file.
     * A single local Spark instance rarely benefits from more than 8 partitions
     * for this workload; exceeding this wastes scheduler overhead.
     */
    private static final int  MAX_PARTITIONS         = 8;

    private final SparkSession spark;
    private final JobStatusService jobStatusService;
    private final SchemaCacheService schemaCache;
    private final StorageConfig storageConfig;

    /**
     * Constructs a ready-to-use SparkService.
     *
     * <p>Called by {@link SparkEngine#initialize} once the SparkSession is fully
     * configured (Iceberg extensions + S3A credentials already applied).
     *
     * @param spark            active SparkSession with Iceberg catalog and S3A configured
     * @param jobStatusService async Redis client for reporting job progress
     * @param storageConfig    storage credentials and bucket names
     */
    public SparkService(SparkSession spark,
                        JobStatusService jobStatusService,
                        StorageConfig storageConfig,
                        SchemaCacheService schemaCache) {
        this.spark = spark;
        this.jobStatusService = jobStatusService;
        this.schemaCache = schemaCache;
        this.storageConfig = storageConfig;

        log.info("SparkService initialized — uploads bucket: {}, warehouse bucket: {}",
                storageConfig.getUploadsBucket(), storageConfig.getWarehouseBucket());
    }

    /**
     * Ingests one uploaded file into its target Iceberg table.
     *
     * <p><b>Steps (to be implemented):</b>
     * <ol>
     *   <li>Mark job {@code PROCESSING} in Redis.</li>
     *   <li>Build the S3A source URI:
     *       {@code "s3a://" + storageConfig.getUploadsBucket() + "/" + message.getStoredPath()}.</li>
     *   <li>Detect file format from {@code message.getContentType()} or filename extension.</li>
     *   <li>Read into a DataFrame with schema inference.</li>
     *   <li>Resolve the Iceberg table: {@code iceberg.<projectId>.<tableName>}.</li>
     *   <li>Append with {@code df.writeTo(icebergTable).append()} — atomic Iceberg commit.</li>
     *   <li>Mark job {@code COMPLETED} (or {@code FAILED} on any exception).</li>
     * </ol>
     *
     * @param message job descriptor from RabbitMQ; must have {@code jobId},
     *                {@code storedPath}, {@code projectId}
     * @throws RuntimeException wrapping any Spark / S3 / Iceberg error
     */
    @Override
    public void ingest(IngestionMessage message) {
        String jobId = message.getJobId();
        log.info("Starting ingestion for jobId: {}", jobId);

        try {
            // 1. Mark job as PROCESSING in Redis
            jobStatusService.writeStatus(jobId, "PROCESSING", null);

            // 2. Build the S3A source URI from the uploads bucket and stored path
            String sourceUri = "s3a://" + storageConfig.getUploadsBucket() + "/" + message.getStoredPath();
            log.info("Reading file from: {}", sourceUri);

            // 3. Detect file format and read into a DataFrame with schema inference
            Dataset<Row> df = readDataFrame(sourceUri, message);
            log.info("Schema inferred: {}", df.schema().treeString());

            // ── Optimisation 1: right-size shuffle partitions ──────────────────
            // Spark's default (200) is designed for large cluster shuffles; for a
            // single-file ingestion it produces hundreds of empty tasks.  We derive
            // a count from the reported file size so small files stay cheap and
            // large files still get parallelism.
            int partitions = computeOptimalPartitions(message.getFileSize());
            log.info("Partition count for jobId {}: {} (fileSize={})",
                    jobId, partitions, message.getFileSize());
            spark.conf().set("spark.sql.shuffle.partitions", String.valueOf(partitions));

            // ── Optimisation 2: sanitize column names ──────────────────────────
            // CSV / JSON headers often contain spaces, dots, or special chars that
            // Parquet and Iceberg writers reject.  Normalise them early so the write
            // never fails on a bad column name.
            df = sanitizeColumnNames(df);

            // ── Optimisation 3: coalesce to target partition count ─────────────
            // Avoids writing many tiny Iceberg data files (one per task) when the
            // reader opens more splits than needed.  coalesce() merges partitions
            // locally without a full shuffle, so it is cheap.
            df = df.coalesce(partitions);

            // 4. Resolve target Iceberg table: iceberg.<projectId>.<tableName>
            String tableName = resolveTableName(message);
            String icebergTable = ICEBERG_CATALOG + "." + message.getProjectId() + "." + tableName;
            log.info("Writing to Iceberg table: {}", icebergTable);

            // Ensure the namespace (database) exists before writing
            spark.sql("CREATE NAMESPACE IF NOT EXISTS " + ICEBERG_CATALOG + "." + message.getProjectId());

            // ── Optimisation 4: target file size hint to the Iceberg writer ────
            // Instructs Iceberg to roll to a new data file at TARGET_FILE_SIZE_BYTES
            // rather than writing one file per task.  Keeps the table file count
            // predictable regardless of how many partitions Spark uses.
            // 5. Append data to Iceberg table (creates on first write, appends thereafter).
            //    Check table existence first to decide between create() and append().
            //    append() preserves existing rows; mergeSchema handles new columns.
            boolean tableExists = spark.catalog().tableExists(icebergTable);

            if (tableExists) {
                log.info("Table {} exists — appending", icebergTable);

                // ── Schema evolution (bi-directional) ──────────────────────────
                // Forward:  incoming data has NEW columns the table lacks
                //           → ALTER TABLE ADD COLUMN so Iceberg accepts the write.
                // Backward: table has columns the incoming data lacks
                //           → add typed nulls to the DataFrame.
                StructType targetSchema = spark.read()
                        .format("iceberg").load(icebergTable)
                        .limit(0).schema();

                // Forward: evolve the Iceberg table schema for any new columns
                Set<String> existingCols = new HashSet<>(Arrays.asList(
                        spark.read().format("iceberg").load(icebergTable).limit(0).columns()));
                for (StructField field : df.schema().fields()) {
                    if (!existingCols.contains(field.name())) {
                        String sparkType = field.dataType().simpleString();
                        log.info("Schema evolution: adding column '{}' ({}) to {}",
                                field.name(), sparkType, icebergTable);
                        spark.sql(String.format("ALTER TABLE %s ADD COLUMN %s %s",
                                icebergTable, field.name(), sparkType));
                    }
                }

                // Backward: fill missing columns with typed nulls
                java.util.List<String> dfCols = Arrays.asList(df.columns());
                for (StructField field : targetSchema.fields()) {
                    if (!dfCols.contains(field.name())) {
                        log.info("Adding missing column '{}' as null ({})",
                                field.name(), field.dataType().simpleString());
                        df = df.withColumn(field.name(),
                                functions.lit(null).cast(field.dataType()));
                    }
                }

                df.writeTo(icebergTable)
                  .option("write.target-file-size-bytes", String.valueOf(TARGET_FILE_SIZE_BYTES))
                  .option("mergeSchema", "true")
                  .append();
            } else {
                log.info("Table {} does not exist — creating", icebergTable);
                df.writeTo(icebergTable)
                  .option("write.target-file-size-bytes", String.valueOf(TARGET_FILE_SIZE_BYTES))
                  .create();
            }

            // 6. Invalidate the schema cache BEFORE marking COMPLETED.
            //    This prevents a race condition where a client sees COMPLETED,
            //    fires a schema request, and gets served the stale cached schema
            //    before the DEL has executed.
            schemaCache.invalidate(icebergTable);

            // 7. Mark job as COMPLETED in Redis — block to guarantee persistence
            jobStatusService.writeStatus(jobId, "COMPLETED", "Ingested into " + icebergTable).join();

            log.info("Ingestion completed for jobId: {}", jobId);

        } catch (Exception e) {
            log.error("Failed to ingest jobId: {}", jobId, e);
            jobStatusService.writeStatus(jobId, "FAILED", e.getMessage()).join();
            throw new RuntimeException("Ingestion failed for jobId: " + jobId, e);
        }
    }

    /**
     * Reads the source file into a Spark DataFrame, auto-detecting the format
     * from the {@code contentType} field or the file extension as a fallback.
     *
     * @param sourceUri S3A URI pointing to the uploaded file
     * @param message   job descriptor carrying content-type and filename hints
     * @return DataFrame with schema inferred from the file content
     */
    private Dataset<Row> readDataFrame(String sourceUri, IngestionMessage message) {

        String contentType = message.getContentType() != null ? message.getContentType().toLowerCase() : "";
        String fileName    = message.getFileName()    != null ? message.getFileName().toLowerCase()    : "";

        if (contentType.contains("parquet") || fileName.endsWith(".parquet")) {
            log.info("Detected format: Parquet");
            return spark.read().parquet(sourceUri);

        } else if (contentType.contains("avro") || fileName.endsWith(".avro")) {
            log.info("Detected format: Avro");
            return spark.read().format("avro").load(sourceUri);

        } else if (contentType.contains("json") || fileName.endsWith(".json")) {
            log.info("Detected format: JSON");
            return spark.read().option("multiline", "true").json(sourceUri);

        } else {
            // Default: CSV (covers text/csv, application/octet-stream, unknown)
            log.info("Detected format: CSV (default)");
            return spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(sourceUri);
        }
    }

    /**
     * Derives the number of Spark partitions proportional to the file size.
     *
     * <ul>
     *   <li>Unknown size → {@link #MIN_PARTITIONS} (safest default).</li>
     *   <li>Computed value is clamped to [{@link #MIN_PARTITIONS}, {@link #MAX_PARTITIONS}]
     *       so a 1 GB file doesn't spin up hundreds of tasks on a single JVM.</li>
     * </ul>
     *
     * @param fileSize reported byte size from the ingestion message; may be null
     * @return partition count in [{@code MIN_PARTITIONS}, {@code MAX_PARTITIONS}]
     */
    private int computeOptimalPartitions(Long fileSize) {
        if (fileSize == null || fileSize <= 0) {
            return MIN_PARTITIONS;
        }
        int computed = (int) Math.ceil((double) fileSize / PARTITION_SIZE_BYTES);
        return Math.max(MIN_PARTITIONS, Math.min(MAX_PARTITIONS, computed));
    }

    /**
     * Renames every column whose name contains characters illegal in Parquet /
     * Iceberg ({@code spaces}, {@code .}, {@code ,}, {@code ;}, etc.).
     *
     * <p>The sanitised name is trimmed, lower-cased, and has every run of
     * non-alphanumeric characters replaced by a single underscore.
     *
     * @param df source DataFrame (may have raw CSV / JSON headers)
     * @return new DataFrame with clean column names (no-op if all names are already clean)
     */
    private Dataset<Row> sanitizeColumnNames(Dataset<Row> df) {
        Dataset<Row> result = df;
        for (String col : df.columns()) {
            String sanitized = col.trim()
                                  .replaceAll("[^a-zA-Z0-9_]+", "_")
                                  .replaceAll("^_|_$", "")   // strip leading/trailing underscores
                                  .toLowerCase();
            if (!sanitized.equals(col)) {
                log.debug("Renaming column '{}' → '{}'", col, sanitized);
                result = result.withColumnRenamed(col, sanitized);
            }
        }
        return result;
    }

    /**
     * Resolves the Iceberg table name from the message.
     * Uses {@code tableName} if present; otherwise derives it from the filename
     * by stripping the extension and sanitising non-alphanumeric characters.
     *
     * @param message job descriptor
     * @return a valid Iceberg table name
     */
    private String resolveTableName(IngestionMessage message) {
        if (message.getTableName() != null && !message.getTableName().isBlank()) {
            return message.getTableName();
        }
        // Derive from filename: "my-data.csv" → "my_data"
        String base = message.getFileName() != null ? message.getFileName() : "table_" + message.getJobId();
        int dotIndex = base.lastIndexOf('.');
        if (dotIndex > 0) {
            base = base.substring(0, dotIndex);
        }
        return base.replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();
    }
}

