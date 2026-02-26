package com.abdelwahab.ingestion_worker.engine.spark;

import com.abdelwahab.ingestion_worker.cache.SchemaCacheService;
import com.abdelwahab.ingestion_worker.engine.IngestionEngine;
import com.abdelwahab.ingestion_worker.engine.IngestionService;
import com.abdelwahab.ingestion_worker.status.JobStatusService;
import com.abdelwahab.ingestion_worker.storage.StorageConfig;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Spark implementation of {@link IngestionEngine}.
 *
 * <p><b>Responsibility:</b> owns the full {@link SparkSession} lifecycle.
 * The session is created in {@link #initialize} (the heavy step) and torn
 * down in {@link #close}. All Spark imports are confined to this class and
 * {@link SparkService} / {@link SparkStorageConfigurer} — nothing Spark-specific
 * leaks into {@code main} or the consumer layer.
 *
 * <p><b>Iceberg catalog registration:</b>
 * The session is configured with a named catalog {@code "iceberg"} backed by
 * the REST catalog implementation. Tables are addressed as
 * {@code iceberg.<namespace>.<table>} in Spark SQL and the DataFrame API.
 *
 * @see SparkStorageConfigurer — applies MinIO / S3 credentials to the builder
 * @see SparkService           — uses the session to read files and write Iceberg
 */
public class SparkEngine implements IngestionEngine {

    private static final Logger log = LoggerFactory.getLogger(SparkEngine.class);

    /** Iceberg REST catalog endpoint, e.g. {@code http://iceberg-rest:8181} */
    private final String icebergCatalogUri;

    /** Iceberg warehouse root, e.g. {@code s3://warehouse} */
    private final String icebergWarehouse;

    /** Null until {@link #initialize} is called. */
    private SparkSession spark;

    /**
     * Stores catalog coordinates; does NOT start Spark yet.
     * Spark starts only when {@link #initialize} is called.
     *
     * @param icebergCatalogUri REST catalog URI
     * @param icebergWarehouse  warehouse root URI
     */
    public SparkEngine(String icebergCatalogUri, String icebergWarehouse) {
        this.icebergCatalogUri = icebergCatalogUri;
        this.icebergWarehouse  = icebergWarehouse;
    }

    /**
     * Builds and starts the SparkSession, then returns a ready
     * {@link SparkService}.
     *
     * <p>Configuration applied here:
     * <ul>
     *   <li>Iceberg SQL extensions (MERGE INTO, time-travel, etc.)</li>
     *   <li>Named catalog {@code "iceberg"} → REST catalog → S3FileIO</li>
     *   <li>S3A / MinIO credentials via {@link SparkStorageConfigurer}</li>
     * </ul>
     *
     * @param jobStatusService Redis client passed through to {@link SparkService}
     * @param storageConfig    MinIO / S3 credentials and bucket names
     * @return initialised {@link SparkService} ready to call {@code ingest()} on
     */
    @Override
    public IngestionService initialize(JobStatusService jobStatusService, StorageConfig storageConfig,
                                       SchemaCacheService schemaCache) {
        log.info("Initializing SparkEngine — catalog={}, warehouse={}",
                icebergCatalogUri, icebergWarehouse);

        SparkSession.Builder builder = SparkSession.builder()
                .appName("ingestion-worker")
                // Enable Iceberg SQL extensions: MERGE INTO, ALTER TABLE, time-travel, etc.
                .config("spark.sql.extensions",
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                // Register a Spark catalog named "iceberg" backed by SparkCatalog
                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
                // Use the REST catalog implementation (talks to Iceberg REST server)
                .config("spark.sql.catalog.iceberg.type", "rest")
                .config("spark.sql.catalog.iceberg.uri", icebergCatalogUri)
                .config("spark.sql.catalog.iceberg.warehouse", icebergWarehouse)
                // Use S3FileIO so Iceberg reads/writes table data through S3A (MinIO-compatible)
                .config("spark.sql.catalog.iceberg.io-impl",
                        "org.apache.iceberg.aws.s3.S3FileIO");

        // Delegate S3A / MinIO credential injection to the configurer
        // so this class stays storage-backend-agnostic
        spark = SparkStorageConfigurer.apply(builder, storageConfig).getOrCreate();

        log.info("SparkSession started successfully");
        warmUp();
        return new SparkService(spark, jobStatusService, storageConfig, schemaCache);
    }

    /**
     * Runs a cheap no-op query to force lazy Spark internals to initialise
     * (executor allocation, Iceberg catalog connection, S3A filesystem init)
     * before the first real job arrives.
     *
     * <p>Without this, the first job pays a hidden ~5 s penalty on top of its
     * own execution time. The warm-up shifts that cost to startup, where it
     * is expected and does not affect perceived query latency.
     */
    private void warmUp() {
        log.info("Warming up Spark session...");
        long start = System.currentTimeMillis();
        try {
            spark.sql("SELECT 1").count();
            log.info("Spark warm-up completed in {} ms", System.currentTimeMillis() - start);
        } catch (Exception e) {
            log.warn("Spark warm-up failed (non-fatal): {}", e.getMessage());
        }
    }

    /**
     * Closes the SparkSession and releases all JVM / native resources.
     * Safe to call even if {@link #initialize} was never called.
     */
    @Override
    public void close() {
        if (spark != null) {
            try {
                spark.close(); // stops the SparkContext, releases executors
                log.info("SparkSession closed");
            } catch (Exception e) {
                log.error("Error closing SparkSession", e);
            }
        }
    }
}
