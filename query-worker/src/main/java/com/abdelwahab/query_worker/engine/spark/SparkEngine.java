package com.abdelwahab.query_worker.engine.spark;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abdelwahab.query_worker.cache.SchemaCacheService;
import com.abdelwahab.query_worker.engine.QueryEngine;
import com.abdelwahab.query_worker.engine.QueryService;
import com.abdelwahab.query_worker.status.JobStatusService;
import com.abdelwahab.query_worker.storage.StorageConfig;
import com.abdelwahab.query_worker.streaming.ResultStreamPublisher;

/**
 * Apache Spark implementation of {@link QueryEngine}.
 *
 * <p><b>Responsibility:</b> creates and manages a SparkSession configured for
 * querying Iceberg tables in the warehouse, then returns a {@link SparkService}
 * for executing individual queries.
 *
 * <p><b>Lifecycle:</b>
 * <ol>
 *   <li>Constructor stores Iceberg catalog coordinates but does not start Spark.</li>
 *   <li>{@link #initialize} builds the SparkSession with Iceberg extensions and credentials.</li>
 *   <li>{@link #close} releases the SparkSession and all resources.</li>
 * </ol>
 *
 * @see SparkService          — handles individual query execution
 * @see SparkStorageConfigurer — injects storage credentials
 */
public class SparkEngine implements QueryEngine {
    
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

    @Override
    public QueryService initialize(JobStatusService jobStatusService, StorageConfig storageConfig,
                                   SchemaCacheService schemaCache, ResultStreamPublisher streamPublisher) {
        log.info("Initializing SparkEngine — catalog={}, warehouse={}",
                icebergCatalogUri, icebergWarehouse);
        
        SparkSession.Builder builder = SparkSession.builder()
            .appName("query-worker")
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
                    "org.apache.iceberg.aws.s3.S3FileIO")
            // Disable Iceberg's CachingCatalog for this worker.
            // By default CachingCatalog wraps RESTCatalog with a 30-minute TTL, which
            // causes spark.read().format("iceberg").load() to return a stale snapshot
            // and stale schema even after the ingestion-worker has added new columns.
            // With cache-enabled=false every table load goes straight to the REST
            // server (one HTTP GET, ~5 ms on the Docker bridge network), so schema
            // evolution is visible immediately — no REFRESH TABLE needed.
            .config("spark.sql.catalog.iceberg.cache-enabled", "false");

        // Delegate S3A / MinIO credential injection to the configurer
        // so this class stays storage-backend-agnostic
        spark = SparkStorageConfigurer.apply(builder, storageConfig).getOrCreate();

        log.info("SparkSession started successfully");
        warmUp();
        return new SparkService(spark, jobStatusService, storageConfig, schemaCache, streamPublisher);
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
