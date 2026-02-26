package com.abdelwahab.ingestion_worker.engine;

import com.abdelwahab.ingestion_worker.cache.SchemaCacheService;
import com.abdelwahab.ingestion_worker.status.JobStatusService;
import com.abdelwahab.ingestion_worker.storage.StorageConfig;

/**
 * Abstraction for a distributed processing engine (Spark, Flink, etc.).
 *
 * <p>Each implementation owns the full lifecycle of its compute session:
 * it is created in {@link #initialize}, used by the returned
 * {@link IngestionService}, and torn down in {@link #close}.
 *
 * <p>This interface is the key boundary that keeps
 * {@link com.abdelwahab.ingestion_worker.IngestionWorkerMain} engine-agnostic —
 * no Spark or Flink import ever leaks into {@code main} or the consumer layer.
 *
 * @see com.abdelwahab.ingestion_worker.engine.IngestionEngineFactory
 * @see com.abdelwahab.ingestion_worker.engine.spark.SparkEngine
 */
public interface IngestionEngine extends AutoCloseable {

    /**
     * Boots the engine and returns a ready-to-use {@link IngestionService}.
     *
     * <p>This is the heavy step: it starts the compute session (SparkContext,
     * FlinkEnv, etc.), connects to the Iceberg catalog, and configures S3/MinIO
     * credentials — all internally. Callers receive a lightweight service handle
     * without needing to know any engine internals.
     *
     * @param jobStatusService Redis client for reporting job progress
     * @param storageConfig    MinIO or S3 credentials and bucket names
     * @param schemaCache      cache for Iceberg table schemas (invalidated after each write)
     * @return a fully initialised {@link IngestionService} ready to call
     *         {@code ingest()} on
     */
    IngestionService initialize(JobStatusService jobStatusService, StorageConfig storageConfig,
                                SchemaCacheService schemaCache);

    /**
     * Shuts down the engine and releases all compute resources.
     *
     * <p>Implementations must close the underlying session (SparkSession,
     * StreamExecutionEnvironment, etc.) and free any thread pools or native
     * memory they hold. Called by the JVM shutdown hook in
     * {@link com.abdelwahab.ingestion_worker.IngestionWorkerMain}.
     */
    @Override
    void close();
}
