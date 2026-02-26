package com.abdelwahab.query_worker.engine;

import com.abdelwahab.query_worker.cache.SchemaCacheService;
import com.abdelwahab.query_worker.status.JobStatusService;
import com.abdelwahab.query_worker.storage.StorageConfig;

/**
 * Contract for creating and managing a query execution engine.
 *
 * <p>Implementations may use Apache Spark, DuckDB, Presto, or other engines
 * to execute queries against Iceberg tables in the warehouse.
 *
 * <p><b>Lifecycle:</b>
 * <ol>
 *   <li>Create an instance (engine not started yet).</li>
 *   <li>Call {@link #initialize} to start the engine and get a {@link QueryService}.</li>
 *   <li>Use the QueryService to execute queries.</li>
 *   <li>Call {@link #close} to release resources.</li>
 * </ol>
 */
public interface QueryEngine {

    /**
     * Initializes and starts the query engine with the given dependencies.
     *
     * @param jobStatusService for reporting job progress to Redis
     * @param storageConfig    storage credentials and configuration
     * @param schemaCache      cache for Iceberg table schemas
     * @return a ready-to-use QueryService instance
     */
    QueryService initialize(JobStatusService jobStatusService, StorageConfig storageConfig,
                            SchemaCacheService schemaCache);

    /**
     * Closes the engine and releases all resources.
     * Safe to call even if {@link #initialize} was never called.
     */
    void close();
}
