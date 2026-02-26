package com.abdelwahab.query_worker.engine;

import com.abdelwahab.query_worker.dto.QueryMessage;

/**
 * Contract for executing queries against Iceberg tables.
 *
 * <p><b>Responsibility:</b> reads a query request (filters, aggregations, etc.),
 * executes it against the warehouse's Iceberg tables, and reports results
 * back via Redis and/or a results sink.
 *
 * <p>Implementations may use Apache Spark, DuckDB, or other query engines.
 */
public interface QueryService {

    /**
     * Executes one query request asynchronously.
     *
     * @param message job descriptor from RabbitMQ; must have {@code jobId},
     *                {@code source}, and {@code queryJson}
     * @throws RuntimeException wrapping any query / storage / execution error
     */
    void query(QueryMessage message);
}
