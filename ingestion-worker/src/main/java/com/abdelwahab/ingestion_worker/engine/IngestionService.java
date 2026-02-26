package com.abdelwahab.ingestion_worker.engine;

import com.abdelwahab.ingestion_worker.dto.IngestionMessage;

/**
 * Contract for the engine-specific ingestion logic.
 *
 * <p>Implementations read the uploaded file from object storage, transform it
 * into a columnar representation, and append it to the target Apache Iceberg
 * table.  Status updates (PROCESSING → COMPLETED / FAILED) are written to Redis
 * during execution.
 *
 * <p>Each implementation is tightly coupled to one compute engine
 * (Spark, Flink, DuckDB, etc.) but callers — the RabbitMQ consumer and
 * {@link com.abdelwahab.ingestion_worker.IngestionWorkerMain} — only depend on
 * this interface, so engines are swappable without touching consumer code.
 *
 * @see com.abdelwahab.ingestion_worker.engine.spark.SparkService
 */
public interface IngestionService {

    /**
     * Synchronously processes one ingestion job end-to-end.
     *
     * <p>The method blocks until the file has been fully written to the Iceberg
     * table (or an error is determined). The caller — typically a RabbitMQ
     * deliver callback — uses the return/throw to decide whether to ACK or NACK
     * the message.
     *
     * @param message job descriptor from the RabbitMQ message body; must contain
     *                at minimum {@code jobId}, {@code storedPath}, and
     *                {@code projectId}
     * @throws RuntimeException if any step of the ingestion pipeline fails;
     *                          the job status is set to FAILED before throwing
     */
    void ingest(IngestionMessage message);
}

