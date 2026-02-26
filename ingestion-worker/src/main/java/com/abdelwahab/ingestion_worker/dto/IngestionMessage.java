package com.abdelwahab.ingestion_worker.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data transfer object for ingestion job events consumed from RabbitMQ.
 *
 * <p><b>Producer:</b> {@code api-service} publishes this as a JSON object to the
 * {@code ingestion.queue} after a file is successfully uploaded to MinIO.
 *
 * <p><b>Consumer:</b> {@link com.abdelwahab.ingestion_worker.consumer.rabbitmq.RabbitMQConsumer}
 * deserialises the raw JSON body into this class using Jackson, then passes it
 * to {@link com.abdelwahab.ingestion_worker.engine.IngestionService#ingest}.
 *
 * <p><b>Schema contract:</b> both services must agree on the field names and types.
 * Changes here must be mirrored in the api-service {@code IngestionMessage} DTO.
 *
 * <p><b>Key fields for the engine:</b>
 * <ul>
 *   <li>{@code jobId}      — used as the Redis status key and for log correlation</li>
 *   <li>{@code storedPath} — relative S3/MinIO path to the uploaded file</li>
 *   <li>{@code projectId}  — becomes the Iceberg namespace</li>
 *   <li>{@code tableName}  — becomes the Iceberg table name (derived from
 *                            {@code fileName} if absent)</li>
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IngestionMessage {
    
    /**
     * Unique job identifier
     */
    private String jobId;
    
    /**
     * User identifier who initiated the upload
     */
    private String userId;
    
    /**
     * Project identifier
     */
    private String projectId;
    
    /**
     * Storage path where file is stored (e.g., "ingestion/jobId/filename.csv")
     */
    private String storedPath;
    
    /**
     * Optional target table name for the ingestion
     */
    private String tableName;
    
    /**
     * Original filename
     */
    private String fileName;
    
    /**
     * File size in bytes
     */
    private Long fileSize;
    
    /**
     * Content type (e.g., "text/csv", "application/json")
     */
    private String contentType;
    
    /**
     * Timestamp when the message was created (ISO-8601 format)
     */
    private String timestamp;
}
