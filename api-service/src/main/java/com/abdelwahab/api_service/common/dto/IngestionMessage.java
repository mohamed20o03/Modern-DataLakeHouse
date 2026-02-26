package com.abdelwahab.api_service.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Message DTO for ingestion events published to RabbitMQ.
 * Workers consume this message to process uploaded files.
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
