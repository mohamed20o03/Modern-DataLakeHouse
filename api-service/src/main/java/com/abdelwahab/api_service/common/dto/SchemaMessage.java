package com.abdelwahab.api_service.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Message DTO for schema retrieval events published to RabbitMQ.
 * Workers consume this message to retrieve table schema from Iceberg catalog.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SchemaMessage {
    
    /**
     * Unique job identifier
     */
    private String jobId;
    
    /**
     * Source table identifier: "{projectId}.{tableName}"
     */
    private String source;
    
    /**
     * Timestamp when the message was created (ISO-8601 format)
     */
    private String timestamp;
}
