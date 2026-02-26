package com.abdelwahab.api_service.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Message DTO for query events published to RabbitMQ.
 * Workers consume this message to execute queries against Iceberg tables.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryMessage {
    
    /**
     * Unique job identifier
     */
    private String jobId;
    
    /**
     * Source table identifier: "{projectId}.{tableName}" or jobId reference
     */
    private String source;
    
    /**
     * Full query JSON as string (contains select, filters, groupBy, orderBy, limit)
     */
    private String queryJson;
    
    /**
     * Timestamp when the message was created (ISO-8601 format)
     */
    private String timestamp;
}
