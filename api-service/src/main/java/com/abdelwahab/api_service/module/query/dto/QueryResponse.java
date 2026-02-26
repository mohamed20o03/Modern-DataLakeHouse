package com.abdelwahab.api_service.module.query.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response DTO for query job submission.
 * Based on API spec: POST /query response
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryResponse {
    
    /**
     * Unique job identifier (UUID)
     */
    private String jobId;
    
    /**
     * Initial status (always "queued")
     */
    private String status;
    
    /**
     * Status message
     */
    private String message;
    
    /**
     * URL to check status
     */
    private String checkStatusAt;
}
