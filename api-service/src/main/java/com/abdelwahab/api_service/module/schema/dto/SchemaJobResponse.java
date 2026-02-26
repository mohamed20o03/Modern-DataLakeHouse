package com.abdelwahab.api_service.module.schema.dto;

import lombok.Builder;
import lombok.Data;

/**
 * Response DTO for a schema retrieval job submission.
 */
@Data
@Builder
public class SchemaJobResponse {
    private String jobId;
    private String status;
    private String message;
    private String checkStatusAt;
}
