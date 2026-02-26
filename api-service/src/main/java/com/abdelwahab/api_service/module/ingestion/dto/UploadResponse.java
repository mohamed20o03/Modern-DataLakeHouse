package com.abdelwahab.api_service.module.ingestion.dto;

import lombok.Builder;
import lombok.Data;

/**
 * Response DTO for a successful file upload.
 */
@Data
@Builder
public class UploadResponse {
    private String jobId;
    private String storedPath;
    private String userId;
    private String projectId;
    private String tableName;
    private String status;
    private String checkStatusAt;
}
