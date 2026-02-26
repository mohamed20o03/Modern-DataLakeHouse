package com.abdelwahab.api_service.module.ingestion.service;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.abdelwahab.api_service.common.dto.IngestionMessage;
import com.abdelwahab.api_service.common.messaging.MessagePublisherService;
import com.abdelwahab.api_service.common.storage.StorageService;
import com.abdelwahab.api_service.jobstatus.service.JobStatusService;
import com.abdelwahab.api_service.module.ingestion.dto.UploadResponse;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Orchestrates the file upload and ingestion workflow.
 * <p>
 * Sequence:
 * <ol>
 *   <li>Generate job ID</li>
 *   <li>Write PENDING status to Redis</li>
 *   <li>Store file in object storage (MinIO / S3)</li>
 *   <li>Publish ingestion message to RabbitMQ</li>
 *   <li>Update status to QUEUED</li>
 * </ol>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IngestionService {

    private final StorageService storageService;
    private final JobStatusService jobStatusService;
    private final MessagePublisherService messagePublisherService;

    public UploadResponse upload(MultipartFile file, String userId, String projectId, String tableName) {
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException("file is required and must not be empty");
        }

        String jobId = UUID.randomUUID().toString();
        log.info("Starting upload: jobId={}, userId={}, projectId={}, file={}", jobId, userId, projectId, file.getOriginalFilename());

        jobStatusService.writeStatus(jobId, "PENDING");

        String storedPath = storageService.storeFile(file, jobId);
        log.debug("File stored: jobId={}, path={}", jobId, storedPath);

        IngestionMessage message = IngestionMessage.builder()
                .jobId(jobId)
                .userId(userId)
                .projectId(projectId)
                .storedPath(storedPath)
                .tableName(tableName)
                .fileName(file.getOriginalFilename())
                .fileSize(file.getSize())
                .contentType(file.getContentType())
                .timestamp(Instant.now().toString())
                .build();

        messagePublisherService.publishIngestionMessage(message);

        jobStatusService.writeStatus(jobId, "QUEUED");

        log.info("Upload complete: jobId={}, path={}", jobId, storedPath);

        return UploadResponse.builder()
                .jobId(jobId)
                .storedPath(storedPath)
                .userId(userId)
                .projectId(projectId)
                .tableName(tableName)
                .status("QUEUED")
                .checkStatusAt("/api/v1/ingestion/status/" + jobId)
                .build();
    }

    public Map<Object, Object> getJobStatus(String jobId) {
        return jobStatusService.readJobDetails(jobId);
    }

    public CompletableFuture<Map<Object, Object>> waitForCompletion(String jobId, Duration timeout) {
        return jobStatusService.waitForCompletion(jobId, timeout);
    }
}
