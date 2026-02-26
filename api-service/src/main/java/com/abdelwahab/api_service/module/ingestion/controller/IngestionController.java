package com.abdelwahab.api_service.module.ingestion.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;
import org.springframework.web.multipart.MultipartFile;

import com.abdelwahab.api_service.module.ingestion.dto.UploadResponse;
import com.abdelwahab.api_service.module.ingestion.service.IngestionService;

import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * REST controller for data ingestion operations.
 * Handles HTTP only — all workflow logic lives in {@link IngestionService}.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/ingestion")
@RequiredArgsConstructor
public class IngestionController {

    private final IngestionService ingestionService;

    @PostMapping("/upload")
    public ResponseEntity<UploadResponse> uploadFile(
            @RequestParam("file") MultipartFile file,
            @RequestParam("userId") @NotBlank String userId,
            @RequestParam("projectId") @NotBlank String projectId,
            @RequestParam(value = "tableName", required = false) String tableName) {

        UploadResponse response = ingestionService.upload(file, userId, projectId, tableName);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/status/{jobId}")
    public ResponseEntity<Map<Object, Object>> getJobStatus(@PathVariable String jobId) {
        Map<Object, Object> jobDetails = ingestionService.getJobStatus(jobId);

        if (jobDetails == null || jobDetails.isEmpty()) {
            return ResponseEntity.status(404)
                    .body(Map.of("error", "Job not found", "jobId", jobId));
        }

        return ResponseEntity.ok(jobDetails);
    }

    /**
     * Long-poll endpoint — blocks until the ingestion job reaches a terminal state
     * or the timeout expires.
     */
    @GetMapping("/status/{jobId}/wait")
    public DeferredResult<ResponseEntity<Map<Object, Object>>> waitForIngestion(
            @PathVariable String jobId,
            @RequestParam(defaultValue = "60") int timeoutSec) {

        DeferredResult<ResponseEntity<Map<Object, Object>>> deferred =
                new DeferredResult<>((long) timeoutSec * 1000);

        deferred.onTimeout(() -> {
            Map<Object, Object> current = ingestionService.getJobStatus(jobId);
            deferred.setResult(current != null && !current.isEmpty()
                    ? ResponseEntity.ok(current)
                    : ResponseEntity.status(404).body(Map.of("error", "Job not found", "jobId", jobId)));
        });

        ingestionService.waitForCompletion(jobId, java.time.Duration.ofSeconds(timeoutSec))
                .thenAccept(details -> {
                    if (details != null && !details.isEmpty()) {
                        deferred.setResult(ResponseEntity.ok(details));
                    }
                });

        return deferred;
    }
}
