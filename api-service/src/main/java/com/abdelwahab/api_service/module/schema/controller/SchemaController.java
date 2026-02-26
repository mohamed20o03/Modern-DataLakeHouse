package com.abdelwahab.api_service.module.schema.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import com.abdelwahab.api_service.module.schema.dto.SchemaJobResponse;
import com.abdelwahab.api_service.module.schema.service.SchemaService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * REST controller for schema retrieval operations.
 * Handles HTTP only — all workflow logic lives in {@link SchemaService}.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/schema")
@RequiredArgsConstructor
public class SchemaController {

    private final SchemaService schemaService;

    @GetMapping("/{source}")
    public ResponseEntity<SchemaJobResponse> getTableSchema(@PathVariable String source) {
        return ResponseEntity.ok(schemaService.requestSchema(source));
    }

    @GetMapping("/status/{jobId}")
    public ResponseEntity<Map<Object, Object>> getSchemaStatus(@PathVariable String jobId) {
        Map<Object, Object> jobDetails = schemaService.getJobStatus(jobId);

        if (jobDetails == null || jobDetails.isEmpty()) {
            return ResponseEntity.status(404)
                    .body(Map.of("error", "Job not found", "jobId", jobId));
        }

        return ResponseEntity.ok(jobDetails);
    }

    /**
     * Long-poll endpoint — blocks until the schema job reaches a terminal state
     * or the timeout expires.
     */
    @GetMapping("/status/{jobId}/wait")
    public DeferredResult<ResponseEntity<Map<Object, Object>>> waitForSchema(
            @PathVariable String jobId,
            @RequestParam(defaultValue = "30") int timeoutSec) {

        DeferredResult<ResponseEntity<Map<Object, Object>>> deferred =
                new DeferredResult<>((long) timeoutSec * 1000);

        deferred.onTimeout(() -> {
            Map<Object, Object> current = schemaService.getJobStatus(jobId);
            deferred.setResult(current != null && !current.isEmpty()
                    ? ResponseEntity.ok(current)
                    : ResponseEntity.status(404).body(Map.of("error", "Job not found", "jobId", jobId)));
        });

        schemaService.waitForCompletion(jobId, java.time.Duration.ofSeconds(timeoutSec))
                .thenAccept(details -> {
                    if (details != null && !details.isEmpty()) {
                        deferred.setResult(ResponseEntity.ok(details));
                    }
                });

        return deferred;
    }
}
