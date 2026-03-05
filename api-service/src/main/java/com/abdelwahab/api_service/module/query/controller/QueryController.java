package com.abdelwahab.api_service.module.query.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import com.abdelwahab.api_service.module.query.dto.QueryRequest;
import com.abdelwahab.api_service.module.query.dto.QueryResponse;
import com.abdelwahab.api_service.module.query.service.QueryService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * REST controller for data query operations.
 * Handles HTTP only — all workflow logic lives in {@link QueryService}.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/query")
@RequiredArgsConstructor
public class QueryController {

    private final QueryService queryService;

    @PostMapping
    public ResponseEntity<QueryResponse> submitQuery(@Valid @RequestBody QueryRequest request) {
        return ResponseEntity.ok(queryService.submit(request));
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<Map<Object, Object>> getQueryStatus(@PathVariable String jobId) {
        Map<Object, Object> jobDetails = queryService.getJobStatus(jobId);

        if (jobDetails == null || jobDetails.isEmpty()) {
            return ResponseEntity.status(404)
                    .body(Map.of("error", "Job not found", "jobId", jobId));
        }

        return ResponseEntity.ok(jobDetails);
    }

    /**
     * Long-poll endpoint — blocks until the query job reaches a terminal state
     * (COMPLETED / FAILED) or the timeout expires.
     *
     * @param jobId      unique query-job identifier
     * @param timeoutSec max seconds to wait (default 30)
     */
    @GetMapping("/{jobId}/wait")
    public DeferredResult<ResponseEntity<Map<Object, Object>>> waitForQuery(
            @PathVariable String jobId,
            @RequestParam(defaultValue = "30") int timeoutSec) {

        DeferredResult<ResponseEntity<Map<Object, Object>>> deferred =
                new DeferredResult<>((long) timeoutSec * 1000);

        // On timeout, return current state instead of an error
        deferred.onTimeout(() -> {
            Map<Object, Object> current = queryService.getJobStatus(jobId);
            deferred.setResult(current != null && !current.isEmpty()
                    ? ResponseEntity.ok(current)
                    : ResponseEntity.status(404).body(Map.of("error", "Job not found", "jobId", jobId)));
        });

        queryService.waitForCompletion(jobId, java.time.Duration.ofSeconds(timeoutSec))
                .thenAccept(details -> {
                    if (details != null && !details.isEmpty()) {
                        deferred.setResult(ResponseEntity.ok(details));
                    }
                    // null means timeout — handled by onTimeout callback
                });

        return deferred;
    }

}
