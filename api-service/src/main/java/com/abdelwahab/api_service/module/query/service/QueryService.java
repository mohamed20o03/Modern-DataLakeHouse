package com.abdelwahab.api_service.module.query.service;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.springframework.stereotype.Service;

import com.abdelwahab.api_service.common.dto.QueryMessage;
import com.abdelwahab.api_service.common.messaging.MessagePublisherService;
import com.abdelwahab.api_service.jobstatus.service.JobStatusService;
import com.abdelwahab.api_service.module.query.dto.QueryRequest;
import com.abdelwahab.api_service.module.query.dto.QueryResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Orchestrates the query submission workflow.
 * <p>
 * Sequence:
 * <ol>
 *   <li>Generate query-prefixed job ID</li>
 *   <li>Write PENDING status to Redis</li>
 *   <li>Serialize request and publish message to RabbitMQ</li>
 *   <li>Update status to QUEUED</li>
 * </ol>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QueryService {

    private final JobStatusService jobStatusService;
    private final MessagePublisherService messagePublisherService;
    private final ObjectMapper objectMapper;

    public QueryResponse submit(QueryRequest request) {
        String jobId = "query-" + UUID.randomUUID();
        log.info("Submitting query: jobId={}, source={}", jobId, request.getSource());

        jobStatusService.writeStatus(jobId, "PENDING");

        String queryJson;
        try {
            queryJson = objectMapper.writeValueAsString(request);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize query request", e);
        }

        QueryMessage message = QueryMessage.builder()
                .jobId(jobId)
                .source(request.getSource())
                .queryJson(queryJson)
                .build();

        messagePublisherService.publishQueryMessage(message);

        jobStatusService.writeStatus(jobId, "QUEUED");

        log.info("Query submitted: jobId={}", jobId);

        return QueryResponse.builder()
                .jobId(jobId)
                .status("queued")
                .message("Query job queued for processing")
                .checkStatusAt("/api/v1/query/" + jobId)
                .streamUrl("/api/v1/query/" + jobId + "/stream")
                .build();
    }

    public Map<Object, Object> getJobStatus(String jobId) {
        return jobStatusService.readJobDetails(jobId);
    }

    /**
     * Waits for a query job to reach a terminal state via Redis Pub/Sub.
     *
     * @param jobId   unique query-job identifier
     * @param timeout maximum time to wait
     * @return future resolving to job details, or {@code null} on timeout
     */
    public CompletableFuture<Map<Object, Object>> waitForCompletion(String jobId, Duration timeout) {
        return jobStatusService.waitForCompletion(jobId, timeout);
    }
}
