package com.abdelwahab.api_service.module.schema.service;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.springframework.stereotype.Service;

import com.abdelwahab.api_service.common.dto.SchemaMessage;
import com.abdelwahab.api_service.common.messaging.MessagePublisherService;
import com.abdelwahab.api_service.jobstatus.service.JobStatusService;
import com.abdelwahab.api_service.module.schema.dto.SchemaJobResponse;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Orchestrates the schema retrieval workflow.
 * <p>
 * Sequence:
 * <ol>
 *   <li>Generate schema-prefixed job ID</li>
 *   <li>Write PENDING status to Redis</li>
 *   <li>Publish schema message to RabbitMQ</li>
 *   <li>Update status to QUEUED</li>
 * </ol>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SchemaService {

    private final JobStatusService jobStatusService;
    private final MessagePublisherService messagePublisherService;

    public SchemaJobResponse requestSchema(String source) {
        String jobId = "schema-" + UUID.randomUUID();
        log.info("Schema retrieval requested: jobId={}, source={}", jobId, source);

        jobStatusService.writeStatus(jobId, "PENDING");

        SchemaMessage message = SchemaMessage.builder()
                .jobId(jobId)
                .source(source)
                .build();

        messagePublisherService.publishSchemaMessage(message);

        jobStatusService.writeStatus(jobId, "QUEUED");

        log.info("Schema retrieval queued: jobId={}, source={}", jobId, source);

        return SchemaJobResponse.builder()
                .jobId(jobId)
                .status("queued")
                .message("Schema retrieval queued")
                .checkStatusAt("/api/v1/schema/status/" + jobId)
                .build();
    }

    public Map<Object, Object> getJobStatus(String jobId) {
        return jobStatusService.readJobDetails(jobId);
    }

    public CompletableFuture<Map<Object, Object>> waitForCompletion(String jobId, Duration timeout) {
        return jobStatusService.waitForCompletion(jobId, timeout);
    }
}
