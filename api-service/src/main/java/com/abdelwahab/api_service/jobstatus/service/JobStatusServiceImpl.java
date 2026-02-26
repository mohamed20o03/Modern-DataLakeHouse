package com.abdelwahab.api_service.jobstatus.service;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.springframework.stereotype.Service;

import com.abdelwahab.api_service.jobstatus.listener.JobCompletionListener;
import com.abdelwahab.api_service.jobstatus.repository.JobStatusRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Business logic layer for job status management.
 * <p>
 * This service provides high-level operations for tracking asynchronous job execution.
 * It delegates to {@link JobStatusRepository} for storage operations while adding
 * business logic such as default TTL management and convenience methods.
 * </p>
 * <p>
 * All jobs automatically expire after {@value #DEFAULT_TTL_HOURS} hours unless a custom
 * TTL is specified.
 * </p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class JobStatusServiceImpl implements JobStatusService {
    
    private final JobStatusRepository repository;
    private final JobCompletionListener completionListener;
    
    /** Default time-to-live for job status entries */
    private static final long DEFAULT_TTL_HOURS = 1;

    /** Statuses that indicate a terminal (done) job. */
    private static final Set<String> TERMINAL_STATUSES = Set.of("COMPLETED", "FAILED");

    /**
     * Writes job status with default 1-hour TTL.
     * <p>
     * Convenience method for simple status updates without message.
     * </p>
     *
     * @param jobId  unique job identifier
     * @param status job status (PENDING, PROCESSING, COMPLETED, FAILED)
     */
    @Override
    public void writeStatus(String jobId, String status) {
        writeStatus(jobId, status, null, Duration.ofHours(DEFAULT_TTL_HOURS));
    }

    /**
     * Writes job status with custom TTL.
     * <p>
     * Use this when you need different expiration times for different job types.
     * </p>
     *
     * @param jobId  unique job identifier
     * @param status job status (PENDING, PROCESSING, COMPLETED, FAILED)
     * @param ttl    custom time-to-live duration
     */
    @Override
    public void writeStatus(String jobId, String status, Duration ttl) {
        writeStatus(jobId, status, null, ttl);
    }
    
    /**
     * Writes complete job status with message and TTL.
     * <p>
     * Full-featured method providing message for error details or status descriptions.
     * </p>
     *
     * @param jobId   unique job identifier
     * @param status  job status (PENDING, PROCESSING, COMPLETED, FAILED)
     * @param message optional status message or error description
     * @param ttl     time-to-live duration for automatic expiration
     */
    @Override
    public void writeStatus(String jobId, String status, String message, Duration ttl) {
        repository.writeStatus(jobId, status, message, ttl);
    }

    /**
     * Reads only the status field for quick status checks.
     * <p>
     * Optimized for performance when only the status is needed.
     * </p>
     *
     * @param jobId unique job identifier
     * @return current status or null if job not found
     */
    @Override
    public String readStatus(String jobId) {
        return repository.readStatus(jobId);
    }
    
    /**
     * Reads all job fields including status, message, and timestamps.
     *
     * @param jobId unique job identifier
     * @return map of all job fields or null if not found
     */
    @Override
    public Map<Object, Object> readJobDetails(String jobId) {
        return repository.readJobDetails(jobId);
    }
    
    /**
     * Updates a specific field atomically.
     * <p>
     * Use this to add custom fields or update specific values without
     * overwriting the entire job status.
     * </p>
     *
     * @param jobId unique job identifier
     * @param field field name to update
     * @param value new field value
     */
    @Override
    public void updateField(String jobId, String field, String value) {
        repository.updateField(jobId, field, value);
    }

    /**
     * Checks if a job exists in storage.
     *
     * @param jobId unique job identifier
     * @return true if job exists, false otherwise
     */
    @Override
    public boolean exists(String jobId) {
        return repository.exists(jobId);
    }
    
    /**
     * Deletes a job and all its associated data.
     * <p>
     * Use this for cleanup or when a client explicitly cancels a job.
     * </p>
     *
     * @param jobId unique job identifier
     */
    @Override
    public void deleteJob(String jobId) {
        repository.deleteJob(jobId);
    }

    /**
     * Waits for a job to reach a terminal state via Redis Pub/Sub.
     * <p>
     * <b>Fast-path:</b> if the job is already COMPLETED or FAILED, returns
     * immediately without subscribing — avoids the race where the Pub/Sub
     * event fired before we subscribed.
     * </p>
     *
     * @param jobId   unique job identifier
     * @param timeout maximum time to wait for the event
     * @return future resolving to the full job-detail map, or {@code null} on timeout
     */
    @Override
    public CompletableFuture<Map<Object, Object>> waitForCompletion(String jobId, Duration timeout) {
        // Fast-path: already done?
        String currentStatus = repository.readStatus(jobId);
        if (currentStatus != null && TERMINAL_STATUSES.contains(currentStatus)) {
            log.debug("Job {} already terminal ({}), returning immediately", jobId, currentStatus);
            return CompletableFuture.completedFuture(repository.readJobDetails(jobId));
        }

        // Subscribe to Pub/Sub and wait for the worker's event
        return completionListener.awaitCompletion(jobId, timeout);
    }
}