package com.abdelwahab.api_service.jobstatus.service;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Abstraction for job status management.
 * Allows swapping implementations (Redis, DB, in-memory) without changing callers.
 */
public interface JobStatusService {

    /** Write status with default TTL and no message. */
    void writeStatus(String jobId, String status);

    /** Write status with custom TTL and no message. */
    void writeStatus(String jobId, String status, Duration ttl);

    /** Write status with message and custom TTL. */
    void writeStatus(String jobId, String status, String message, Duration ttl);

    /** Read only the status field. */
    String readStatus(String jobId);

    /** Read all job fields. */
    Map<Object, Object> readJobDetails(String jobId);

    /** Update a single field atomically. */
    void updateField(String jobId, String field, String value);

    /** Check if a job exists. */
    boolean exists(String jobId);

    /** Delete a job and all its data. */
    void deleteJob(String jobId);

    /**
     * Wait for a job to reach a terminal state (COMPLETED or FAILED) using
     * Redis Pub/Sub, falling back to a direct Redis read if the job is
     * already complete or the wait times out.
     *
     * @param jobId   unique job identifier
     * @param timeout maximum time to wait
     * @return future resolving to the full job-detail map, or {@code null} on timeout
     */
    CompletableFuture<Map<Object, Object>> waitForCompletion(String jobId, Duration timeout);
}
