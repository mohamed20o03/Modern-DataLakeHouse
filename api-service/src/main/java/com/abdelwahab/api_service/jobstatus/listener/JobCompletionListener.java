package com.abdelwahab.api_service.jobstatus.listener;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Abstraction for listening to job-completion events from workers.
 *
 * <p>Implementations subscribe to a notification channel (e.g., Redis Pub/Sub)
 * and resolve a {@link CompletableFuture} the instant a terminal status
 * ({@code COMPLETED} or {@code FAILED}) is received — eliminating the need
 * for the client to poll.
 *
 * <p><b>Contract:</b>
 * <ul>
 *   <li>Each call to {@link #awaitCompletion} registers a one-shot listener
 *       that automatically cleans up after it fires or times out.</li>
 *   <li>If the job is already complete when the method is called, the
 *       implementation must still honour the timeout; it is the caller's
 *       responsibility to check current status first if needed.</li>
 * </ul>
 *
 * <p>Follows the same abstraction pattern as
 * {@link com.abdelwahab.api_service.jobstatus.repository.JobStatusRepository}
 * so the backing technology (Redis, Kafka, SQS, …) can be swapped without
 * touching controllers or services.
 */
public interface JobCompletionListener {

    /**
     * Subscribes to completion events for a specific job and returns a future
     * that resolves with the full job details once the worker publishes a
     * terminal status.
     *
     * <p>The returned future completes with:
     * <ul>
     *   <li>{@code Map<Object, Object>} of job fields on success</li>
     *   <li>{@code null} if the timeout expires before any event is received</li>
     * </ul>
     *
     * @param jobId   unique job identifier to listen for
     * @param timeout maximum time to wait before giving up
     * @return future containing job details or {@code null} on timeout
     */
    CompletableFuture<Map<Object, Object>> awaitCompletion(String jobId, Duration timeout);
}
