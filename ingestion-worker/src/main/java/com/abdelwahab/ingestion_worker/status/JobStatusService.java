package com.abdelwahab.ingestion_worker.status;

import java.util.concurrent.CompletableFuture;

/**
 * Contract for reading and writing job-execution status.
 *
 * <p>All mutating operations return a {@link CompletableFuture} so callers
 * can choose to fire-and-forget or block with {@code .join()} depending on
 * how critical the status update is.  For example:
 * <ul>
 *   <li><em>Fire-and-forget:</em>
 *       {@code writeStatus(jobId, "PROCESSING", null);} — don't block Spark.</li>
 *   <li><em>Block:</em>
 *       {@code writeStatus(jobId, "COMPLETED", msg).join();} — guarantee the
 *       final state is persisted before the method returns.</li>
 * </ul>
 *
 * <p>The storage key format used by implementations is {@code "job:<jobId>"}.
 * Each job is stored as a Redis Hash with at least the fields
 * {@code status}, {@code message}, {@code updatedAt}.
 *
 * @see com.abdelwahab.ingestion_worker.status.redis.AsyncRedisJobStatusService
 * @see com.abdelwahab.ingestion_worker.status.JobStatusServiceFactory
 */
public interface JobStatusService {

    /**
     * Writes (or overwrites) the job status, optional message, and an
     * {@code updatedAt} timestamp atomically.
     *
     * @param jobId   unique job identifier
     * @param status  lifecycle state: {@code PENDING}, {@code PROCESSING},
     *                {@code COMPLETED}, or {@code FAILED}
     * @param message human-readable description or error detail; may be
     *                {@code null} when no message is needed
     * @return future that completes when all fields are persisted
     */
    CompletableFuture<Void> writeStatus(String jobId, String status, String message);

    /**
     * Closes the underlying connection / thread pool.
     * Called by the JVM shutdown hook in
     * {@link com.abdelwahab.ingestion_worker.IngestionWorkerMain}.
     */
    void close();
}
