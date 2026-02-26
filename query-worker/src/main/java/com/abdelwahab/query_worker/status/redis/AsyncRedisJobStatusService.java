package com.abdelwahab.query_worker.status.redis;

import com.abdelwahab.query_worker.status.JobStatusService;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Non-blocking Redis implementation of {@link JobStatusService}.
 *
 * <p>Uses the <a href="https://lettuce.io">Lettuce</a> async API so that status
 * writes never block query worker threads.  Every Redis command returns a
 * {@link CompletableFuture}; callers can {@code .join()} only when they need a
 * guaranteed write (e.g., the final COMPLETED / FAILED update).
 *
 * <p><b>Redis data model — one Hash per job:</b>
 * <pre>
 *   Key:    "job:{jobId}"   (String)
 *   Fields: status     → "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED"
 *           message    → human-readable description or error text
 *           updatedAt  → ISO-8601 timestamp of the last write
 * </pre>
 *
 * @see com.abdelwahab.query_worker.status.JobStatusServiceFactory — selects this class via {@code REDIS_MODE=async}
 */
public class AsyncRedisJobStatusService implements JobStatusService {

    private static final Logger log = LoggerFactory.getLogger(AsyncRedisJobStatusService.class);

    /** All job keys share this prefix to avoid collisions with other Redis data. */
    private static final String JOB_PREFIX = "job:";

    /**
     * Redis Pub/Sub channel prefix for job-completion events.
     * Channel name: {@code job-done:{jobId}} — one per job, auto-cleaned by Redis
     * when there are no subscribers.
     */
    private static final String COMPLETION_CHANNEL_PREFIX = "job-done:";

    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> redisConnection;

    /** Async command interface — each method returns a RedisFuture (extends CompletableFuture). */
    private final RedisAsyncCommands<String, String> asyncCommands;

    /**
     * Opens a Lettuce connection to the given Redis instance.
     *
     * <p>The URI format used by Lettuce:
     * <ul>
     *   <li>No auth: {@code redis://host:port}</li>
     *   <li>With password: {@code redis://password@host:port}</li>
     * </ul>
     *
     * @param redisHost     hostname or IP of the Redis server
     * @param redisPort     port (default 6379)
     * @param redisPassword auth password; pass an empty string for no auth
     */
    public AsyncRedisJobStatusService(String redisHost, int redisPort, String redisPassword) {
        log.info("Connecting to Redis: {}:{}", redisHost, redisPort);

        // Build the Lettuce URI — password is inserted before the host
        String redisUri = redisPassword.isEmpty()
                ? String.format("redis://%s:%d", redisHost, redisPort)
                : String.format("redis://%s@%s:%d", redisPassword, redisHost, redisPort);

        this.redisClient    = RedisClient.create(redisUri);
        this.redisConnection = redisClient.connect();         // creates one TCP connection
        this.asyncCommands  = redisConnection.async();        // async command dispatcher
        log.info("Redis connected successfully");
    }

    /**
     * Writes {@code status}, {@code message}, and {@code updatedAt} to the job Hash
     * in three parallel HSET commands, then resolves when all three confirm.
     *
     * <p>Using {@link CompletableFuture#allOf} means the future returned here only
     * resolves after Redis has persisted all three fields — safe to {@code .join()}
     * when a guaranteed write is needed.
     *
     * @param jobId   unique job identifier
     * @param status  new lifecycle state
     * @param message optional description; skipped (no HSET) when {@code null}
     * @return future that completes when all fields are persisted
     */
    @Override
    public CompletableFuture<Void> writeStatus(String jobId, String status, String message) {
        try {
            String key       = JOB_PREFIX + jobId;        // e.g. "job:abc-123"
            String timestamp = Instant.now().toString();  // ISO-8601 e.g. "2026-02-19T10:00:00Z"

            // Fire three HSET commands in parallel — Lettuce pipelines them on one TCP connection
            CompletableFuture<Boolean> statusWrite = asyncCommands
                    .hset(key, "status", status)
                    .toCompletableFuture();

            CompletableFuture<Boolean> timestampWrite = asyncCommands
                    .hset(key, "updatedAt", timestamp)
                    .toCompletableFuture();

            // Only write the message field if one was provided
            CompletableFuture<Boolean> messageWrite = message != null
                    ? asyncCommands.hset(key, "message", message).toCompletableFuture()
                    : CompletableFuture.completedFuture(false); // no-op placeholder

            // allOf waits for all three before resolving; any failure is caught by exceptionally
            return CompletableFuture.allOf(statusWrite, timestampWrite, messageWrite)
                    .thenAccept(v -> {
                        log.debug("Status written — jobId={}, status={}", jobId, status);
                        publishCompletionEvent(jobId, status);
                    })
                    .exceptionally(e -> {
                        log.error("Failed to write status to Redis — jobId={}", jobId, e);
                        return null; // swallow so the caller's future still completes
                    });

        } catch (Exception e) {
            log.error("Unexpected error in writeStatus — jobId={}", jobId, e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Atomically writes the final result metadata of a completed or failed query job.
     *
     * <p>All HSET commands are fired in parallel on the same Lettuce TCP connection
     * and the returned future resolves only when every field is persisted — safe
     * to {@code .join()} before the worker ACKs the RabbitMQ message.
     *
     * <p>Optional fields ({@code resultPath} and {@code resultData}) are only written
     * when non-null, keeping the Redis hash lean for FAILED / schema-only jobs.
     */
    @Override
    public CompletableFuture<Void> writeResult(String jobId, String status, String message,
                                               String resultPath, long rowCount,
                                               long fileSizeBytes, String resultDataJson) {
        try {
            String key       = JOB_PREFIX + jobId;
            String timestamp = Instant.now().toString();

            // Build the list of parallel writes — always include the core fields
            List<CompletableFuture<Boolean>> writes = new ArrayList<>();
            writes.add(asyncCommands.hset(key, "status",        status)                          .toCompletableFuture());
            writes.add(asyncCommands.hset(key, "message",       message != null ? message : "") .toCompletableFuture());
            writes.add(asyncCommands.hset(key, "updatedAt",     timestamp)                       .toCompletableFuture());
            writes.add(asyncCommands.hset(key, "rowCount",      String.valueOf(rowCount))         .toCompletableFuture());
            writes.add(asyncCommands.hset(key, "fileSizeBytes", String.valueOf(fileSizeBytes))    .toCompletableFuture());

            // Only write optional fields when present (null = schema job or failure)
            if (resultPath != null) {
                writes.add(asyncCommands.hset(key, "resultPath", resultPath).toCompletableFuture());
            }
            if (resultDataJson != null) {
                writes.add(asyncCommands.hset(key, "resultData", resultDataJson).toCompletableFuture());
            }

            return CompletableFuture.allOf(writes.toArray(new CompletableFuture[0]))
                    .thenAccept(v -> {
                        log.info("Result written — jobId={}, status={}, rows={}, bytes={}",
                                jobId, status, rowCount, fileSizeBytes);
                        publishCompletionEvent(jobId, status);
                    })
                    .exceptionally(e -> {
                        log.error("Failed to write result to Redis — jobId={}", jobId, e);
                        return null;
                    });

        } catch (Exception e) {
            log.error("Unexpected error in writeResult — jobId={}", jobId, e);
            return CompletableFuture.failedFuture(e);
        }
    }

    // ── Pub/Sub notification ──────────────────────────────────────────────────

    /**
     * Publishes a completion event to Redis Pub/Sub so long-poll listeners in
     * the API service can resolve immediately instead of polling.
     *
     * <p>Only terminal states ({@code COMPLETED}, {@code FAILED}) trigger a
     * publish — intermediate states like PROCESSING are not interesting to the
     * listener because the client already knows the job is running.
     *
     * <p>Fire-and-forget: the publish is best-effort. If no subscriber is
     * listening the message is silently discarded by Redis — no data loss,
     * clients fall back to polling.
     *
     * @param jobId  job identifier (used as part of the channel name)
     * @param status lifecycle state — only COMPLETED or FAILED triggers a publish
     */
    private void publishCompletionEvent(String jobId, String status) {
        if ("COMPLETED".equals(status) || "FAILED".equals(status)) {
            String channel = COMPLETION_CHANNEL_PREFIX + jobId;
            asyncCommands.publish(channel, status)
                    .thenAccept(receivers -> log.debug(
                            "Published completion event — channel={}, receivers={}", channel, receivers))
                    .exceptionally(e -> {
                        log.warn("Failed to publish completion event — channel={}: {}",
                                channel, e.getMessage());
                        return null;
                    });
        }
    }

    /**
     * Close Redis connection.
     */
    @Override
    public void close() {
        try {
            if (redisConnection != null) {
                redisConnection.close();
            }
            if (redisClient != null) {
                redisClient.shutdown();
            }
            log.info("Redis connection closed");
        } catch (Exception e) {
            log.error("Error closing Redis connection", e);
        }
    }
}
