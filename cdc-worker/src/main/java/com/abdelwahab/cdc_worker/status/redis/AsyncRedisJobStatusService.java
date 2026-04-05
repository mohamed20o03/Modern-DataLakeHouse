package com.abdelwahab.cdc_worker.status.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Non-blocking Redis implementation for CDC job status reporting.
 *
 * <p>Adapted from the ingestion-worker's {@code AsyncRedisJobStatusService}.
 * Uses the Lettuce async API so that status writes never block Spark threads.
 *
 * <p><b>Redis data model — one Hash per job:</b>
 * <pre>
 *   Key:    "job:{jobId}"
 *   Fields: status     → "STARTING" | "RUNNING" | "FAILED" | "STOPPED"
 *           message    → human-readable description or error text
 *           updatedAt  → ISO-8601 timestamp
 * </pre>
 */
public class AsyncRedisJobStatusService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AsyncRedisJobStatusService.class);

    private static final String JOB_PREFIX = "job:";
    private static final String COMPLETION_CHANNEL_PREFIX = "job-done:";

    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisAsyncCommands<String, String> asyncCommands;

    public AsyncRedisJobStatusService(String redisHost, int redisPort, String redisPassword) {
        log.info("Connecting to Redis: {}:{}", redisHost, redisPort);

        String redisUri = redisPassword.isEmpty()
                ? String.format("redis://%s:%d", redisHost, redisPort)
                : String.format("redis://%s@%s:%d", redisPassword, redisHost, redisPort);

        this.redisClient    = RedisClient.create(redisUri);
        this.connection     = redisClient.connect();
        this.asyncCommands  = connection.async();
        log.info("Redis connected successfully");
    }

    /**
     * Writes status, message, and updatedAt to the job Hash.
     */
    public CompletableFuture<Void> writeStatus(String jobId, String status, String message) {
        try {
            String key       = JOB_PREFIX + jobId;
            String timestamp = Instant.now().toString();

            CompletableFuture<Boolean> statusWrite = asyncCommands
                    .hset(key, "status", status).toCompletableFuture();
            CompletableFuture<Boolean> timestampWrite = asyncCommands
                    .hset(key, "updatedAt", timestamp).toCompletableFuture();
            CompletableFuture<Boolean> messageWrite = message != null
                    ? asyncCommands.hset(key, "message", message).toCompletableFuture()
                    : CompletableFuture.completedFuture(false);

            return CompletableFuture.allOf(statusWrite, timestampWrite, messageWrite)
                    .thenAccept(v -> {
                        log.debug("Status written — jobId={}, status={}", jobId, status);
                        publishCompletionEvent(jobId, status);
                    })
                    .exceptionally(e -> {
                        log.error("Failed to write status — jobId={}", jobId, e);
                        return null;
                    });

        } catch (Exception e) {
            log.error("Unexpected error in writeStatus — jobId={}", jobId, e);
            return CompletableFuture.failedFuture(e);
        }
    }

    private void publishCompletionEvent(String jobId, String status) {
        if ("COMPLETED".equals(status) || "FAILED".equals(status) || "STOPPED".equals(status)) {
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

    @Override
    public void close() {
        try {
            if (connection != null) connection.close();
            if (redisClient != null) redisClient.shutdown();
            log.info("Redis connection closed");
        } catch (Exception e) {
            log.error("Error closing Redis connection", e);
        }
    }
}
