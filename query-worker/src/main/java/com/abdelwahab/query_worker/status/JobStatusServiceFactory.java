package com.abdelwahab.query_worker.status;

import com.abdelwahab.query_worker.status.redis.AsyncRedisJobStatusService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating JobStatusService instances.
 * Reads REDIS_MODE environment variable to decide which implementation to use.
 *
 * <p>Supported modes:
 * <ul>
 *   <li>{@code async} (default) - Non-blocking Redis operations via Lettuce async API</li>
 *   <li>{@code sync} - Blocking Redis operations (future implementation)</li>
 * </ul>
 *
 * <p>Usage: set env variable {@code REDIS_MODE=async} or {@code REDIS_MODE=sync}
 */
public class JobStatusServiceFactory {

    private static final Logger log = LoggerFactory.getLogger(JobStatusServiceFactory.class);

    /** Utility class — no instances. */
    private JobStatusServiceFactory() {}

    /**
     * Creates a {@link JobStatusService} connected to the given Redis instance.
     *
     * <p>The implementation is chosen by the {@code REDIS_MODE} environment variable:
     * <ul>
     *   <li>{@code async} (default) — {@link AsyncRedisJobStatusService} using
     *       Lettuce's non-blocking async API; best for high-throughput workers.</li>
     *   <li>{@code sync}            — blocking implementation (not yet built).</li>
     * </ul>
     *
     * @param redisHost     Redis hostname
     * @param redisPort     Redis port (default 6379)
     * @param redisPassword Redis password; pass an empty string for no auth
     * @return a connected, ready-to-use {@link JobStatusService}
     */
    public static JobStatusService create(String redisHost, int redisPort, String redisPassword) {
        // Async mode is the default — non-blocking writes keep Spark threads free
        String mode = System.getenv().getOrDefault("REDIS_MODE", "async").toLowerCase();

        log.info("Creating JobStatusService: mode={}, host={}:{}", mode, redisHost, redisPort);

        return switch (mode) {
            // Lettuce async client — writeStatus returns CompletableFuture immediately
            case "async" -> new AsyncRedisJobStatusService(redisHost, redisPort, redisPassword);

            // case "sync" -> new SyncRedisJobStatusService(redisHost, redisPort, redisPassword);

            default -> {
                // Unknown mode — warn and fall back so the worker still starts
                log.warn("Unknown REDIS_MODE '{}', falling back to async", mode);
                yield new AsyncRedisJobStatusService(redisHost, redisPort, redisPassword);
            }
        };
    }
}
