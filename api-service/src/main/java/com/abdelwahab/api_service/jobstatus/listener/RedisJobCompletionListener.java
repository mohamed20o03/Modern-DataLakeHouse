package com.abdelwahab.api_service.jobstatus.listener;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Redis Pub/Sub implementation of {@link JobCompletionListener}.
 *
 * <p>Subscribes to the channel {@code job-done:{jobId}} and resolves the
 * returned future as soon as the worker publishes a terminal status event.
 * After receiving the event (or timing out), the subscription is automatically
 * removed to prevent resource leaks.
 *
 * <p><b>Fallback strategy:</b> when the Pub/Sub event fires, this class reads
 * the full job Hash from Redis (using {@link StringRedisTemplate}) rather than
 * trusting the published payload. This guarantees the caller always gets the
 * complete, authoritative result — including {@code resultData}, {@code rowCount},
 * etc. — even if the publish message only contains the status string.
 *
 * <p><b>Thread safety:</b> each call to {@link #awaitCompletion} creates its own
 * one-shot listener inside the shared {@link RedisMessageListenerContainer}.
 * Spring's container handles the subscription lifecycle on its own thread pool,
 * so this class is safe to call from any thread.
 *
 * @see JobCompletionListener — the contract this class fulfils
 */
@Slf4j
@Component
public class RedisJobCompletionListener implements JobCompletionListener {

    /** Channel prefix — must match the worker's COMPLETION_CHANNEL_PREFIX. */
    private static final String CHANNEL_PREFIX = "job-done:";
    private static final String JOB_PREFIX     = "job:";

    private final RedisMessageListenerContainer listenerContainer;
    private final StringRedisTemplate redis;

    public RedisJobCompletionListener(RedisMessageListenerContainer listenerContainer,
                                      StringRedisTemplate redis) {
        this.listenerContainer = listenerContainer;
        this.redis             = redis;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Implementation notes:
     * <ol>
     *   <li>Subscribes a one-shot {@link MessageListener} to {@code job-done:{jobId}}.</li>
     *   <li>When the event fires, reads the full job Hash from Redis and
     *       completes the future.</li>
     *   <li>On timeout, completes with {@code null} — the caller should fall
     *       back to reading current status from Redis.</li>
     *   <li>In both cases the listener is removed from the container.</li>
     * </ol>
     */
    @Override
    public CompletableFuture<Map<Object, Object>> awaitCompletion(String jobId, Duration timeout) {
        CompletableFuture<Map<Object, Object>> future = new CompletableFuture<>();
        ChannelTopic topic = new ChannelTopic(CHANNEL_PREFIX + jobId);

        // One-shot listener: fires once then unsubscribes itself
        MessageListener listener = (message, pattern) -> {
            log.debug("Pub/Sub event received — jobId={}", jobId);
            // Read the authoritative state from the Redis Hash
            Map<Object, Object> details = readJobHash(jobId);
            future.complete(details);
        };

        // Register the subscription
        listenerContainer.addMessageListener(listener, topic);
        log.debug("Subscribed to completion channel — jobId={}", jobId);

        // Timeout safety net + cleanup
        future.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
              .whenComplete((result, ex) -> {
                  // Always unsubscribe — whether event fired or timeout expired
                  listenerContainer.removeMessageListener(listener, topic);
                  if (ex != null) {
                      log.debug("Completion wait timed out — jobId={}", jobId);
                      // Don't propagate the TimeoutException — resolve with null
                      // so the caller can fall back to reading current state
                  }
              });

        // Handle the timeout case: resolve with null instead of an exception
        return future.exceptionally(ex -> null);
    }

    /**
     * Reads all fields of a job Hash from Redis.
     * Mirrors the logic in {@code RedisJobStatusRepository.readJobDetails}.
     */
    private Map<Object, Object> readJobHash(String jobId) {
        try {
            String key = JOB_PREFIX + jobId;
            Map<Object, Object> details = redis.opsForHash().entries(key);
            return details.isEmpty() ? null : details;
        } catch (Exception e) {
            log.warn("Failed to read job Hash after Pub/Sub event — jobId={}: {}", jobId, e.getMessage());
            return null;
        }
    }
}
