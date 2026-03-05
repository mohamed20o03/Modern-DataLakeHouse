package com.abdelwahab.api_service.module.query.streaming;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.abdelwahab.api_service.jobstatus.service.JobStatusService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Service responsible for reading query result batches from Redis Streams
 * and pushing them to an {@link SseEmitter} for progressive delivery.
 *
 * <p><b>Architecture:</b>
 * <pre>
 *   Query Worker                     Redis                      API Service
 *       │                              │                            │
 *       ├─ XADD metadata ─────────────►│                            │
 *       ├─ XADD batch 0 ──────────────►│◄─── XREAD (blocking) ──────┤
 *       │                              │──── SSE: metadata ────────►│ → Client
 *       │                              │──── SSE: batch 0 ─────────►│ → Client
 *       ├─ XADD batch 1 ──────────────►│                            │
 *       │                              │──── SSE: batch 1 ─────────►│ → Client
 *       ├─ XADD complete ─────────────►│                            │
 *       │                              │──── SSE: complete ────────►│ → Client
 *       │                              │                            │   (close)
 * </pre>
 *
 * <p>A dedicated thread pool handles the blocking {@code XREAD} loop so that
 * servlet threads are not consumed during long-running streams.
 *
 * <p><b>Fallback for inline results:</b> if the job already completed with
 * inline data (small result embedded in Redis), the service sends a single
 * {@code batch} event with all rows and closes the SSE connection immediately.
 *
 * @see QueryStreamController — the HTTP layer
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QueryStreamService {

    /** Matches the key prefix used by RedisResultStreamPublisher in the worker. */
    private static final String STREAM_PREFIX = "query-result-stream:";

    /** Duration each XREAD BLOCK call waits before re-checking. */
    private static final Duration XREAD_BLOCK_TIMEOUT = Duration.ofSeconds(2);

    private final StringRedisTemplate redis;
    private final JobStatusService jobStatusService;

    /**
     * Executor for the blocking XREAD loop — daemon threads so they don't
     * prevent JVM shutdown.
     */
    private final ExecutorService streamExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "sse-stream-reader");
        t.setDaemon(true);
        return t;
    });

    /**
     * Creates an {@link SseEmitter} for the given job and starts a background
     * reader that pushes Redis Stream entries as SSE events.
     *
     * @param jobId      unique query-job identifier
     * @param timeoutSec max SSE connection lifetime in seconds
     * @return the SseEmitter, or {@code null} if the job does not exist
     */
    public SseEmitter createStream(String jobId, int timeoutSec) {
        // Verify job exists
        if (!jobStatusService.exists(jobId)) {
            log.warn("SSE stream requested for non-existent job: {}", jobId);
            return null;
        }

        long timeoutMs = (long) timeoutSec * 1000;
        SseEmitter emitter = new SseEmitter(timeoutMs);

        // Register cleanup callbacks
        emitter.onCompletion(() -> log.debug("SSE completed — jobId={}", jobId));
        emitter.onTimeout(() -> {
            log.debug("SSE timed out — jobId={}", jobId);
            emitter.complete();
        });
        emitter.onError(e -> log.warn("SSE error — jobId={}: {}", jobId, e.getMessage()));

        // Check if the result is already available inline
        Map<Object, Object> jobDetails = jobStatusService.readJobDetails(jobId);
        String status = jobDetails != null ? String.valueOf(jobDetails.get("status")) : null;
        String streamed = jobDetails != null ? String.valueOf(jobDetails.get("streamed")) : null;

        if ("COMPLETED".equals(status) && !"true".equals(streamed)) {
            // Small inline result — send everything in one shot
            sendInlineResult(emitter, jobId, jobDetails);
        } else {
            // Large streamed result — start background Redis Stream reader
            streamExecutor.submit(() -> readStreamAndPush(emitter, jobId, timeoutMs));
        }

        return emitter;
    }

    // ── Inline fallback ────────────────────────────────────────────────────────

    /**
     * Sends a completed inline result as a single batch event and closes the SSE.
     */
    private void sendInlineResult(SseEmitter emitter, String jobId, Map<Object, Object> jobDetails) {
        try {
            Object resultData = jobDetails.get("resultData");
            String rowCountStr = String.valueOf(jobDetails.getOrDefault("rowCount", "0"));

            // Send metadata event (extract column names from first row if available)
            if (resultData != null) {
                emitter.send(SseEmitter.event()
                        .name("batch")
                        .data(resultData.toString()));
            }

            emitter.send(SseEmitter.event()
                    .name("complete")
                    .data("{\"totalRows\":" + rowCountStr + ",\"inline\":true}"));

            emitter.complete();
            log.info("Sent inline result via SSE — jobId={}", jobId);

        } catch (IOException e) {
            log.warn("Failed to send inline result — jobId={}: {}", jobId, e.getMessage());
            emitter.completeWithError(e);
        }
    }

    // ── Redis Stream reader ────────────────────────────────────────────────────

    /**
     * Blocking loop that reads from the Redis Stream and pushes entries
     * to the SSE emitter.  Runs on a dedicated thread from {@code streamExecutor}.
     */
    private void readStreamAndPush(SseEmitter emitter, String jobId, long timeoutMs) {
        String streamKey = STREAM_PREFIX + jobId;
        String lastId = "0"; // Start from the beginning of the stream
        long deadline = System.currentTimeMillis() + timeoutMs;

        try {
            while (System.currentTimeMillis() < deadline) {
                // XREAD BLOCK — waits up to XREAD_BLOCK_TIMEOUT for new entries
                @SuppressWarnings("unchecked")
                List<MapRecord<String, Object, Object>> records = redis.opsForStream().read(
                        StreamOffset.create(streamKey, ReadOffset.from(lastId))
                );

                if (records == null || records.isEmpty()) {
                    // No new entries — check if stream exists yet (worker may not have started)
                    if (!Boolean.TRUE.equals(redis.hasKey(streamKey))) {
                        // Stream doesn't exist yet — wait a bit and retry
                        Thread.sleep(XREAD_BLOCK_TIMEOUT.toMillis());
                        continue;
                    }
                    // Stream exists but no new entries — wait and retry
                    Thread.sleep(500);
                    continue;
                }

                for (MapRecord<String, Object, Object> record : records) {
                    lastId = record.getId().getValue();
                    Map<Object, Object> fields = record.getValue();
                    String type = String.valueOf(fields.get("type"));

                    switch (type) {
                        case "metadata" -> {
                            emitter.send(SseEmitter.event()
                                    .name("metadata")
                                    .data(String.valueOf(fields.get("columns"))));
                        }
                        case "batch" -> {
                            emitter.send(SseEmitter.event()
                                    .name("batch")
                                    .data(String.valueOf(fields.get("rows"))));
                        }
                        case "complete" -> {
                            String completeData = String.format(
                                    "{\"totalBatches\":%s,\"totalRows\":%s}",
                                    fields.get("totalBatches"),
                                    fields.get("totalRows"));
                            emitter.send(SseEmitter.event()
                                    .name("complete")
                                    .data(completeData));
                            emitter.complete();
                            log.info("SSE stream completed — jobId={}, totalRows={}",
                                    jobId, fields.get("totalRows"));
                            return; // Exit the loop
                        }
                        default -> log.warn("Unknown stream entry type: {}", type);
                    }
                }
            }

            // Timeout reached without completion
            log.warn("SSE stream timed out before completion — jobId={}", jobId);
            emitter.send(SseEmitter.event()
                    .name("error")
                    .data("{\"error\":\"Stream timeout\"}"));
            emitter.complete();

        } catch (IOException e) {
            // Client disconnected — this is normal (e.g., browser tab closed)
            log.debug("SSE client disconnected — jobId={}: {}", jobId, e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("SSE stream reader interrupted — jobId={}", jobId);
        } catch (Exception e) {
            log.error("Unexpected error in SSE stream reader — jobId={}", jobId, e);
            try {
                emitter.send(SseEmitter.event()
                        .name("error")
                        .data("{\"error\":\"" + e.getMessage() + "\"}"));
            } catch (IOException ignored) { /* client already gone */ }
            emitter.completeWithError(e);
        }
    }
}
