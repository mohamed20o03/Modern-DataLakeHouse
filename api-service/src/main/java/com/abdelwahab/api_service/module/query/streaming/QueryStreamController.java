package com.abdelwahab.api_service.module.query.streaming;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * SSE (Server-Sent Events) controller for streaming query results progressively.
 *
 * <p>When a query produces a large result set (above the inline threshold), the
 * query-worker streams rows in batches via Redis Streams.  This controller
 * subscribes to that stream and forwards each batch to the client as an SSE event,
 * enabling real-time progressive rendering without waiting for the entire result.
 *
 * <p><b>SSE event types:</b>
 * <ul>
 *   <li>{@code metadata} — column names; sent once before any data.</li>
 *   <li>{@code batch}    — a slice of rows; sent N times.</li>
 *   <li>{@code complete} — totals; sent once after the last batch.</li>
 *   <li>{@code error}    — sent if something goes wrong.</li>
 * </ul>
 *
 * <p><b>Usage:</b>
 * <pre>
 *   const evtSource = new EventSource("/api/v1/query/{jobId}/stream");
 *   evtSource.addEventListener("metadata", e => { ... });
 *   evtSource.addEventListener("batch",    e => { ... });
 *   evtSource.addEventListener("complete", e => { evtSource.close(); });
 * </pre>
 *
 * @see QueryStreamService — reads from Redis Streams and pushes to SseEmitter
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/query")
@RequiredArgsConstructor
public class QueryStreamController {

    private final QueryStreamService streamService;

    /**
     * Opens an SSE connection and progressively streams query result batches.
     *
     * <p>If the job was completed with inline data (small result), all rows
     * are sent as a single {@code batch} event and the connection is closed.
     * If the job is still processing, the endpoint subscribes to the Redis
     * Stream and waits for batches.
     *
     * @param jobId      unique query-job identifier
     * @param timeoutSec max seconds to keep the SSE connection open (default 120)
     * @return SSE event stream producing {@code text/event-stream}
     */
    @GetMapping(value = "/{jobId}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<SseEmitter> streamResults(
            @PathVariable String jobId,
            @RequestParam(defaultValue = "120") int timeoutSec) {

        log.info("SSE stream requested — jobId={}, timeoutSec={}", jobId, timeoutSec);

        SseEmitter emitter = streamService.createStream(jobId, timeoutSec);

        if (emitter == null) {
            // Job not found
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok()
                .contentType(MediaType.TEXT_EVENT_STREAM)
                .body(emitter);
    }
}
