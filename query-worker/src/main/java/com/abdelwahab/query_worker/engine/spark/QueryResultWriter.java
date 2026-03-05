package com.abdelwahab.query_worker.engine.spark;

import com.abdelwahab.query_worker.dto.query.QueryResult;
import com.abdelwahab.query_worker.streaming.ResultStreamPublisher;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes query results using one of two strategies based on result size:
 *
 * <p><b>Inline vs streaming strategy:</b>
 * <ul>
 *   <li>{@code rowCount <= INLINE_THRESHOLD} → rows embedded in Redis directly;
 *       no streaming, fast path for small queries.</li>
 *   <li>{@code rowCount > INLINE_THRESHOLD} → rows streamed in batches via
 *       {@link ResultStreamPublisher} (Redis Streams), enabling the API service
 *       to forward chunks progressively to the client via SSE.</li>
 * </ul>
 *
 * <p><b>Memory efficiency:</b> for large results, the writer uses Spark's
 * {@link Dataset#toLocalIterator()} to pull rows partition-by-partition rather
 * than collecting the entire result set into the driver's heap.  A small buffer
 * (up to {@value #INLINE_THRESHOLD} + 1 rows) is used to decide the strategy
 * in a single Spark job — no recomputation needed.
 *
 * <p><b>Responsibility (SRP):</b> only handles result delivery.
 * Query building and status updates live elsewhere.
 *
 * @see SparkService             — the orchestrator that calls this writer
 * @see ResultStreamPublisher    — the streaming backend (Redis Streams)
 */
public class QueryResultWriter {

    private static final Logger log = LoggerFactory.getLogger(QueryResultWriter.class);

    /**
     * Row-count threshold below which results are returned inline (embedded in
     * Redis) and streaming is skipped entirely.
     */
    private static final int INLINE_THRESHOLD = 1_000;

    /**
     * Number of rows per streaming batch.  Balances network overhead (fewer,
     * larger batches) against progressive-rendering latency (smaller, more
     * frequent batches).  500 rows ≈ 50–200 KB of JSON per batch.
     */
    private static final int BATCH_SIZE = 500;

    private final ResultStreamPublisher streamPublisher;

    /**
     * @param streamPublisher Redis Streams publisher for large-result streaming
     */
    public QueryResultWriter(ResultStreamPublisher streamPublisher) {
        this.streamPublisher = streamPublisher;
    }

    /**
     * Delivers the result Dataset using the optimal strategy.
     *
     * <p>For small results ({@code rowCount <= } {@value #INLINE_THRESHOLD}) the rows
     * are collected and embedded directly in the returned {@link QueryResult}.
     * For large results the rows are streamed in batches and the returned
     * {@code QueryResult} carries only metadata ({@code streamed = true}).
     *
     * @param result computed result Dataset (already filtered / aggregated / limited)
     * @param jobId  unique job identifier (used as the stream key)
     * @return {@link QueryResult} with inline rows or streaming metadata
     */
    public QueryResult write(Dataset<Row> result, String jobId) {

        String[] columns = result.columns();

        // ── Buffer up to INLINE_THRESHOLD + 1 rows to decide strategy ────────
        // Uses toLocalIterator() which streams partitions one at a time,
        // so we never load the entire result set into memory at once.
        Iterator<Row> iterator = result.toLocalIterator();
        List<Map<String, Object>> buffer = new ArrayList<>(INLINE_THRESHOLD + 1);

        while (iterator.hasNext() && buffer.size() <= INLINE_THRESHOLD) {
            buffer.add(rowToMap(iterator.next(), columns));
        }

        // ── Small result: return inline ──────────────────────────────────────
        if (buffer.size() <= INLINE_THRESHOLD) {
            long rowCount = buffer.size();
            log.info("Inline result — rowCount={} (≤ {}), embedding in Redis",
                    rowCount, INLINE_THRESHOLD);
            return QueryResult.builder()
                    .resultPath(null)
                    .rowCount(rowCount)
                    .fileSizeBytes(0L)
                    .resultData(buffer)
                    .streamed(false)
                    .build();
        }

        // ── Large result: stream via Redis Streams ───────────────────────────
        log.info("Large result detected — streaming via Redis Streams, jobId={}", jobId);

        // 1. Publish column metadata
        streamPublisher.publishMetadata(jobId, columns);

        // 2. Flush the buffer + continue iterating
        int  batchIndex = 0;
        long totalRows  = 0;
        List<Map<String, Object>> batch = new ArrayList<>(BATCH_SIZE);

        // First, drain the buffer we already collected
        for (Map<String, Object> row : buffer) {
            batch.add(row);
            totalRows++;
            if (batch.size() >= BATCH_SIZE) {
                streamPublisher.publishBatch(jobId, batchIndex++, batch);
                batch = new ArrayList<>(BATCH_SIZE);
            }
        }

        // Then continue with the remaining rows from the iterator
        while (iterator.hasNext()) {
            batch.add(rowToMap(iterator.next(), columns));
            totalRows++;
            if (batch.size() >= BATCH_SIZE) {
                streamPublisher.publishBatch(jobId, batchIndex++, batch);
                batch = new ArrayList<>(BATCH_SIZE);
            }
        }

        // Flush any remaining rows
        if (!batch.isEmpty()) {
            streamPublisher.publishBatch(jobId, batchIndex++, batch);
        }

        // 3. Publish completion marker
        streamPublisher.publishComplete(jobId, batchIndex, totalRows);

        log.info("Streaming complete — jobId={}, totalBatches={}, totalRows={}",
                jobId, batchIndex, totalRows);

        return QueryResult.builder()
                .resultPath(null)
                .rowCount(totalRows)
                .fileSizeBytes(0L)
                .resultData(null) // no inline data for streamed results
                .streamed(true)
                .build();
    }

    // ── Private helpers ────────────────────────────────────────────────────────

    /**
     * Converts a single Spark {@link Row} to a plain Java map.
     * Column order is preserved via {@link LinkedHashMap}.
     */
    private Map<String, Object> rowToMap(Row row, String[] columns) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < columns.length; i++) {
            map.put(columns[i], row.get(i));
        }
        return map;
    }
}
