package com.abdelwahab.query_worker.streaming;

import java.util.List;
import java.util.Map;

/**
 * Contract for publishing query result chunks to a streaming channel.
 *
 * <p>Large query results (above the inline threshold) are streamed in batches
 * rather than written to MinIO as a single Parquet file.  Each batch is
 * published as a separate entry so that downstream consumers (e.g., the API
 * service's SSE endpoint) can forward rows progressively to the client.
 *
 * <p><b>Protocol (3 event types):</b>
 * <ol>
 *   <li>{@link #publishMetadata} — sent once before any rows; contains column names.</li>
 *   <li>{@link #publishBatch}    — sent N times, each with a slice of rows.</li>
 *   <li>{@link #publishComplete} — sent once after the last batch; contains totals.</li>
 * </ol>
 *
 * <p>Implementations must guarantee ordering so consumers can reassemble the
 * full result set without additional sorting.
 *
 * @see com.abdelwahab.query_worker.streaming.redis.RedisResultStreamPublisher
 */
public interface ResultStreamPublisher {

    /**
     * Publishes the column metadata before any row batches.
     *
     * <p>Consumers use this to set up table headers / schema before data arrives.
     *
     * @param jobId   unique job identifier (determines the stream key)
     * @param columns ordered array of column names in the result set
     */
    void publishMetadata(String jobId, String[] columns);

    /**
     * Publishes one batch of result rows.
     *
     * <p>Called repeatedly until all rows have been streamed.  Batch order
     * matches the order returned by the query engine (respecting ORDER BY).
     *
     * @param jobId      unique job identifier
     * @param batchIndex zero-based index of this batch
     * @param rows       list of column→value maps for each row in the batch
     */
    void publishBatch(String jobId, int batchIndex, List<Map<String, Object>> rows);

    /**
     * Publishes the final completion marker.
     *
     * <p>After this call, no more batches will be published for the given job.
     * Consumers should close their stream / SSE connection upon receipt.
     *
     * @param jobId        unique job identifier
     * @param totalBatches total number of batches that were published
     * @param totalRows    total number of rows across all batches
     */
    void publishComplete(String jobId, int totalBatches, long totalRows);

    /**
     * Releases any underlying connections or thread pools.
     */
    void close();
}
