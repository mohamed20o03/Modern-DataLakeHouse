package com.abdelwahab.query_worker.dto.query;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Result metadata produced after a successful query or schema execution.
 *
 * <p>All fields are persisted to Redis so the API service can return them
 * from {@code GET /query/{jobId}} or {@code GET /schema/{projectId}/{tableName}}.
 *
 * <p><b>Delivery strategies:</b>
 * <ul>
 *   <li><b>Inline</b> ({@code streamed = false}): rows embedded in {@code resultData}
 *       and stored directly in the Redis job hash.</li>
 *   <li><b>Streamed</b> ({@code streamed = true}): rows published in batches via
 *       Redis Streams; {@code resultData} is {@code null} and clients should read
 *       from the SSE endpoint {@code GET /api/v1/query/{jobId}/stream}.</li>
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryResult {

    /**
     * MinIO-relative path to the Parquet result file.
     * {@code null} when results are inline or streamed.
     */
    private String resultPath;

    /** Total number of rows in the result set. */
    private long rowCount;

    /** Size of the written Parquet file in bytes (0 when streamed). */
    private long fileSizeBytes;

    /**
     * Inline result rows (only for small results, ≤ 1,000 rows).
     * Each entry maps column name → value for one result row.
     * {@code null} when the result was streamed.
     */
    private List<Map<String, Object>> resultData;

    /**
     * {@code true} when the result was delivered via Redis Streams instead
     * of being embedded inline.  Clients should connect to the SSE endpoint
     * to consume the rows progressively.
     */
    private boolean streamed;
}
