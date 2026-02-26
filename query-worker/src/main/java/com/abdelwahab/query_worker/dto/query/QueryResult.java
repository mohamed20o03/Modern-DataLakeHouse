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
 * <p><b>Result path format:</b>
 * {@code wh/{projectId}/queries/query_{timestamp}/result.parquet}
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryResult {

    /**
     * MinIO-relative path to the Parquet result file.
     * Consumers can download it via the MinIO SDK or console.
     */
    private String resultPath;

    /** Total number of rows in the result set. */
    private long rowCount;

    /** Size of the written Parquet file in bytes. */
    private long fileSizeBytes;

    /**
     * Inline result rows (up to 10,000).
     * Each entry maps column name → value for one result row.
     * Stored in Redis as a JSON string.
     */
    private List<Map<String, Object>> resultData;
}
