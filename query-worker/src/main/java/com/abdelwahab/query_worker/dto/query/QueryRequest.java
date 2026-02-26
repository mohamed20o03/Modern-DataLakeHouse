package com.abdelwahab.query_worker.dto.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Structured representation of a query request, deserialized from the
 * {@code queryJson} field carried inside a
 * {@link com.abdelwahab.query_worker.dto.QueryMessage}.
 *
 * <p>This mirrors the API-layer {@code QueryRequest} so the worker operates
 * on typed objects rather than raw JSON strings throughout the engine.
 *
 * <p>Unknown JSON fields are silently ignored so the worker stays compatible
 * if the API adds new optional fields (e.g., {@code encoding}).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryRequest {

    /** Table identifier: {@code "{projectId}.{tableName}"} */
    private String source;

    /** Columns to select — at least one required. */
    private List<SelectColumn> select;

    /** Optional WHERE conditions. */
    private List<FilterCondition> filters;

    /** Optional GROUP BY column names. */
    private List<String> groupBy;

    /** Optional ORDER BY clauses. */
    private List<OrderByClause> orderBy;

    /** Optional maximum rows to return (capped at 10,000 by the engine). */
    private Integer limit;
}
