package com.abdelwahab.query_worker.dto.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * One entry in the SELECT clause of a {@link QueryRequest}.
 *
 * <p>Examples:
 * <ul>
 *   <li>Plain column:      {@code {"column": "Region", "as": "region"}}</li>
 *   <li>Aggregated column: {@code {"column": "Revenue", "aggregation": "sum", "as": "total_revenue"}}</li>
 *   <li>Wildcard:          {@code {"column": "*"}}</li>
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SelectColumn {

    /** Column name; use {@code "*"} for a wildcard or {@code count(*)}. */
    private String column;

    /**
     * Optional aggregation function.
     * Supported: {@code sum, avg, count, min, max, stddev, variance}.
     */
    private String aggregation;

    /** Optional output alias for the result column. */
    private String as;
}
