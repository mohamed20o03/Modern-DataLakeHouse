package com.abdelwahab.query_worker.dto.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * One ORDER BY clause in a {@link QueryRequest}.
 *
 * <p>Example: {@code {"column": "total_revenue", "direction": "desc"}}
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderByClause {

    /** Column to sort by. */
    private String column;

    /**
     * Sort direction: {@code "asc"} (default) or {@code "desc"}.
     * Any value other than {@code "desc"} (case-insensitive) is treated as ascending.
     */
    private String direction;
}
