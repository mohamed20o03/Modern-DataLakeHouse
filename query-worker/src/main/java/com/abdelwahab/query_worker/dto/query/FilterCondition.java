package com.abdelwahab.query_worker.dto.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * One WHERE condition in a {@link QueryRequest}.
 *
 * <p>Supported operators:
 * {@code =, !=, <, <=, >, >=, LIKE, IN, IS NULL, IS NOT NULL}.
 *
 * <p>The {@code value} field is left as {@link Object} so it can hold:
 * <ul>
 *   <li>a scalar ({@code "US"}, {@code 1000})</li>
 *   <li>a list for the {@code IN} operator ({@code ["A", "B", "C"]})</li>
 *   <li>{@code null} for {@code IS NULL} / {@code IS NOT NULL}</li>
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FilterCondition {

    /** Column to filter on. */
    private String column;

    /**
     * Comparison operator.
     * Supported: {@code =, !=, <, <=, >, >=, LIKE, IN, IS NULL, IS NOT NULL}.
     */
    private String operator;

    /** Value to compare against; may be a scalar, a list, or {@code null}. */
    private Object value;
}
