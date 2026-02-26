package com.abdelwahab.api_service.module.query.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a column in the SELECT clause.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SelectColumn {
    
    /**
     * Column name (required)
     */
    private String column;
    
    /**
     * Optional aggregation: sum, avg, count, min, max, stddev, variance
     */
    private String aggregation;
    
    /**
     * Optional alias for result column
     */
    private String as;
}
