package com.abdelwahab.api_service.module.query.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a filter condition in the WHERE clause.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FilterCondition {
    
    /**
     * Column to filter
     */
    private String column;
    
    /**
     * Operator: =, !=, <, <=, >, >=, LIKE, IN, IS NULL, IS NOT NULL
     */
    private String operator;
    
    /**
     * Value to compare (can be string, number, array for IN operator)
     */
    private Object value;
}
