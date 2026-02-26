package com.abdelwahab.api_service.module.query.dto;

import java.util.List;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request DTO for submitting query jobs.
 * Based on API spec: POST /query
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryRequest {
    
    /**
     * Table identifier: "{projectId}.{tableName}" or jobId
     * Required
     */
    @NotBlank(message = "source is required")
    private String source;
    
    /**
     * Columns to select (required, at least one)
     */
    @NotEmpty(message = "select is required and must contain at least one column")
    private List<SelectColumn> select;
    
    /**
     * Optional: WHERE conditions
     */
    private List<FilterCondition> filters;
    
    /**
     * Optional: GROUP BY columns
     */
    private List<String> groupBy;
    
    /**
     * Optional: ORDER BY clauses
     */
    private List<OrderByClause> orderBy;
    
    /**
     * Optional: max rows to return
     */
    private Integer limit;
    
    /**
     * Optional: visualization hints
     */
    private EncodingHints encoding;
}
