package com.abdelwahab.api_service.module.query.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents an ORDER BY clause.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderByClause {
    
    /**
     * Column to order by
     */
    private String column;
    
    /**
     * Sort direction: asc or desc (default: asc)
     */
    private String direction;
}
