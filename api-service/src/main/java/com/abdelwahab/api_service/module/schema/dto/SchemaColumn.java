package com.abdelwahab.api_service.module.schema.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a column in a table schema.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SchemaColumn {
    
    /**
     * Column name
     */
    private String name;
    
    /**
     * Column data type (e.g., STRING, INTEGER, DOUBLE, TIMESTAMP, etc.)
     */
    private String type;
    
    /**
     * Whether column is nullable
     */
    private Boolean nullable;
}
