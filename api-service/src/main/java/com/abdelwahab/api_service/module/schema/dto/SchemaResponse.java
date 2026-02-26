package com.abdelwahab.api_service.module.schema.dto;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response DTO for schema retrieval.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SchemaResponse {
    
    /**
     * Table identifier (e.g., "myproject.sales")
     */
    private String tableName;
    
    /**
     * List of columns in the table
     */
    private List<SchemaColumn> columns;
    
    /**
     * Table location in storage
     */
    private String location;
    
    /**
     * Number of rows (if available)
     */
    private Long rowCount;
    
    /**
     * Table size in bytes (if available)
     */
    private Long sizeBytes;
}
