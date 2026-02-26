package com.abdelwahab.api_service.module.query.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Optional visualization hints for query results.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EncodingHints {
    
    private String x;
    private String y;
    private String color;
}
