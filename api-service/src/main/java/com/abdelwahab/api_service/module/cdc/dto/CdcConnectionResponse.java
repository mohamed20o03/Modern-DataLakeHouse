package com.abdelwahab.api_service.module.cdc.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response DTO returned by {@code POST /api/v1/cdc/connections}.
 *
 * <p>Contains the newly created connection's ID, initial status, source/target
 * table names, and a convenience URL for checking status. Sensitive fields
 * (password, username) are intentionally excluded.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CdcConnectionResponse {

    /** Unique identifier (UUID) for this CDC connection. */
    private String connectionId;

    /** Current connection status (initially PENDING). */
    private String status;

    /** Source table in {@code schema.table} format. */
    private String sourceTable;

    /** Target Iceberg table in {@code iceberg.namespace.table} format. */
    private String targetTable;

    /** URL to check connection status: {@code /api/v1/cdc/connections/{connectionId}}. */
    private String checkStatusAt;

    /** ISO-8601 timestamp when the connection was created. */
    private String createdAt;
}
