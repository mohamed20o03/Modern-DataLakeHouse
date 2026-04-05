package com.abdelwahab.api_service.module.cdc.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Status DTO returned by {@code GET /api/v1/cdc/connections/{connectionId}}.
 *
 * <p>Provides comprehensive status information about a CDC connection,
 * including source/target details, Debezium connector status, and error
 * messages. Sensitive fields (password, username) are intentionally excluded.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CdcConnectionStatus {

    /** Unique identifier (UUID) for this CDC connection. */
    private String connectionId;

    /** Current connection lifecycle state (e.g., PENDING, STREAMING, FAILED). */
    private String status;

    /** Source database hostname. */
    private String sourceHost;

    /** Source database name. */
    private String sourceDatabase;

    /** Source table in {@code schema.table} format. */
    private String sourceTable;

    /** Target Iceberg table in {@code iceberg.namespace.table} format. */
    private String targetTable;

    /** Live Debezium connector status (RUNNING, FAILED, UNASSIGNED). */
    private String connectorStatus;

    /** Error details when status is a *_FAILED state; null if healthy. */
    private String errorMessage;

    /** ISO-8601 timestamp when the connection was created. */
    private String createdAt;

    /** ISO-8601 timestamp of the last status update. */
    private String updatedAt;
}
