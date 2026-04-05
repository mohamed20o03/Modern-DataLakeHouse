package com.abdelwahab.api_service.module.cdc.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request DTO for creating a new CDC connection.
 *
 * <p>Accepted by {@code POST /api/v1/cdc/connections}. Contains the source
 * database credentials and target table information needed to set up
 * end-to-end Change Data Capture.
 *
 * <p><b>Security note:</b> The {@code password} field is never logged,
 * never returned in API responses, and is encrypted (AES-256-GCM) before
 * being stored in Redis.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CdcConnectionRequest {

    /** Source database hostname or IP address. */
    @NotBlank(message = "host is required")
    private String host;

    /** Source database port (1–65535, default: 5432). */
    @Min(value = 1, message = "port must be between 1 and 65535")
    @Max(value = 65535, message = "port must be between 1 and 65535")
    @Builder.Default
    private int port = 5432;

    /** Source database name. */
    @NotBlank(message = "database is required")
    private String database;

    /** Source database schema (default: public). */
    @Builder.Default
    private String schema = "public";

    /** Source table name to replicate. */
    @NotBlank(message = "table is required")
    private String table;

    /** Database user with replication privilege. */
    @NotBlank(message = "username is required")
    private String username;

    /** Database password — never logged or returned. */
    @NotBlank(message = "password is required")
    private String password;
}
