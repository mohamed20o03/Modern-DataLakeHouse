package com.abdelwahab.api_service.module.cdc.service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Validates source database connectivity and discovers table metadata.
 *
 * <p><b>Responsibilities:</b>
 * <ol>
 *   <li>Test JDBC connectivity to the source PostgreSQL database</li>
 *   <li>Validate that {@code wal_level=logical} is configured</li>
 *   <li>Discover the primary key column of the target table</li>
 *   <li>Discover all columns and their types for the target table</li>
 * </ol>
 *
 * <p><b>Security:</b> Passwords are never logged by this class. Connection
 * strings are constructed without embedding the password in logged output.
 */
@Slf4j
@Component
public class SourceDatabaseValidator {

    /**
     * Represents a discovered column from the source database.
     */
    @Data
    public static class ColumnInfo {
        private final String name;
        private final String type;
        private final boolean nullable;
    }

    /**
     * Result of a full validation including connectivity, WAL level, PK, and columns.
     */
    @Data
    public static class ValidationResult {
        private final boolean valid;
        private final String errorMessage;
        private final String primaryKeyColumn;
        private final List<ColumnInfo> columns;

        public static ValidationResult success(String primaryKeyColumn, List<ColumnInfo> columns) {
            return new ValidationResult(true, null, primaryKeyColumn, columns);
        }

        public static ValidationResult failure(String errorMessage) {
            return new ValidationResult(false, errorMessage, null, null);
        }
    }

    /**
     * Performs full validation of the source database.
     *
     * @param host     source DB hostname
     * @param port     source DB port
     * @param database source DB name
     * @param schema   source schema
     * @param table    source table name
     * @param username DB username
     * @param password DB password (never logged)
     * @return validation result with PK and column info on success
     */
    public ValidationResult validate(String host, int port, String database,
                                     String schema, String table,
                                     String username, String password) {

        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
        log.info("Validating source DB: host={}, port={}, database={}, schema={}, table={}",
                host, port, database, schema, table);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {

            // 1. Connection test passed implicitly

            // 2. Validate WAL level
            if (!validateWalLevel(conn)) {
                return ValidationResult.failure(
                        "Source database does not have wal_level=logical. "
                        + "CDC requires PostgreSQL to be configured with wal_level=logical.");
            }

            // 3. Verify table exists
            if (!tableExists(conn, schema, table)) {
                return ValidationResult.failure(
                        String.format("Table %s.%s does not exist in database %s.", schema, table, database));
            }

            // 4. Discover primary key
            String primaryKey = discoverPrimaryKey(conn, schema, table);
            if (primaryKey == null) {
                return ValidationResult.failure(
                        String.format("Table %s.%s has no primary key. "
                                + "CDC requires a primary key column for MERGE INTO.", schema, table));
            }

            // 5. Discover columns
            List<ColumnInfo> columns = discoverColumns(conn, schema, table);
            if (columns.isEmpty()) {
                return ValidationResult.failure(
                        String.format("Could not discover columns for table %s.%s.", schema, table));
            }

            log.info("Source DB validation passed — PK={}, columns={}", primaryKey, columns.size());
            return ValidationResult.success(primaryKey, columns);

        } catch (Exception e) {
            String msg = e.getMessage();
            // Sanitize: don't leak the password in the error message
            if (msg != null && password != null) {
                msg = msg.replace(password, "***");
            }
            log.error("Source DB validation failed: host={}, error={}", host, msg);
            return ValidationResult.failure("Cannot connect to source database: " + msg);
        }
    }

    /**
     * Tests basic JDBC connectivity.
     */
    public boolean testConnection(String host, int port, String database,
                                   String username, String password) {
        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            return conn.isValid(5);
        } catch (Exception e) {
            log.warn("Connection test failed: host={}", host);
            return false;
        }
    }

    /**
     * Validates that the source PostgreSQL has {@code wal_level=logical}.
     */
    boolean validateWalLevel(Connection conn) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW wal_level")) {
            if (rs.next()) {
                String walLevel = rs.getString(1);
                log.debug("Source DB wal_level={}", walLevel);
                return "logical".equalsIgnoreCase(walLevel);
            }
            return false;
        } catch (Exception e) {
            log.warn("Could not check wal_level", e);
            return false;
        }
    }

    /**
     * Checks if the target table exists.
     */
    boolean tableExists(Connection conn, String schema, String table) {
        try {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(null, schema, table, new String[]{"TABLE"})) {
                return rs.next();
            }
        } catch (Exception e) {
            log.warn("Could not check table existence: {}.{}", schema, table, e);
            return false;
        }
    }

    /**
     * Discovers the primary key column name for the given table.
     *
     * @return the PK column name, or {@code null} if no PK exists
     */
    String discoverPrimaryKey(Connection conn, String schema, String table) {
        try {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getPrimaryKeys(null, schema, table)) {
                if (rs.next()) {
                    String pk = rs.getString("COLUMN_NAME");
                    log.debug("Discovered PK for {}.{}: {}", schema, table, pk);
                    return pk;
                }
            }
            return null;
        } catch (Exception e) {
            log.warn("Could not discover PK for {}.{}", schema, table, e);
            return null;
        }
    }

    /**
     * Discovers all columns and their types for the given table.
     */
    List<ColumnInfo> discoverColumns(Connection conn, String schema, String table) {
        List<ColumnInfo> columns = new ArrayList<>();
        try {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getColumns(null, schema, table, null)) {
                while (rs.next()) {
                    String name = rs.getString("COLUMN_NAME");
                    String type = rs.getString("TYPE_NAME");
                    boolean nullable = rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
                    columns.add(new ColumnInfo(name, type, nullable));
                }
            }
            log.debug("Discovered {} columns for {}.{}", columns.size(), schema, table);
        } catch (Exception e) {
            log.warn("Could not discover columns for {}.{}", schema, table, e);
        }
        return columns;
    }
}
