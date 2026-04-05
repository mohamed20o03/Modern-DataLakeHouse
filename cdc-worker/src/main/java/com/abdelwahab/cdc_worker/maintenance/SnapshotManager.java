package com.abdelwahab.cdc_worker.maintenance;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.regex.Pattern;

/**
 * Manages Iceberg snapshot lifecycle: tagging and expiration.
 *
 * <p><b>Tagging:</b> Creates named references to specific snapshots using
 * {@code ALTER TABLE ... CREATE TAG}. Tagged snapshots are automatically
 * preserved by Iceberg's {@code ExpireSnapshots} procedure.
 *
 * <p><b>Expiration:</b> Removes old snapshots older than a configurable
 * retention period. Tagged snapshots are never expired — they survive
 * until the tag is explicitly dropped or its retention expires.
 *
 * @see <a href="https://iceberg.apache.org/docs/latest/spark-procedures/#expire_snapshots">
 *     Iceberg ExpireSnapshots</a>
 */
public class SnapshotManager {

    private static final Logger log = LoggerFactory.getLogger(SnapshotManager.class);
    private static final Pattern TAG_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9_-]+");

    private final SparkSession spark;

    public SnapshotManager(SparkSession spark) {
        this.spark = spark;
    }

    /**
     * Creates a named tag on a specific snapshot.
     *
     * @param tableName  fully-qualified table (e.g. {@code iceberg.cdc_namespace.customers})
     * @param tagName    tag label (e.g. {@code training-2026-Q1})
     * @param snapshotId snapshot ID to tag (null = current/latest snapshot)
     * @param retainDays optional retention in days (null = forever)
     * @throws IllegalArgumentException if tagName is invalid
     */
    public void createTag(String tableName, String tagName, Long snapshotId, Integer retainDays) {
        validateTagName(tagName);

        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(tableName)
           .append(" CREATE TAG `").append(tagName).append("`");

        if (snapshotId != null) {
            sql.append(" AS OF VERSION ").append(snapshotId);
        }

        if (retainDays != null && retainDays > 0) {
            sql.append(" RETAIN ").append(retainDays).append(" DAYS");
        }

        String sqlStr = sql.toString();
        log.info("Creating snapshot tag — sql: {}", sqlStr);

        try {
            spark.sql(sqlStr).collect();
            log.info("Tag '{}' created successfully on table '{}'", tagName, tableName);
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("already exists")) {
                log.warn("Tag '{}' already exists on table '{}' — skipping", tagName, tableName);
            } else {
                log.error("Failed to create tag '{}' on table '{}'", tagName, tableName, e);
                throw new RuntimeException("Failed to create tag: " + tagName, e);
            }
        }
    }

    /**
     * Expires snapshots older than the specified retention period.
     * Tagged snapshots are automatically preserved by Iceberg.
     *
     * @param tableName    fully-qualified table name
     * @param retainMillis retention period in milliseconds
     */
    public void expireSnapshots(String tableName, long retainMillis) {
        long expireBefore = Instant.now().toEpochMilli() - retainMillis;

        log.info("Expiring snapshots — table={}, olderThan={}",
                tableName, Instant.ofEpochMilli(expireBefore));

        try {
            String sql = String.format(
                    "CALL iceberg.system.expire_snapshots(" +
                    "  table => '%s', " +
                    "  older_than => TIMESTAMP '%s'" +
                    ")",
                    tableName,
                    Instant.ofEpochMilli(expireBefore).toString()
            );

            spark.sql(sql);
            log.info("Snapshot expiration completed for table '{}'", tableName);

        } catch (Exception e) {
            log.error("Failed to expire snapshots for table '{}'", tableName, e);
            throw new RuntimeException("Snapshot expiration failed: " + tableName, e);
        }
    }

    /**
     * Validates that a tag name contains only allowed characters.
     *
     * @throws IllegalArgumentException if name is empty or contains invalid characters
     */
    private void validateTagName(String tagName) {
        if (tagName == null || tagName.isBlank()) {
            throw new IllegalArgumentException("Tag name must not be empty");
        }
        if (!TAG_NAME_PATTERN.matcher(tagName).matches()) {
            throw new IllegalArgumentException(
                    "Invalid tag name '" + tagName + "'. " +
                    "Must match pattern: [a-zA-Z0-9_-]+");
        }
    }
}
