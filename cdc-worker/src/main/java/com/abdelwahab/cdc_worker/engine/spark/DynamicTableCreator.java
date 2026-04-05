package com.abdelwahab.cdc_worker.engine.spark;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dynamically creates Iceberg tables from the schema of the first CDC event batch.
 *
 * <p><b>Responsibilities:</b>
 * <ul>
 *   <li>Infer column names and types from the DataFrame schema</li>
 *   <li>Map Spark SQL types to Iceberg-compatible SQL types</li>
 *   <li>Add CDC audit columns ({@code _cdc_op}, {@code _cdc_ts})</li>
 *   <li>Create the table with format-version 2 and merge-on-read</li>
 *   <li>Track which tables have been created (avoid redundant DDL)</li>
 * </ul>
 */
public class DynamicTableCreator implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(DynamicTableCreator.class);
    private static final long serialVersionUID = 1L;

    /** Set of tables that have already been created (or verified to exist). */
    private static final Set<String> createdTables = ConcurrentHashMap.newKeySet();

    /** Columns that are CDC metadata — not part of the source table schema. */
    private static final Set<String> CDC_META_COLUMNS = Set.of("_cdc_op", "_cdc_ts", "_kafka_offset", "topic");

    private DynamicTableCreator() { /* utility class */ }

    /**
     * Ensures the target Iceberg table exists, creating it from the DataFrame schema
     * if necessary.
     *
     * @param spark       SparkSession
     * @param targetTable fully qualified table name (e.g., {@code iceberg.cdc_namespace.orders})
     * @param batchDF     DataFrame from which to infer the schema
     * @param pk          primary key column name
     */
    public static void ensureTable(SparkSession spark, String targetTable,
                                    Dataset<Row> batchDF, String pk) {
        if (createdTables.contains(targetTable)) {
            return;
        }

        synchronized (DynamicTableCreator.class) {
            if (createdTables.contains(targetTable)) {
                return;
            }

            try {
                // Extract namespace from fully qualified name: iceberg.{namespace}.{table}
                String namespace = targetTable.substring(
                        targetTable.indexOf('.') + 1, targetTable.lastIndexOf('.'));

                // Create namespace if not exists
                spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg." + namespace);

                // Build column definitions from DataFrame schema
                StringBuilder columnsDef = new StringBuilder();
                StructField[] fields = batchDF.schema().fields();

                for (StructField field : fields) {
                    // Skip CDC metadata columns — they'll be added explicitly
                    if (CDC_META_COLUMNS.contains(field.name())) {
                        continue;
                    }

                    if (columnsDef.length() > 0) {
                        columnsDef.append(", ");
                    }

                    String sqlType = sparkTypeToSqlType(field.dataType());
                    boolean notNull = field.name().equals(pk);
                    columnsDef.append(field.name())
                              .append(" ")
                              .append(sqlType);
                    if (notNull) {
                        columnsDef.append(" NOT NULL");
                    }
                }

                // Add CDC audit columns
                if (columnsDef.length() > 0) {
                    columnsDef.append(", ");
                }
                columnsDef.append("_cdc_op STRING, _cdc_ts BIGINT");

                String createSql = String.format(
                        "CREATE TABLE IF NOT EXISTS %s (%s) USING iceberg " +
                        "TBLPROPERTIES (" +
                        "  'format-version' = '2', " +
                        "  'write.merge.mode' = 'merge-on-read', " +
                        "  'write.delete.mode' = 'merge-on-read', " +
                        "  'write.update.mode' = 'merge-on-read'" +
                        ")",
                        targetTable, columnsDef
                );

                log.info("Creating Iceberg table: {}", createSql);
                spark.sql(createSql);

                createdTables.add(targetTable);
                log.info("Target Iceberg table created: {}", targetTable);

            } catch (Exception e) {
                log.error("Failed to create target table: {}", targetTable, e);
                throw new RuntimeException("Cannot create target Iceberg table: " + targetTable, e);
            }
        }
    }

    /**
     * Maps Spark SQL DataType to a SQL type string for CREATE TABLE.
     */
    static String sparkTypeToSqlType(DataType dataType) {
        if (dataType == DataTypes.IntegerType) return "INT";
        if (dataType == DataTypes.LongType) return "BIGINT";
        if (dataType == DataTypes.ShortType) return "SMALLINT";
        if (dataType == DataTypes.FloatType) return "FLOAT";
        if (dataType == DataTypes.DoubleType) return "DOUBLE";
        if (dataType == DataTypes.StringType) return "STRING";
        if (dataType == DataTypes.BooleanType) return "BOOLEAN";
        if (dataType == DataTypes.DateType) return "DATE";
        if (dataType == DataTypes.TimestampType) return "TIMESTAMP";
        if (dataType == DataTypes.BinaryType) return "BINARY";
        if (dataType instanceof org.apache.spark.sql.types.DecimalType) {
            org.apache.spark.sql.types.DecimalType dt =
                    (org.apache.spark.sql.types.DecimalType) dataType;
            return String.format("DECIMAL(%d,%d)", dt.precision(), dt.scale());
        }
        // Fallback
        return "STRING";
    }
}
