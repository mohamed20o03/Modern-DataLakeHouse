package com.abdelwahab.cdc_worker.engine.spark;

import com.abdelwahab.cdc_worker.config.CdcConfig;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes CDC events to an Iceberg table using the {@code foreachBatch} callback.
 *
 * <p><b>Responsibilities:</b>
 * <ol>
 *   <li>Create the target Iceberg table if it does not exist (format-version 2, MoR)</li>
 *   <li>Register the micro-batch as a temp view</li>
 *   <li>Execute {@code MERGE INTO} with DELETE/UPDATE/INSERT logic</li>
 * </ol>
 *
 * <p>This class implements {@link VoidFunction2} so it can be passed directly
 * to {@code writeStream().foreachBatch(writer)}.
 */
public class IcebergMergeWriter implements VoidFunction2<Dataset<Row>, Long> {

    private static final Logger log = LoggerFactory.getLogger(IcebergMergeWriter.class);
    private static final long serialVersionUID = 1L;

    private final String targetTable;
    private volatile boolean tableCreated = false;

    /**
     * @param config CDC configuration (used to derive target table name)
     */
    public IcebergMergeWriter(CdcConfig config) {
        // Target table: iceberg.{namespace}.customers
        this.targetTable = "iceberg." + config.getCdcTargetNamespace() + ".customers";
    }

    @Override
    public void call(Dataset<Row> batchDF, Long batchId) throws Exception {
        if (batchDF.isEmpty()) {
            log.debug("Batch {} is empty — skipping", batchId);
            return;
        }

        SparkSession spark = batchDF.sparkSession();

        // Step 1: Ensure target table exists
        ensureTargetTable(spark);

        // Step 2: Parse and deduplicate
        Dataset<Row> parsed = DebeziumEventParser.parse(batchDF);
        if (parsed.isEmpty()) {
            log.debug("Batch {} has no valid events after parsing — skipping", batchId);
            return;
        }

        Dataset<Row> deduped = EventDeduplicator.deduplicate(parsed);

        // Step 3: Register as temp view for MERGE INTO
        String tempView = "cdc_batch_" + batchId;
        deduped.createOrReplaceTempView(tempView);

        long eventCount = deduped.count();
        log.info("Batch {} — processing {} events into {}", batchId, eventCount, targetTable);

        // Step 4: Execute MERGE INTO with broadcast hint on the (small) batch
        String mergeSql = String.format(
                "MERGE INTO %s t " +
                "USING (SELECT /*+ BROADCAST(%s) */ * FROM %s) s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND s._cdc_op = 'd' THEN DELETE " +
                "WHEN MATCHED AND s._cdc_op IN ('c', 'u', 'r') THEN UPDATE SET " +
                "  t.name = s.name, t.email = s.email, t.status = s.status, " +
                "  t.created_at = s.created_at, t.updated_at = s.updated_at, " +
                "  t._cdc_op = s._cdc_op, t._cdc_ts = s._cdc_ts " +
                "WHEN NOT MATCHED AND s._cdc_op != 'd' THEN INSERT " +
                "  (id, name, email, status, created_at, updated_at, _cdc_op, _cdc_ts) " +
                "  VALUES (s.id, s.name, s.email, s.status, s.created_at, s.updated_at, s._cdc_op, s._cdc_ts)",
                targetTable, tempView, tempView
        );

        spark.sql(mergeSql);
        log.info("Batch {} — MERGE INTO completed successfully", batchId);

        // Clean up temp view
        spark.catalog().dropTempView(tempView);
    }

    /**
     * Creates the target Iceberg table if it does not already exist.
     * Uses format-version 2 for row-level delete support (equality deletes).
     * Configures merge-on-read for all write modes (optimal for streaming CDC).
     */
    private synchronized void ensureTargetTable(SparkSession spark) {
        if (tableCreated) return;

        try {
            // Extract namespace from the fully qualified table name
            String namespace = targetTable.substring(
                    targetTable.indexOf('.') + 1, targetTable.lastIndexOf('.'));

            // Create namespace if not exists
            spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg." + namespace);

            // Create table if not exists with format-version 2 and MoR
            String createSql = String.format(
                    "CREATE TABLE IF NOT EXISTS %s (" +
                    "  id INT NOT NULL, " +
                    "  name STRING NOT NULL, " +
                    "  email STRING, " +
                    "  status STRING, " +
                    "  created_at BIGINT, " +
                    "  updated_at BIGINT, " +
                    "  _cdc_op STRING, " +
                    "  _cdc_ts BIGINT" +
                    ") USING iceberg " +
                    "TBLPROPERTIES (" +
                    "  'format-version' = '2', " +
                    "  'write.merge.mode' = 'merge-on-read', " +
                    "  'write.delete.mode' = 'merge-on-read', " +
                    "  'write.update.mode' = 'merge-on-read'" +
                    ")",
                    targetTable
            );
            spark.sql(createSql);

            tableCreated = true;
            log.info("Target Iceberg table ensured: {}", targetTable);

        } catch (Exception e) {
            log.error("Failed to create target table: {}", targetTable, e);
            throw new RuntimeException("Cannot create target Iceberg table", e);
        }
    }
}
