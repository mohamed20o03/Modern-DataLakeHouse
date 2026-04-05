package com.abdelwahab.cdc_worker.engine.spark;

import com.abdelwahab.cdc_worker.registry.ConnectionRegistry;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Writes CDC events to Iceberg tables using the {@code foreachBatch} callback.
 *
 * <p><b>Dynamic multi-table processing:</b> This writer handles events from
 * multiple source tables in a single micro-batch by grouping events by
 * their Kafka topic. For each topic group:
 * <ol>
 *   <li>Look up connection metadata from {@link ConnectionRegistry} (target table, PK)</li>
 *   <li>Create the Iceberg table if it doesn't exist ({@link DynamicTableCreator})</li>
 *   <li>Build and execute a dynamic {@code MERGE INTO} SQL statement</li>
 * </ol>
 *
 * <p><b>Dynamic MERGE INTO:</b> The SQL is constructed from the DataFrame's
 * column names — no hardcoded column references. The PK column for the
 * {@code ON} condition comes from the connection registry.
 */
public class IcebergMergeWriter implements VoidFunction2<Dataset<Row>, Long> {

    private static final Logger log = LoggerFactory.getLogger(IcebergMergeWriter.class);
    private static final long serialVersionUID = 2L;

    /** Columns that are CDC metadata — excluded from the SET/INSERT clauses. */
    private static final Set<String> META_COLUMNS = Set.of(
            "_cdc_op", "_cdc_ts", "_kafka_offset", "topic"
    );

    private final ConnectionRegistry registry;

    /**
     * @param registry connection registry for PK and target table lookups
     */
    public IcebergMergeWriter(ConnectionRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void call(Dataset<Row> batchDF, Long batchId) throws Exception {
        if (batchDF.isEmpty()) {
            log.debug("Batch {} is empty — skipping", batchId);
            return;
        }

        SparkSession spark = batchDF.sparkSession();

        // Step 1: Parse and deduplicate
        Dataset<Row> parsed = DebeziumEventParser.parse(batchDF);
        if (parsed.isEmpty()) {
            log.debug("Batch {} has no valid events after parsing — skipping", batchId);
            return;
        }

        Dataset<Row> deduped = EventDeduplicator.deduplicate(parsed);

        // Step 2: Get distinct topics in this batch
        List<String> topics = deduped.select("topic").distinct()
                .collectAsList().stream()
                .map(row -> row.getString(0))
                .collect(Collectors.toList());

        log.info("Batch {} — processing events for {} topics: {}", batchId, topics.size(), topics);

        // Step 3: Process each topic group separately
        for (String topicName : topics) {
            try {
                processTopicGroup(spark, deduped, topicName, batchId);
            } catch (Exception e) {
                log.error("Batch {} — failed to process topic {}: {}",
                        batchId, topicName, e.getMessage(), e);

                // Update connection status to FAILED
                ConnectionRegistry.ConnectionMeta meta = registry.getMetaForTopic(topicName);
                if (meta.connectionId != null) {
                    registry.updateStatus(meta.connectionId, "STREAM_FAILED",
                            "MERGE INTO failed: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Processes events for a single Kafka topic (= single source table).
     */
    private void processTopicGroup(SparkSession spark, Dataset<Row> allEvents,
                                    String topicName, Long batchId) {

        // Filter events for this topic
        Dataset<Row> topicEvents = allEvents.filter(allEvents.col("topic").equalTo(topicName));

        if (topicEvents.isEmpty()) {
            return;
        }

        // Look up connection metadata
        ConnectionRegistry.ConnectionMeta meta = registry.getMetaForTopic(topicName);
        String targetTable = meta.targetTable;
        String pk = meta.primaryKeyColumn;

        long eventCount = topicEvents.count();
        log.info("Batch {} — topic={}, events={}, target={}, pk={}",
                batchId, topicName, eventCount, targetTable, pk);

        // Drop the topic and _kafka_offset columns — not needed in the Iceberg table
        Dataset<Row> cleanedEvents = topicEvents.drop("topic", "_kafka_offset");

        // Step 1: Ensure target Iceberg table exists
        DynamicTableCreator.ensureTable(spark, targetTable, cleanedEvents, pk);

        // Step 2: Register as temp view
        String tempView = "cdc_batch_" + batchId + "_" + topicName.replace(".", "_");
        cleanedEvents.createOrReplaceTempView(tempView);

        // Step 3: Build and execute dynamic MERGE INTO
        String mergeSql = buildMergeSql(targetTable, tempView, pk, cleanedEvents);
        log.debug("Executing MERGE INTO: {}", mergeSql);
        spark.sql(mergeSql);

        log.info("Batch {} — MERGE INTO completed for {}", batchId, targetTable);

        // Clean up temp view
        spark.catalog().dropTempView(tempView);

        // Update connection status to STREAMING (if it was SNAPSHOTTING)
        if (meta.connectionId != null) {
            registry.updateStatus(meta.connectionId, "STREAMING",
                    "Streaming active — processed " + eventCount + " events");
        }
    }

    /**
     * Builds a dynamic MERGE INTO SQL statement from the DataFrame's column names.
     *
     * <pre>
     * MERGE INTO iceberg.{ns}.{table} t
     * USING (SELECT /*+ BROADCAST({tempView}) *&#47; * FROM {tempView}) s
     * ON t.{pk} = s.{pk}
     * WHEN MATCHED AND s._cdc_op = 'd' THEN DELETE
     * WHEN MATCHED AND s._cdc_op IN ('c', 'u', 'r') THEN UPDATE SET
     *   t.{col1} = s.{col1}, t.{col2} = s.{col2}, ...
     * WHEN NOT MATCHED AND s._cdc_op != 'd' THEN INSERT
     *   ({col1}, {col2}, ...) VALUES (s.{col1}, s.{col2}, ...)
     * </pre>
     */
    private String buildMergeSql(String targetTable, String tempView,
                                  String pk, Dataset<Row> df) {

        // Get all data columns (excluding meta columns and PK for SET clause)
        List<String> allColumns = Arrays.stream(df.schema().fields())
                .map(StructField::name)
                .filter(name -> !META_COLUMNS.contains(name))
                .collect(Collectors.toList());

        // SET clause: all columns except PK
        String setClause = allColumns.stream()
                .filter(name -> !name.equals(pk))
                .map(name -> "t." + name + " = s." + name)
                .collect(Collectors.joining(", "));

        // Add CDC metadata to SET
        setClause += ", t._cdc_op = s._cdc_op, t._cdc_ts = s._cdc_ts";

        // INSERT columns and values
        List<String> insertColumns = new java.util.ArrayList<>(allColumns);
        insertColumns.add("_cdc_op");
        insertColumns.add("_cdc_ts");

        String insertColumnList = String.join(", ", insertColumns);
        String insertValueList = insertColumns.stream()
                .map(c -> "s." + c)
                .collect(Collectors.joining(", "));

        return String.format(
                "MERGE INTO %s t " +
                "USING (SELECT /*+ BROADCAST(%s) */ * FROM %s) s " +
                "ON t.%s = s.%s " +
                "WHEN MATCHED AND s._cdc_op = 'd' THEN DELETE " +
                "WHEN MATCHED AND s._cdc_op IN ('c', 'u', 'r') THEN UPDATE SET %s " +
                "WHEN NOT MATCHED AND s._cdc_op != 'd' THEN INSERT (%s) VALUES (%s)",
                targetTable, tempView, tempView,
                pk, pk,
                setClause,
                insertColumnList, insertValueList
        );
    }
}
