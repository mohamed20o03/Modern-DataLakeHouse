package com.abdelwahab.cdc_worker.engine.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Parses Debezium CDC event envelopes from raw Kafka JSON values.
 *
 * <p><b>Dynamic schema extraction:</b> This parser does NOT hardcode column names.
 * It uses a two-pass approach:
 * <ol>
 *   <li><b>First pass:</b> Extract the Debezium envelope ({@code op}, {@code before},
 *       {@code after}, {@code ts_ms}) using a minimal schema</li>
 *   <li><b>Second pass:</b> Dynamically extract all fields from the {@code after}
 *       (or {@code before} for deletes) payload by inspecting the JSON structure</li>
 * </ol>
 *
 * <p><b>Debezium envelope structure</b> (with {@code schemas.enable=false}):
 * <pre>
 * {
 *   "op": "c|u|d|r",
 *   "before": { ... },
 *   "after":  { ... },
 *   "ts_ms":  1234567890,
 *   "source": { ... }
 * }
 * </pre>
 *
 * <p>For INSERT/UPDATE/SNAPSHOT ({@code c, u, r}): uses {@code after}<br>
 * For DELETE ({@code d}): uses {@code before} (to get the PK for deletion)
 */
public class DebeziumEventParser {

    private static final Logger log = LoggerFactory.getLogger(DebeziumEventParser.class);

    private DebeziumEventParser() { /* stateless utility */ }

    /**
     * Parses a micro-batch DataFrame of raw Kafka messages into structured CDC events.
     *
     * <p>The output DataFrame contains:
     * <ul>
     *   <li>All columns from the source table (dynamically extracted)</li>
     *   <li>{@code _cdc_op} — the CDC operation type ({@code c/u/d/r})</li>
     *   <li>{@code _cdc_ts} — the event timestamp in milliseconds</li>
     *   <li>{@code _kafka_offset} — the Kafka offset for deduplication</li>
     *   <li>{@code topic} — the Kafka topic name (for grouping by source table)</li>
     * </ul>
     *
     * @param rawKafkaDF micro-batch DataFrame with columns: key, value, topic, offset, ...
     * @return structured DataFrame with flattened CDC fields
     */
    public static Dataset<Row> parse(Dataset<Row> rawKafkaDF) {
        log.debug("Parsing Debezium event envelopes (dynamic schema)");

        // Step 1: Extract the raw JSON and topic from Kafka
        Dataset<Row> withJson = rawKafkaDF
                .selectExpr(
                        "CAST(value AS STRING) as json_value",
                        "offset as _kafka_offset",
                        "topic"
                );

        // Step 2: Infer the Debezium envelope schema from the data
        // We parse the JSON to discover what columns the source table has
        Dataset<Row> withEnvelope = withJson
                .withColumn("parsed", from_json(col("json_value"), inferEnvelopeSchema(withJson)))
                .filter(col("parsed").isNotNull())
                .filter(col("parsed.op").isNotNull());

        // Step 3: Extract the record data based on operation type
        // Use COALESCE(after, before) so we get data for both normal ops and deletes
        Dataset<Row> withRecord = withEnvelope
                .withColumn("record",
                        when(col("parsed.op").equalTo("d"), col("parsed.before"))
                                .otherwise(col("parsed.after")));

        // Step 4: Flatten the record columns dynamically
        StructType recordSchema = getRecordSchema(withRecord);
        if (recordSchema == null || recordSchema.fields().length == 0) {
            log.warn("Could not determine record schema — returning empty dataset");
            return withRecord.limit(0);
        }

        List<Column> selectColumns = new ArrayList<>();
        for (StructField field : recordSchema.fields()) {
            selectColumns.add(col("record." + field.name()).alias(field.name()));
        }
        // Add CDC metadata columns
        selectColumns.add(col("parsed.op").alias("_cdc_op"));
        selectColumns.add(col("parsed.ts_ms").alias("_cdc_ts"));
        selectColumns.add(col("_kafka_offset"));
        selectColumns.add(col("topic"));

        return withRecord.select(selectColumns.toArray(new Column[0]));
    }

    /**
     * Infers the Debezium envelope schema by sampling the JSON data.
     * Uses schema_of_json to auto-detect the structure.
     */
    private static StructType inferEnvelopeSchema(Dataset<Row> withJson) {
        try {
            // Sample a single row to infer the schema
            Row sample = withJson.select("json_value")
                    .filter(col("json_value").isNotNull())
                    .first();

            if (sample != null) {
                String jsonStr = sample.getString(0);
                Dataset<Row> schemaDf = withJson.sparkSession()
                        .read().json(withJson.sparkSession()
                                .createDataset(
                                        java.util.Collections.singletonList(jsonStr),
                                        org.apache.spark.sql.Encoders.STRING()
                                ));
                return schemaDf.schema();
            }
        } catch (Exception e) {
            log.debug("Schema inference from sampling failed, using fallback: {}", e.getMessage());
        }

        // Fallback: minimal envelope schema with STRING fields (parsed dynamically)
        return new StructType()
                .add("op", DataTypes.StringType)
                .add("before", DataTypes.StringType)
                .add("after", DataTypes.StringType)
                .add("ts_ms", DataTypes.LongType);
    }

    /**
     * Extracts the schema of the "record" column from the DataFrame.
     */
    private static StructType getRecordSchema(Dataset<Row> df) {
        try {
            StructField recordField = df.schema().apply("record");
            if (recordField.dataType() instanceof StructType) {
                return (StructType) recordField.dataType();
            }
        } catch (Exception e) {
            log.debug("Could not extract record schema: {}", e.getMessage());
        }
        return null;
    }
}
