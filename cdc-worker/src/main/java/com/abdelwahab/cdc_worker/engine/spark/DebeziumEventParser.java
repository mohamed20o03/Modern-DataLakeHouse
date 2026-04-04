package com.abdelwahab.cdc_worker.engine.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

/**
 * Parses Debezium CDC event envelopes from raw Kafka JSON values.
 *
 * <p><b>Responsibility</b>: Transform the raw Kafka {@code value} (JSON string)
 * into a structured DataFrame with flattened columns:
 * {@code id, name, email, status, created_at, updated_at, _cdc_op, _cdc_ts, _kafka_offset}
 *
 * <p><b>Debezium envelope structure</b>:
 * <pre>
 * {
 *   "payload": {
 *     "op": "c|u|d|r",
 *     "before": { ... },
 *     "after":  { ... },
 *     "ts_ms":  1234567890
 *   }
 * }
 * </pre>
 *
 * <p>For INSERT/UPDATE/SNAPSHOT ({@code c, u, r}): uses {@code payload.after}<br>
 * For DELETE ({@code d}): uses {@code payload.before} (to get the PK for deletion)
 */
public class DebeziumEventParser {

    private static final Logger log = LoggerFactory.getLogger(DebeziumEventParser.class);

    /** Schema for the row data fields inside before/after. */
    private static final StructType ROW_SCHEMA = new StructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("name", DataTypes.StringType, true),
            DataTypes.createStructField("email", DataTypes.StringType, true),
            DataTypes.createStructField("status", DataTypes.StringType, true),
            DataTypes.createStructField("created_at", DataTypes.LongType, true),
            DataTypes.createStructField("updated_at", DataTypes.LongType, true)
    });

    /** Schema for the Debezium payload envelope. */
    private static final StructType PAYLOAD_SCHEMA = new StructType(new StructField[]{
            DataTypes.createStructField("op", DataTypes.StringType, true),
            DataTypes.createStructField("before", ROW_SCHEMA, true),
            DataTypes.createStructField("after", ROW_SCHEMA, true),
            DataTypes.createStructField("ts_ms", DataTypes.LongType, true)
    });

    /** Top-level schema wrapping the payload. */
    private static final StructType ENVELOPE_SCHEMA = new StructType(new StructField[]{
            DataTypes.createStructField("payload", PAYLOAD_SCHEMA, true)
    });

    private DebeziumEventParser() { /* stateless utility */ }

    /**
     * Parses a streaming DataFrame of raw Kafka messages into structured CDC events.
     *
     * @param rawKafkaDF streaming DataFrame with columns: key, value, offset, ...
     * @return streaming DataFrame with columns: id, name, email, status, created_at,
     *         updated_at, _cdc_op, _cdc_ts, _kafka_offset
     */
    public static Dataset<Row> parse(Dataset<Row> rawKafkaDF) {
        log.debug("Parsing Debezium event envelopes");

        // Cast binary value to string and parse JSON envelope
        Dataset<Row> parsed = rawKafkaDF
                .selectExpr("CAST(value AS STRING) as json_value", "offset as _kafka_offset")
                // With value.converter.schemas.enable=false, the value IS the payload
                .withColumn("envelope", from_json(col("json_value"), PAYLOAD_SCHEMA))
                .filter(col("envelope").isNotNull())
                .filter(col("envelope.op").isNotNull());

        // Extract the row data based on operation type:
        // - For c/u/r: use "after"
        // - For d: use "before"
        return parsed.select(
                when(col("envelope.op").equalTo("d"),
                        col("envelope.before.id"))
                        .otherwise(col("envelope.after.id"))
                        .alias("id"),
                when(col("envelope.op").equalTo("d"),
                        col("envelope.before.name"))
                        .otherwise(col("envelope.after.name"))
                        .alias("name"),
                when(col("envelope.op").equalTo("d"),
                        col("envelope.before.email"))
                        .otherwise(col("envelope.after.email"))
                        .alias("email"),
                when(col("envelope.op").equalTo("d"),
                        col("envelope.before.status"))
                        .otherwise(col("envelope.after.status"))
                        .alias("status"),
                when(col("envelope.op").equalTo("d"),
                        col("envelope.before.created_at"))
                        .otherwise(col("envelope.after.created_at"))
                        .alias("created_at"),
                when(col("envelope.op").equalTo("d"),
                        col("envelope.before.updated_at"))
                        .otherwise(col("envelope.after.updated_at"))
                        .alias("updated_at"),
                col("envelope.op").alias("_cdc_op"),
                col("envelope.ts_ms").alias("_cdc_ts"),
                col("_kafka_offset")
        ).filter(col("id").isNotNull()); // Skip events without a valid PK
    }
}
