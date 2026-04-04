package com.abdelwahab.cdc_worker.engine.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

/**
 * Deduplicates CDC events within a single micro-batch by primary key.
 *
 * <p><b>Why deduplication is needed:</b> Kafka provides at-least-once delivery.
 * Within a single micro-batch, the same PK may appear multiple times due to:
 * <ul>
 *   <li>Rapid consecutive updates to the same row</li>
 *   <li>Consumer rebalancing causing message replay</li>
 *   <li>Debezium connector restart replaying recent events</li>
 * </ul>
 *
 * <p><b>Strategy:</b> For each PK, keep only the event with the highest
 * Kafka offset ({@code _kafka_offset}). This is deterministic and respects
 * the total ordering within a partition. Falls back to {@code _cdc_ts}
 * (Debezium timestamp) if offset is unavailable.
 */
public class EventDeduplicator {

    private static final Logger log = LoggerFactory.getLogger(EventDeduplicator.class);

    private EventDeduplicator() { /* stateless utility */ }

    /**
     * Deduplicates a batch DataFrame by {@code id}, keeping the latest event.
     *
     * @param batchDF DataFrame with columns: id, name, email, ..., _cdc_op, _cdc_ts, _kafka_offset
     * @return deduplicated DataFrame — each PK appears at most once
     */
    public static Dataset<Row> deduplicate(Dataset<Row> batchDF) {
        long inputCount = batchDF.count();
        if (inputCount == 0) {
            log.debug("Empty batch — nothing to deduplicate");
            return batchDF;
        }

        // Window: partition by PK, order by offset DESC (latest first)
        WindowSpec window = Window
                .partitionBy("id")
                .orderBy(col("_kafka_offset").desc(), col("_cdc_ts").desc());

        Dataset<Row> deduped = batchDF
                .withColumn("_row_num", row_number().over(window))
                .filter(col("_row_num").equalTo(1))
                .drop("_row_num");

        long outputCount = deduped.count();
        if (inputCount != outputCount) {
            log.info("Deduplication: {} events → {} unique PKs ({} duplicates removed)",
                    inputCount, outputCount, inputCount - outputCount);
        }

        return deduped;
    }
}
