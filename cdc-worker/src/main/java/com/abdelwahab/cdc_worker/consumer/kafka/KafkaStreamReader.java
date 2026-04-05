package com.abdelwahab.cdc_worker.consumer.kafka;

import com.abdelwahab.cdc_worker.config.CdcConfig;
import com.abdelwahab.cdc_worker.consumer.StreamReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads CDC events from Kafka topics as a Spark Structured Streaming source.
 *
 * <p><b>Dynamic topic discovery:</b> Uses {@code subscribePattern("cdc\\..*")}
 * to auto-discover new topics created by Debezium when new connectors are
 * registered. New topics are picked up on the next micro-batch without
 * restarting the streaming query.
 *
 * <p>The returned DataFrame has the standard Kafka source columns:<br>
 * {@code key, value, topic, partition, offset, timestamp, timestampType}
 *
 * <p>Downstream processing should cast {@code value} from binary to STRING
 * and then parse the Debezium JSON envelope. The {@code topic} column is
 * used to group events by source table for per-table MERGE INTO.
 */
public class KafkaStreamReader implements StreamReader {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamReader.class);

    private final String bootstrapServers;
    private final String topicPattern;

    public KafkaStreamReader(CdcConfig config) {
        this.bootstrapServers = config.getKafkaBootstrapServers();
        // Use pattern subscription for dynamic multi-table support
        // Matches all Debezium topics: cdc.public.customers, cdc.public.orders, etc.
        this.topicPattern = config.getCdcTopicPrefix() + "\\..*";
    }

    @Override
    public Dataset<Row> read(SparkSession spark) {
        log.info("Opening Kafka streaming source — servers={}, pattern={}",
                bootstrapServers, topicPattern);

        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribePattern", topicPattern)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();
    }
}
