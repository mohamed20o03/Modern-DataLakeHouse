package com.abdelwahab.cdc_worker.consumer.kafka;

import com.abdelwahab.cdc_worker.config.CdcConfig;
import com.abdelwahab.cdc_worker.consumer.StreamReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads CDC events from a Kafka topic as a Spark Structured Streaming source.
 *
 * <p>The returned DataFrame has the standard Kafka source columns:<br>
 * {@code key, value, topic, partition, offset, timestamp, timestampType}
 *
 * <p>Downstream processing should cast {@code value} from binary to STRING
 * and then parse the Debezium JSON envelope.
 */
public class KafkaStreamReader implements StreamReader {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamReader.class);

    private final String bootstrapServers;
    private final String topic;

    public KafkaStreamReader(CdcConfig config) {
        this.bootstrapServers = config.getKafkaBootstrapServers();
        // Derive topic from the first table in the include list
        // e.g. "public.customers" → "cdc.public.customers"
        String firstTable = config.getCdcTableIncludeList().split(",")[0].trim();
        this.topic = config.getTopicForTable(firstTable);
    }

    @Override
    public Dataset<Row> read(SparkSession spark) {
        log.info("Opening Kafka streaming source — servers={}, topic={}", bootstrapServers, topic);

        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load();
    }
}
