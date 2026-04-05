package com.abdelwahab.cdc_worker.engine.spark;

import com.abdelwahab.cdc_worker.config.CdcConfig;
import com.abdelwahab.cdc_worker.consumer.StreamReader;
import com.abdelwahab.cdc_worker.status.redis.AsyncRedisJobStatusService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the CDC streaming pipeline: read → parse → deduplicate → merge.
 *
 * <p><b>Processing flow within each micro-batch:</b>
 * <ol>
 *   <li>{@link StreamReader#read(SparkSession)} — raw Kafka events</li>
 *   <li>{@link DebeziumEventParser#parse(Dataset)} — extract structured CDC fields</li>
 *   <li>{@link EventDeduplicator#deduplicate(Dataset)} — collapse duplicates per PK</li>
 *   <li>{@link IcebergMergeWriter#call(Dataset, Long)} — MERGE INTO Iceberg table</li>
 * </ol>
 *
 * <p>Steps 2-4 happen inside the {@code foreachBatch} callback. The
 * {@link IcebergMergeWriter} already calls the parser and deduplicator
 * internally, so this class just wires the reader to the writer and
 * configures the streaming query.
 */
public class CdcStreamProcessor {

    private static final Logger log = LoggerFactory.getLogger(CdcStreamProcessor.class);

    private final CdcConfig config;
    private final SparkSession spark;
    private final StreamReader reader;
    private final IcebergMergeWriter writer;
    private final AsyncRedisJobStatusService statusService;
    private volatile StreamingQuery query;

    public CdcStreamProcessor(CdcConfig config,
                               SparkSession spark,
                               StreamReader reader,
                               IcebergMergeWriter writer,
                               AsyncRedisJobStatusService statusService) {
        this.config        = config;
        this.spark         = spark;
        this.reader        = reader;
        this.writer        = writer;
        this.statusService = statusService;
    }

    /**
     * Starts the streaming query and blocks until termination.
     */
    public void start() {
        log.info("Starting CDC stream processor — trigger={}, checkpoint={}",
                config.getCdcTriggerInterval(), config.getCdcCheckpointDir());

        try {
            Dataset<Row> rawStream = reader.read(spark);

            this.query = rawStream.writeStream()
                    .foreachBatch(writer)
                    .option("checkpointLocation", config.getCdcCheckpointDir())
                    .trigger(Trigger.ProcessingTime(config.getCdcTriggerInterval()))
                    .start();

            statusService.writeStatus("cdc-worker", "RUNNING",
                    "Streaming pipeline active — consuming from Kafka");
            log.info("CDC streaming query started — awaiting termination");

            query.awaitTermination();

        } catch (org.apache.spark.sql.streaming.StreamingQueryException | java.util.concurrent.TimeoutException e) {
            log.error("CDC stream processor failed", e);
            statusService.writeStatus("cdc-worker", "FAILED",
                    "Stream processing error: " + e.getMessage());
            throw new RuntimeException("CDC streaming failed", e);
        }
    }

    public void stop() {
        if (query != null && query.isActive()) {
            log.info("Stopping streaming query");
            try {
                query.stop();
            } catch (java.util.concurrent.TimeoutException e) {
                log.warn("Timeout while stopping streaming query", e);
            }
            statusService.writeStatus("cdc-worker", "STOPPED", "Streaming query stopped");
        }
    }
}
