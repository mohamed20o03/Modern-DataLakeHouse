package com.abdelwahab.cdc_worker.engine.spark;

import com.abdelwahab.cdc_worker.config.CdcConfig;
import com.abdelwahab.cdc_worker.consumer.kafka.KafkaStreamReader;
import com.abdelwahab.cdc_worker.consumer.rabbitmq.CdcCommandConsumer;
import com.abdelwahab.cdc_worker.engine.CdcEngine;
import com.abdelwahab.cdc_worker.maintenance.CompactionService;
import com.abdelwahab.cdc_worker.maintenance.SnapshotManager;
import com.abdelwahab.cdc_worker.registry.ConnectionRegistry;
import com.abdelwahab.cdc_worker.status.redis.AsyncRedisJobStatusService;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark-based implementation of {@link CdcEngine}.
 *
 * <p><b>Lifecycle:</b>
 * <ol>
 *   <li>{@link #init()} — creates SparkSession, runs warm-up query,
 *       initialises ConnectionRegistry and CdcCommandConsumer</li>
 *   <li>{@link #start(String[])} — routes to streaming or maintenance mode based on CLI args</li>
 *   <li>{@link #stop()} — stops streaming query, consumers, and closes SparkSession</li>
 * </ol>
 *
 * <p>This class owns the SparkSession lifecycle. It delegates streaming to
 * {@link CdcStreamProcessor} and maintenance to {@link CompactionService}
 * and {@link SnapshotManager}.
 */
public class SparkCdcEngine implements CdcEngine {

    private static final Logger log = LoggerFactory.getLogger(SparkCdcEngine.class);

    private final CdcConfig config;
    private SparkSession spark;
    private CdcStreamProcessor streamProcessor;
    private AsyncRedisJobStatusService statusService;
    private ConnectionRegistry connectionRegistry;
    private CdcCommandConsumer cdcCommandConsumer;

    public SparkCdcEngine(CdcConfig config) {
        this.config = config;
    }

    @Override
    public void init() {
        log.info("Initialising Spark CDC engine");

        // SparkSession is configured via spark-submit (entrypoint.sh);
        // getOrCreate() picks up those settings.
        this.spark = SparkSession.builder()
                .appName("cdc-worker")
                .getOrCreate();

        // Warm-up query — forces lazy Spark initialisation to happen now
        spark.sql("SELECT 1").collect();
        log.info("SparkSession warm-up complete");

        // Redis status service
        this.statusService = new AsyncRedisJobStatusService(
                config.getRedisHost(),
                config.getRedisPort(),
                config.getRedisPassword()
        );

        // ConnectionRegistry — load active connections from Redis on startup
        this.connectionRegistry = new ConnectionRegistry(config);
        connectionRegistry.loadFromRedis();
        log.info("ConnectionRegistry initialised and loaded from Redis");

        // CdcCommandConsumer — start RabbitMQ listener in background thread
        this.cdcCommandConsumer = new CdcCommandConsumer(config, connectionRegistry);
        cdcCommandConsumer.start();
        log.info("CdcCommandConsumer started");

        log.info("Spark CDC engine initialised");
    }

    @Override
    public void start(String[] args) {
        if (args.length > 0) {
            routeMaintenanceCommand(args);
        } else {
            startStreaming();
        }
    }

    @Override
    public void stop() {
        log.info("Stopping Spark CDC engine");
        try {
            if (streamProcessor != null) {
                streamProcessor.stop();
            }
            if (cdcCommandConsumer != null) {
                cdcCommandConsumer.close();
            }
            if (connectionRegistry != null) {
                connectionRegistry.close();
            }
            if (statusService != null) {
                statusService.close();
            }
            if (spark != null) {
                spark.stop();
            }
        } catch (Exception e) {
            log.error("Error during Spark CDC engine shutdown", e);
        }
    }

    // ── Private ─────────────────────────────────────────────────────────────

    private void startStreaming() {
        log.info("Starting CDC streaming pipeline");
        statusService.writeStatus("cdc-worker", "STARTING", "Initialising streaming pipeline");

        var reader = new KafkaStreamReader(config);
        var writer = new IcebergMergeWriter(connectionRegistry);

        this.streamProcessor = new CdcStreamProcessor(config, spark, reader, writer, statusService);
        streamProcessor.start();
    }

    private void routeMaintenanceCommand(String[] args) {
        String command = args[0];
        switch (command) {
            case "--compact" -> {
                if (args.length < 2) {
                    log.error("Usage: --compact <tableName>");
                    System.exit(1);
                }
                String tableName = args[1];
                log.info("Running compaction on table: {}", tableName);

                var compaction = new CompactionService(spark, statusService);
                compaction.compact(tableName, config.getCompactionTargetFileSizeBytes());
            }
            case "--tag" -> {
                if (args.length < 3) {
                    log.error("Usage: --tag <tableName> <tagName> [snapshotId]");
                    System.exit(1);
                }
                String tableName = args[1];
                String tagName = args[2];
                Long snapshotId = args.length > 3 ? Long.parseLong(args[3]) : null;

                var snapshotManager = new SnapshotManager(spark);
                snapshotManager.createTag(tableName, tagName, snapshotId, null);
            }
            case "--expire-snapshots" -> {
                if (args.length < 2) {
                    log.error("Usage: --expire-snapshots <tableName> [retainDays]");
                    System.exit(1);
                }
                String tableName = args[1];
                int retainDays = args.length > 2
                        ? Integer.parseInt(args[2])
                        : config.getSnapshotRetainDays();

                var snapshotManager = new SnapshotManager(spark);
                long retainMillis = retainDays * 24L * 60 * 60 * 1000;
                snapshotManager.expireSnapshots(tableName, retainMillis);
            }
            default -> {
                log.error("Unknown command: {}. Supported: --compact, --tag, --expire-snapshots", command);
                System.exit(1);
            }
        }
    }
}
