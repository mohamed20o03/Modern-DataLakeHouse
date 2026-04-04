package com.abdelwahab.cdc_worker;

import com.abdelwahab.cdc_worker.config.CdcConfig;
import com.abdelwahab.cdc_worker.engine.CdcEngine;
import com.abdelwahab.cdc_worker.engine.CdcEngineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the CDC Worker application.
 *
 * <p>Follows the same plain-Java startup pattern as the existing
 * ingestion-worker and query-worker: no Spring, no framework —
 * just a {@code main()} that wires factories and manages the lifecycle.
 *
 * <p><b>Modes of operation:</b>
 * <ul>
 *   <li><b>Streaming</b> (no args): Starts a Spark Structured Streaming
 *       query that continuously processes CDC events from Kafka.</li>
 *   <li><b>Compaction</b> ({@code --compact <table>}): Runs a one-shot
 *       {@code RewriteDataFiles} job, then exits.</li>
 *   <li><b>Tagging</b> ({@code --tag <table> <tagName> [snapshotId]}):
 *       Creates a named tag on an Iceberg snapshot, then exits.</li>
 *   <li><b>Expiration</b> ({@code --expire-snapshots <table> [retainDays]}):
 *       Expires old snapshots while preserving tagged ones, then exits.</li>
 * </ul>
 */
public class CdcWorkerMain {

    private static final Logger log = LoggerFactory.getLogger(CdcWorkerMain.class);

    public static void main(String[] args) {
        log.info("=== CDC Worker Starting ===");

        CdcConfig config = new CdcConfig();
        CdcEngine engine = CdcEngineFactory.create(config);

        // Register shutdown hook for graceful stop
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered — stopping engine");
            try {
                engine.stop();
            } catch (Exception e) {
                log.error("Error during engine shutdown", e);
            }
        }));

        try {
            engine.init();
            engine.start(args);
        } catch (Exception e) {
            log.error("CDC Worker failed to start", e);
            System.exit(1);
        }

        log.info("=== CDC Worker Finished ===");
    }
}
