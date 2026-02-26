package com.abdelwahab.ingestion_worker.engine;

import com.abdelwahab.ingestion_worker.engine.spark.SparkEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating IngestionEngine instances.
 * Reads INGESTION_ENGINE environment variable to decide which implementation to use.
 *
 * <p>The returned {@link IngestionEngine} is not yet started — call
 * {@link IngestionEngine#initialize(com.abdelwahab.ingestion_worker.status.JobStatusService,
 * com.abdelwahab.ingestion_worker.storage.StorageConfig)} to build
 * the internal session and obtain a ready {@link IngestionService}.
 *
 * <p>Supported engines:
 * <ul>
 *   <li>{@code spark} (default) - Apache Spark ingestion engine</li>
 *   <li>{@code flink} - Apache Flink ingestion engine (future implementation)</li>
 * </ul>
 *
 * <p>Usage: set env variable {@code INGESTION_ENGINE=spark} or {@code INGESTION_ENGINE=flink}
 */
public class IngestionEngineFactory {

    private static final Logger log = LoggerFactory.getLogger(IngestionEngineFactory.class);

    /** Utility class — no instances. */
    private IngestionEngineFactory() {}

    /**
     * Creates an uninitialised {@link IngestionEngine} for the requested type.
     *
     * <p>The engine is chosen by the {@code INGESTION_ENGINE} environment variable:
     * <ul>
     *   <li>{@code spark} (default) — {@link SparkEngine}</li>
     *   <li>{@code flink}           — not yet implemented</li>
     * </ul>
     *
     * @param icebergCatalogUri Iceberg REST catalog endpoint
     *                          (e.g. {@code http://iceberg-rest:8181})
     * @param icebergWarehouse  S3/MinIO warehouse root URI
     *                          (e.g. {@code s3://warehouse})
     * @return a freshly constructed engine — call {@code initialize()} before use
     */
    public static IngestionEngine create(String icebergCatalogUri, String icebergWarehouse) {
        // Read which engine implementation to use from the environment
        String engine = System.getenv().getOrDefault("INGESTION_ENGINE", "spark").toLowerCase();

        log.info("Creating IngestionEngine: type={}", engine);

        return switch (engine) {
            // Spark: distributed batch processing with Iceberg ACID writes
            case "spark" -> new SparkEngine(icebergCatalogUri, icebergWarehouse);

            // case "flink" -> new FlinkEngine(icebergCatalogUri, icebergWarehouse);

            default -> {
                // Unknown engine — warn and fall back so the worker can still start
                log.warn("Unknown INGESTION_ENGINE '{}', falling back to spark", engine);
                yield new SparkEngine(icebergCatalogUri, icebergWarehouse);
            }
        };
    }
}
