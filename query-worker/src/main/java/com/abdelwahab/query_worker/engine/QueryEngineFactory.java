package com.abdelwahab.query_worker.engine;

import com.abdelwahab.query_worker.engine.spark.SparkEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the appropriate {@link QueryEngine} implementation based on the
 * {@code QUERY_ENGINE} environment variable (default: {@code spark}).
 *
 * <p>This is a pure factory — no state, no instances.
 */
public class QueryEngineFactory {

    private static final Logger log = LoggerFactory.getLogger(QueryEngineFactory.class);

    private QueryEngineFactory() {}

    /**
     * @param icebergCatalogUri  REST catalog endpoint, e.g. {@code http://iceberg-catalog:8181}
     * @param icebergWarehouse   S3/MinIO warehouse prefix, e.g. {@code s3://warehouse}
     * @return configured {@link QueryEngine} ready to call {@link QueryEngine#initialize}
     */
    public static QueryEngine create(String icebergCatalogUri, String icebergWarehouse) {

        String type = System.getenv().getOrDefault("QUERY_ENGINE", "spark").toLowerCase();

        return switch (type) {
            case "spark" -> new SparkEngine(icebergCatalogUri, icebergWarehouse);
            // case "flink" -> new FlinkEngine(icebergCatalogUri, icebergWarehouse);

            default -> {
                log.warn("Unknown QUERY_ENGINE '{}', falling back to spark", type);
                yield new SparkEngine(icebergCatalogUri, icebergWarehouse);
            }
        };
    }
}
