package com.abdelwahab.cdc_worker.engine;

import com.abdelwahab.cdc_worker.config.CdcConfig;
import com.abdelwahab.cdc_worker.engine.spark.SparkCdcEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory that selects the {@link CdcEngine} implementation based on the
 * {@code CDC_ENGINE} environment variable.
 *
 * <p>Currently only {@code "spark"} is supported. The factory pattern
 * mirrors the existing {@code IngestionEngineFactory} and
 * {@code QueryEngineFactory} so all workers share a consistent startup
 * convention.
 */
public final class CdcEngineFactory {

    private static final Logger log = LoggerFactory.getLogger(CdcEngineFactory.class);

    private CdcEngineFactory() { /* factory class */ }

    /**
     * Creates the appropriate engine based on configuration.
     *
     * @param config CDC configuration
     * @return a fully-constructed (but not yet initialised) engine
     * @throws IllegalArgumentException if the configured engine type is unknown
     */
    public static CdcEngine create(CdcConfig config) {
        String engineType = config.getCdcEngine().toLowerCase();
        log.info("Creating CDC engine: type={}", engineType);

        return switch (engineType) {
            case "spark" -> new SparkCdcEngine(config);
            default -> throw new IllegalArgumentException(
                    "Unknown CDC engine type: " + engineType
                    + ". Supported: spark");
        };
    }
}
