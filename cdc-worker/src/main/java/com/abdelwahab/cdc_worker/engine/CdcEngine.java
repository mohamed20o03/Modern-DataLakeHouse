package com.abdelwahab.cdc_worker.engine;

/**
 * Lifecycle interface for CDC engine implementations.
 *
 * <p>Mirrors the existing {@code IngestionEngine} / {@code QueryEngine} pattern.
 * The engine is responsible for creating and managing the execution runtime
 * (e.g., a SparkSession) and coordinating the CDC streaming pipeline.
 *
 * <p><b>Contract:</b>
 * <ol>
 *   <li>{@link #init()} — one-time setup (SparkSession creation, warm-up)</li>
 *   <li>{@link #start(String[])} — start the streaming query or run a maintenance command</li>
 *   <li>{@link #stop()} — graceful shutdown (stop streaming query, close SparkSession)</li>
 * </ol>
 */
public interface CdcEngine {

    /** Initialise the engine (create runtime, warm up). */
    void init();

    /**
     * Start the engine.
     *
     * @param args CLI arguments — empty for streaming mode,
     *             or {@code --compact}, {@code --tag}, {@code --expire-snapshots}
     *             for maintenance mode.
     */
    void start(String[] args);

    /** Gracefully stop the engine and release resources. */
    void stop();
}
