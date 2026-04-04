package com.abdelwahab.cdc_worker.consumer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Abstraction over the streaming source that produces CDC events.
 *
 * <p>The single method {@link #read(SparkSession)} returns a streaming
 * {@link Dataset} that the pipeline consumes via {@code writeStream().foreachBatch()}.
 *
 * <p>The interface exists so the CDC pipeline can be tested with alternative
 * sources (e.g., a file-based stream reader for integration tests) without
 * modifying the processing logic.
 */
public interface StreamReader {

    /**
     * Opens a streaming source and returns a raw (unparsed) streaming DataFrame.
     *
     * @param spark active SparkSession
     * @return a streaming Dataset containing raw CDC event data
     */
    Dataset<Row> read(SparkSession spark);
}
