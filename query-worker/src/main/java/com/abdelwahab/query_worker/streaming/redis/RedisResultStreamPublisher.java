package com.abdelwahab.query_worker.streaming.redis;

import com.abdelwahab.query_worker.streaming.ResultStreamPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Redis Streams implementation of {@link ResultStreamPublisher}.
 *
 * <p>Uses the Redis Streams data structure ({@code XADD}) to publish result
 * chunks as ordered, persistent entries.  Unlike Pub/Sub, Redis Streams
 * retain entries — consumers that connect late can read from the beginning
 * without missing any data.
 *
 * <p><b>Stream key format:</b> {@code query-result-stream:{jobId}}
 *
 * <p><b>Entry types:</b>
 * <pre>
 *   type=metadata  columns=[JSON array of column names]
 *   type=batch     batchIndex=N  rows=[JSON array of row objects]
 *   type=complete  totalBatches=N  totalRows=N
 * </pre>
 *
 * <p>Each stream key is given a 1-hour TTL after the completion marker is
 * published, matching the job-hash TTL.
 *
 * @see ResultStreamPublisher — the contract this class fulfils
 */
public class RedisResultStreamPublisher implements ResultStreamPublisher {

    private static final Logger log = LoggerFactory.getLogger(RedisResultStreamPublisher.class);

    /** All result streams share this prefix. */
    static final String STREAM_PREFIX = "query-result-stream:";

    /** TTL for the stream key — matches the job-hash TTL in the API service. */
    private static final long STREAM_TTL_SECONDS = 3600; // 1 hour

    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;
    private final ObjectMapper objectMapper;

    /**
     * Opens a Lettuce connection dedicated to stream publishing.
     *
     * @param redisHost     hostname or IP of the Redis server
     * @param redisPort     port (default 6379)
     * @param redisPassword auth password; empty string for no auth
     */
    public RedisResultStreamPublisher(String redisHost, int redisPort, String redisPassword) {
        log.info("Connecting ResultStreamPublisher to Redis: {}:{}", redisHost, redisPort);

        String redisUri = redisPassword.isEmpty()
                ? String.format("redis://%s:%d", redisHost, redisPort)
                : String.format("redis://%s@%s:%d", redisPassword, redisHost, redisPort);

        this.redisClient   = RedisClient.create(redisUri);
        this.connection    = redisClient.connect();
        this.commands      = connection.sync();
        this.objectMapper  = new ObjectMapper();

        log.info("ResultStreamPublisher connected to Redis");
    }

    @Override
    public void publishMetadata(String jobId, String[] columns) {
        try {
            String streamKey  = STREAM_PREFIX + jobId;
            String columnsJson = objectMapper.writeValueAsString(columns);

            commands.xadd(streamKey,
                    Map.of("type", "metadata",
                           "columns", columnsJson));

            log.debug("Published metadata — jobId={}, columns={}", jobId, columns.length);
        } catch (Exception e) {
            log.error("Failed to publish metadata — jobId={}", jobId, e);
            throw new RuntimeException("Stream metadata publish failed", e);
        }
    }

    @Override
    public void publishBatch(String jobId, int batchIndex, List<Map<String, Object>> rows) {
        try {
            String streamKey = STREAM_PREFIX + jobId;
            String rowsJson  = objectMapper.writeValueAsString(rows);

            commands.xadd(streamKey,
                    Map.of("type", "batch",
                           "batchIndex", String.valueOf(batchIndex),
                           "rowCount", String.valueOf(rows.size()),
                           "rows", rowsJson));

            log.debug("Published batch — jobId={}, batchIndex={}, rows={}",
                    jobId, batchIndex, rows.size());
        } catch (Exception e) {
            log.error("Failed to publish batch — jobId={}, batchIndex={}", jobId, batchIndex, e);
            throw new RuntimeException("Stream batch publish failed", e);
        }
    }

    @Override
    public void publishComplete(String jobId, int totalBatches, long totalRows) {
        try {
            String streamKey = STREAM_PREFIX + jobId;

            commands.xadd(streamKey,
                    Map.of("type", "complete",
                           "totalBatches", String.valueOf(totalBatches),
                           "totalRows", String.valueOf(totalRows)));

            // Set TTL on the stream so it auto-expires
            commands.expire(streamKey, STREAM_TTL_SECONDS);

            log.info("Published completion — jobId={}, totalBatches={}, totalRows={}",
                    jobId, totalBatches, totalRows);
        } catch (Exception e) {
            log.error("Failed to publish completion — jobId={}", jobId, e);
            throw new RuntimeException("Stream completion publish failed", e);
        }
    }

    @Override
    public void close() {
        try {
            if (connection != null) connection.close();
            if (redisClient != null) redisClient.shutdown();
            log.info("ResultStreamPublisher closed");
        } catch (Exception e) {
            log.error("Error closing ResultStreamPublisher", e);
        }
    }
}
