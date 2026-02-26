package com.abdelwahab.query_worker.cache;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis implementation of {@link SchemaCacheService}.
 *
 * <p><b>Key format:</b> {@code schema:{icebergTable}}
 * — e.g. {@code schema:iceberg.myproject.sales_data}
 *
 * <p><b>No TTL</b> is applied: entries are invalidated explicitly by the
 * ingestion worker after a successful table write.
 *
 * <p>Uses the Lettuce <em>synchronous</em> API. A single GET/SET/DEL is
 * a single RTT (~0.1 ms on localhost), so blocking is acceptable here —
 * this is not on the hot path for query execution.
 */
public class RedisSchemaCacheService implements SchemaCacheService {

    private static final Logger log = LoggerFactory.getLogger(RedisSchemaCacheService.class);

    private static final String KEY_PREFIX = "schema:";

    private final RedisClient                             redisClient;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String>           commands;

    /**
     * Opens a synchronous Lettuce connection to the given Redis instance.
     *
     * @param redisHost     Redis hostname
     * @param redisPort     Redis port (default 6379)
     * @param redisPassword auth password; pass an empty string for no auth
     */
    public RedisSchemaCacheService(String redisHost, int redisPort, String redisPassword) {
        String uri = redisPassword.isEmpty()
                ? String.format("redis://%s:%d", redisHost, redisPort)
                : String.format("redis://%s@%s:%d", redisPassword, redisHost, redisPort);

        this.redisClient = RedisClient.create(uri);
        this.connection  = redisClient.connect();
        this.commands    = connection.sync();
        log.info("SchemaCacheService connected to Redis: {}:{}", redisHost, redisPort);
    }

    /** {@inheritDoc} */
    @Override
    public String get(String icebergTable) {
        String value = commands.get(KEY_PREFIX + icebergTable);
        if (value != null) {
            log.info("Schema cache HIT  — table={}", icebergTable);
        } else {
            log.info("Schema cache MISS — table={}", icebergTable);
        }
        return value;
    }

    /** {@inheritDoc} */
    @Override
    public void put(String icebergTable, String schemaJson) {
        commands.set(KEY_PREFIX + icebergTable, schemaJson);
        log.info("Schema cached — table={}", icebergTable);
    }

    /** {@inheritDoc} */
    @Override
    public void invalidate(String icebergTable) {
        Long removed = commands.del(KEY_PREFIX + icebergTable);
        if (removed != null && removed > 0) {
            log.info("Schema cache invalidated — table={}", icebergTable);
        }
    }

    /** Closes the Lettuce connection and shuts down the client. */
    @Override
    public void close() {
        try {
            if (connection != null) connection.close();
            if (redisClient != null) redisClient.shutdown();
            log.info("SchemaCacheService closed");
        } catch (Exception e) {
            log.warn("Error closing SchemaCacheService", e);
        }
    }
}
