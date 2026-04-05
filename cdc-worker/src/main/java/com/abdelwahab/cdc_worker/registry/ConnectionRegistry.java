package com.abdelwahab.cdc_worker.registry;

import com.abdelwahab.cdc_worker.config.CdcConfig;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe registry of CDC connection metadata, keyed by Kafka topic name.
 *
 * <p><b>Data flow:</b>
 * <ol>
 *   <li>The api-service stores connection metadata in Redis as {@code cdc-conn:{connectionId}} hashes</li>
 *   <li>The {@link com.abdelwahab.cdc_worker.consumer.rabbitmq.CdcCommandConsumer} receives
 *       CREATE/DELETE messages and registers/unregisters entries here</li>
 *   <li>The {@link com.abdelwahab.cdc_worker.engine.spark.IcebergMergeWriter} looks up PK
 *       and target table info by Kafka topic name</li>
 * </ol>
 *
 * <p><b>Backward compatibility:</b> For topics without a Redis entry (e.g., the
 * existing {@code cdc.public.customers} from spec 001), the registry falls back
 * to default config values from {@link CdcConfig}.
 */
public class ConnectionRegistry implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConnectionRegistry.class);
    private static final String CDC_CONN_PREFIX = "cdc-conn:";

    /**
     * Metadata for a single CDC connection, cached in memory.
     */
    public static class ConnectionMeta {
        public final String connectionId;
        public final String topicName;
        public final String targetTable;
        public final String primaryKeyColumn;
        public final String sourceTable;

        public ConnectionMeta(String connectionId, String topicName,
                              String targetTable, String primaryKeyColumn,
                              String sourceTable) {
            this.connectionId = connectionId;
            this.topicName = topicName;
            this.targetTable = targetTable;
            this.primaryKeyColumn = primaryKeyColumn;
            this.sourceTable = sourceTable;
        }
    }

    private final CdcConfig config;
    private final ConcurrentHashMap<String, ConnectionMeta> topicToMeta = new ConcurrentHashMap<>();

    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> redisConnection;
    private final RedisCommands<String, String> redisCommands;

    public ConnectionRegistry(CdcConfig config) {
        this.config = config;

        // Connect to Redis
        String redisUri = config.getRedisPassword().isEmpty()
                ? String.format("redis://%s:%d", config.getRedisHost(), config.getRedisPort())
                : String.format("redis://%s@%s:%d", config.getRedisPassword(),
                        config.getRedisHost(), config.getRedisPort());

        this.redisClient = RedisClient.create(redisUri);
        this.redisConnection = redisClient.connect();
        this.redisCommands = redisConnection.sync();

        log.info("ConnectionRegistry initialised — connected to Redis");
    }

    /**
     * Registers a connection from a RabbitMQ CREATE message.
     */
    public void register(String connectionId, String topicName,
                         String targetTable, String primaryKeyColumn,
                         String sourceTable) {
        ConnectionMeta meta = new ConnectionMeta(connectionId, topicName,
                targetTable, primaryKeyColumn, sourceTable);
        topicToMeta.put(topicName, meta);
        log.info("Registered connection: topic={}, target={}, pk={}",
                topicName, targetTable, primaryKeyColumn);
    }

    /**
     * Unregisters a connection from a RabbitMQ DELETE message.
     */
    public void unregister(String topicName) {
        ConnectionMeta removed = topicToMeta.remove(topicName);
        if (removed != null) {
            log.info("Unregistered connection: topic={}", topicName);
        }
    }

    /**
     * Looks up metadata for a Kafka topic.
     *
     * <p><b>Fallback for backward compatibility:</b> If no explicit entry exists
     * (e.g., for the existing hardcoded {@code cdc.public.customers}), returns
     * default metadata derived from the topic name and CdcConfig.
     *
     * @param topicName the Kafka topic name (e.g., {@code cdc.public.orders})
     * @return connection metadata, never null
     */
    public ConnectionMeta getMetaForTopic(String topicName) {
        ConnectionMeta meta = topicToMeta.get(topicName);
        if (meta != null) {
            return meta;
        }

        // Fallback: derive metadata from topic name
        // Topic format: cdc.{schema}.{table} → table name is the last segment
        String[] parts = topicName.split("\\.");
        String tableName = parts.length >= 3 ? parts[parts.length - 1] : topicName;
        String targetTable = "iceberg." + config.getCdcTargetNamespace() + "." + tableName;
        // Default PK = "id" for backward compat with customers table
        String pk = "id";

        log.debug("No registry entry for topic {} — using fallback (pk={}, target={})",
                topicName, pk, targetTable);

        return new ConnectionMeta(null, topicName, targetTable, pk,
                topicName.replace(config.getCdcTopicPrefix() + ".", ""));
    }

    /**
     * Checks if a topic is registered (or is a known legacy topic).
     */
    public boolean isTopicActive(String topicName) {
        if (topicToMeta.containsKey(topicName)) {
            return true;
        }
        // Legacy topics (from hardcoded connector) are always active
        String legacyTopic = config.getTopicForTable(config.getCdcTableIncludeList().split(",")[0].trim());
        return topicName.equals(legacyTopic);
    }

    /**
     * Reloads all active connections from Redis on startup.
     * Called during worker initialisation to recover state after a restart.
     */
    public void loadFromRedis() {
        log.info("Loading active connections from Redis...");
        try {
            var keys = redisCommands.keys(CDC_CONN_PREFIX + "*");
            int loaded = 0;

            for (String key : keys) {
                Map<String, String> fields = redisCommands.hgetall(key);
                String status = fields.get("status");

                // Only load active connections (not DELETED or FAILED)
                if (status != null && !status.equals("DELETED")
                        && !status.endsWith("_FAILED")) {

                    String connId = fields.get("connectionId");
                    String topic = fields.get("topicName");
                    String target = fields.get("targetTable");
                    String pk = fields.get("primaryKeyColumn");
                    String src = fields.get("sourceTable");

                    if (topic != null && target != null && pk != null) {
                        register(connId, topic, target, pk, src);
                        loaded++;
                    }
                }
            }
            log.info("Loaded {} active connections from Redis", loaded);
        } catch (Exception e) {
            log.error("Failed to load connections from Redis", e);
        }
    }

    /**
     * Updates connection status in Redis.
     */
    public void updateStatus(String connectionId, String status, String message) {
        if (connectionId == null) return;
        try {
            String key = CDC_CONN_PREFIX + connectionId;
            redisCommands.hset(key, "status", status);
            redisCommands.hset(key, "updatedAt", java.time.Instant.now().toString());
            if (message != null) {
                redisCommands.hset(key, "message", message);
            }
            log.debug("Updated status: connectionId={}, status={}", connectionId, status);
        } catch (Exception e) {
            log.error("Failed to update status in Redis: connectionId={}", connectionId, e);
        }
    }

    @Override
    public void close() {
        try {
            if (redisConnection != null) redisConnection.close();
            if (redisClient != null) redisClient.shutdown();
            log.info("ConnectionRegistry closed");
        } catch (Exception e) {
            log.error("Error closing ConnectionRegistry", e);
        }
    }
}
