package com.abdelwahab.cdc_worker.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Centralised configuration loaded from environment variables.
 *
 * <p>Every configurable knob for the CDC worker lives here so that
 * individual classes never read {@code System.getenv} directly.
 * This makes the configuration surface explicit and testable.
 */
public final class CdcConfig {

    private static final Logger log = LoggerFactory.getLogger(CdcConfig.class);

    // ── Kafka ────────────────────────────────────────────────────────────────
    private final String kafkaBootstrapServers;
    private final String cdcTopicPrefix;
    private final String cdcTableIncludeList;

    // ── Spark Streaming ──────────────────────────────────────────────────────
    private final String cdcTriggerInterval;
    private final String cdcCheckpointDir;
    private final String cdcTargetNamespace;

    // ── Iceberg / Storage ────────────────────────────────────────────────────
    private final String icebergCatalogUri;
    private final String icebergWarehouse;
    private final String minioEndpoint;
    private final String awsAccessKeyId;
    private final String awsSecretAccessKey;
    private final String awsRegion;

    // ── Redis ────────────────────────────────────────────────────────────────
    private final String redisHost;
    private final int    redisPort;
    private final String redisPassword;

    // ── Compaction ───────────────────────────────────────────────────────────
    private final long compactionTargetFileSizeBytes;

    // ── Snapshot ─────────────────────────────────────────────────────────────
    private final int snapshotRetainDays;

    // ── Engine ───────────────────────────────────────────────────────────────
    private final String cdcEngine;

    public CdcConfig() {
        // Kafka
        this.kafkaBootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        this.cdcTopicPrefix        = env("CDC_TOPIC_PREFIX", "cdc");
        this.cdcTableIncludeList   = env("CDC_TABLE_INCLUDE_LIST", "public.customers");

        // Spark Streaming
        this.cdcTriggerInterval  = env("CDC_TRIGGER_INTERVAL", "30 seconds");
        this.cdcCheckpointDir    = env("CDC_CHECKPOINT_DIR", "s3a://warehouse/checkpoints/cdc");
        this.cdcTargetNamespace  = env("CDC_TARGET_NAMESPACE", "cdc_namespace");

        // Iceberg / Storage
        this.icebergCatalogUri   = env("ICEBERG_CATALOG_URI", "http://iceberg-rest-catalog:8181");
        this.icebergWarehouse    = env("ICEBERG_WAREHOUSE", "s3://warehouse");
        this.minioEndpoint       = env("MINIO_ENDPOINT", "http://minio:9000");
        this.awsAccessKeyId      = env("AWS_ACCESS_KEY_ID", "admin");
        this.awsSecretAccessKey  = env("AWS_SECRET_ACCESS_KEY", "changeme_in_production");
        this.awsRegion           = env("AWS_REGION", "us-east-1");

        // Redis
        this.redisHost     = env("REDIS_HOST", "redis");
        this.redisPort     = Integer.parseInt(env("REDIS_PORT", "6379"));
        this.redisPassword = env("REDIS_PASSWORD", "");

        // Compaction
        long defaultSize = 128L * 1024 * 1024; // 128 MiB
        this.compactionTargetFileSizeBytes = Long.parseLong(
                env("COMPACTION_TARGET_FILE_SIZE_BYTES", String.valueOf(defaultSize)));

        // Snapshot
        this.snapshotRetainDays = Integer.parseInt(env("SNAPSHOT_RETAIN_DAYS", "7"));

        // Engine
        this.cdcEngine = env("CDC_ENGINE", "spark");

        log.info("CdcConfig loaded — kafka={}, topic-prefix={}, trigger={}, namespace={}",
                kafkaBootstrapServers, cdcTopicPrefix, cdcTriggerInterval, cdcTargetNamespace);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }

    // ── Getters ──────────────────────────────────────────────────────────────

    public String getKafkaBootstrapServers()      { return kafkaBootstrapServers; }
    public String getCdcTopicPrefix()              { return cdcTopicPrefix; }
    public String getCdcTableIncludeList()         { return cdcTableIncludeList; }
    public String getCdcTriggerInterval()          { return cdcTriggerInterval; }
    public String getCdcCheckpointDir()            { return cdcCheckpointDir; }
    public String getCdcTargetNamespace()          { return cdcTargetNamespace; }
    public String getIcebergCatalogUri()           { return icebergCatalogUri; }
    public String getIcebergWarehouse()            { return icebergWarehouse; }
    public String getMinioEndpoint()               { return minioEndpoint; }
    public String getAwsAccessKeyId()              { return awsAccessKeyId; }
    public String getAwsSecretAccessKey()          { return awsSecretAccessKey; }
    public String getAwsRegion()                   { return awsRegion; }
    public String getRedisHost()                   { return redisHost; }
    public int    getRedisPort()                   { return redisPort; }
    public String getRedisPassword()               { return redisPassword; }
    public long   getCompactionTargetFileSizeBytes(){ return compactionTargetFileSizeBytes; }
    public int    getSnapshotRetainDays()           { return snapshotRetainDays; }
    public String getCdcEngine()                   { return cdcEngine; }

    /**
     * Derives the Kafka topic name for a given table.
     * Convention: {@code {topicPrefix}.{schema}.{table}} — e.g. {@code cdc.public.customers}.
     */
    public String getTopicForTable(String schemaTable) {
        return cdcTopicPrefix + "." + schemaTable;
    }
}
