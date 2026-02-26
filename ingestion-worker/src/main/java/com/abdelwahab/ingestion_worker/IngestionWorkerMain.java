package com.abdelwahab.ingestion_worker;

import com.abdelwahab.ingestion_worker.cache.RedisSchemaCacheService;
import com.abdelwahab.ingestion_worker.cache.SchemaCacheService;
import com.abdelwahab.ingestion_worker.consumer.MessageConsumer;
import com.abdelwahab.ingestion_worker.consumer.MessageConsumerFactory;
import com.abdelwahab.ingestion_worker.engine.IngestionEngine;
import com.abdelwahab.ingestion_worker.engine.IngestionService;
import com.abdelwahab.ingestion_worker.engine.IngestionEngineFactory;
import com.abdelwahab.ingestion_worker.status.JobStatusService;
import com.abdelwahab.ingestion_worker.status.JobStatusServiceFactory;
import com.abdelwahab.ingestion_worker.storage.StorageConfig;
import com.abdelwahab.ingestion_worker.storage.StorageConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the Ingestion Worker.
 * Wires all factories together and starts the message consumer.
 * No engine-specific code lives here — all Spark/Flink details stay in their engine packages.
 */
public class IngestionWorkerMain {

    private static final Logger log = LoggerFactory.getLogger(IngestionWorkerMain.class);

    public static void main(String[] args) {
        log.info("Starting Ingestion Worker...");

        // Read environment variables
        String rabbitmqHost     = getEnv("RABBITMQ_HOST",          "localhost");
        int    rabbitmqPort     = Integer.parseInt(getEnv("RABBITMQ_PORT", "5672"));
        String rabbitmqUsername = getEnv("RABBITMQ_USERNAME",       "guest");
        String rabbitmqPassword = getEnv("RABBITMQ_PASSWORD",       "guest");

        String redisHost     = getEnv("REDIS_HOST",     "localhost");
        int    redisPort     = Integer.parseInt(getEnv("REDIS_PORT", "6379"));
        String redisPassword = getEnv("REDIS_PASSWORD", "");

        String icebergCatalogUri    = getEnv("ICEBERG_CATALOG_URI",       "http://localhost:8181");
        String icebergWarehouse     = getEnv("ICEBERG_WAREHOUSE",         "s3://warehouse");
        String minioEndpoint        = getEnv("MINIO_ENDPOINT",            "http://localhost:9000");
        String awsAccessKey         = getEnv("AWS_ACCESS_KEY_ID",         "minioadmin");
        String awsSecretKey         = getEnv("AWS_SECRET_ACCESS_KEY",     "minioadmin");
        String awsRegion            = getEnv("AWS_REGION",                "us-east-1");
        String minioBucketUploads   = getEnv("MINIO_BUCKET_UPLOADS",      "staging-applicationarea");
        String minioBucketWarehouse = getEnv("MINIO_BUCKET_WAREHOUSE",    "warehouse");

        IngestionEngine  engine          = null;
        JobStatusService jobStatusService = null;
        SchemaCacheService schemaCache    = null;
        MessageConsumer  consumer        = null;

        try {
            // 1. Create StorageConfig (MinIO or S3, driven by STORAGE_TYPE env var)
            StorageConfig storageConfig = StorageConfigFactory.create(
                    minioEndpoint, awsAccessKey, awsSecretKey,
                    awsRegion, minioBucketUploads, minioBucketWarehouse);

            // 2. Create JobStatusService (async Redis, driven by REDIS_MODE env var)
            jobStatusService = JobStatusServiceFactory.create(redisHost, redisPort, redisPassword);

            // 3. Create SchemaCacheService — used to invalidate cached schemas after ingestion
            schemaCache = new RedisSchemaCacheService(redisHost, redisPort, redisPassword);

            // 4. Create IngestionEngine (Spark / Flink, driven by INGESTION_ENGINE env var)
            //    The engine is not started yet — initialize() builds the session internally.
            engine = IngestionEngineFactory.create(icebergCatalogUri, icebergWarehouse);

            // 5. Initialize the engine — returns a ready IngestionService
            log.info("Initializing ingestion engine...");
            IngestionService ingestionService = engine.initialize(jobStatusService, storageConfig, schemaCache);

            // 6. Create MessageConsumer (RabbitMQ / Kafka, driven by CONSUMER_TYPE env var)
            int concurrency = Integer.parseInt(getEnv("WORKER_CONCURRENCY", "1"));
            consumer = MessageConsumerFactory.create(
                    rabbitmqHost, rabbitmqPort, rabbitmqUsername, rabbitmqPassword,
                    ingestionService, concurrency);

            // 7. Register graceful shutdown hook
            final IngestionEngine   finalEngine           = engine;
            final JobStatusService  finalJobStatusService = jobStatusService;
            final SchemaCacheService finalSchemaCache      = schemaCache;
            final MessageConsumer   finalConsumer         = consumer;

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down Ingestion Worker...");

                if (finalConsumer != null) {
                    try { finalConsumer.close();           log.info("Consumer closed"); }
                    catch (Exception e) { log.error("Error closing consumer", e); }
                }

                if (finalJobStatusService != null) {
                    try { finalJobStatusService.close();   log.info("JobStatusService closed"); }
                    catch (Exception e) { log.error("Error closing JobStatusService", e); }
                }

                if (finalSchemaCache != null) {
                    try { finalSchemaCache.close();        log.info("SchemaCacheService closed"); }
                    catch (Exception e) { log.error("Error closing SchemaCacheService", e); }
                }

                if (finalEngine != null) {
                    try { finalEngine.close();             log.info("Engine closed"); }
                    catch (Exception e) { log.error("Error closing engine", e); }
                }

                log.info("Ingestion Worker stopped");
            }));

            // 8. Start consuming messages
            log.info("Starting message consumer...");
            consumer.start();

            log.info("Ingestion Worker is running. Press Ctrl+C to stop.");

            // 8. Keep main thread alive
            Thread.currentThread().join();

        } catch (Exception e) {
            log.error("Fatal error in Ingestion Worker", e);
            System.exit(1);
        }
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }
}
