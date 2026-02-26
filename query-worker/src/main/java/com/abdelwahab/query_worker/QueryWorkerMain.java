package com.abdelwahab.query_worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abdelwahab.query_worker.cache.RedisSchemaCacheService;
import com.abdelwahab.query_worker.cache.SchemaCacheService;
import com.abdelwahab.query_worker.consumer.MessageConsumer;
import com.abdelwahab.query_worker.consumer.MessageConsumerFactory;
import com.abdelwahab.query_worker.engine.QueryEngine;
import com.abdelwahab.query_worker.engine.QueryEngineFactory;
import com.abdelwahab.query_worker.engine.QueryService;
import com.abdelwahab.query_worker.status.JobStatusService;
import com.abdelwahab.query_worker.status.JobStatusServiceFactory;
import com.abdelwahab.query_worker.storage.StorageConfig;
import com.abdelwahab.query_worker.storage.StorageConfigFactory;

public class QueryWorkerMain {

	private static final Logger log = LoggerFactory.getLogger(QueryWorkerMain.class);

	public static void main(String[] args) {
		log.info("Starting Query Worker");

		// ── Environment variables ─────────────────────────────────────────────
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
		int    concurrency          = Integer.parseInt(getEnv("CONSUMER_CONCURRENCY", "2"));
		// ─────────────────────────────────────────────────────────────────────

		QueryEngine      engine           = null;
		JobStatusService jobStatusService = null;
		SchemaCacheService schemaCache    = null;
		MessageConsumer  consumer         = null;

		try {
			StorageConfig storageConfig = StorageConfigFactory.create(
					minioEndpoint, awsAccessKey, awsSecretKey,
					awsRegion, minioBucketUploads, minioBucketWarehouse);

			jobStatusService = JobStatusServiceFactory.create(redisHost, redisPort, redisPassword);

			schemaCache = new RedisSchemaCacheService(redisHost, redisPort, redisPassword);

			engine = QueryEngineFactory.create(icebergCatalogUri, icebergWarehouse);

			log.info("Initializing query engine...");
			QueryService queryService = engine.initialize(jobStatusService, storageConfig, schemaCache);

			consumer = MessageConsumerFactory.create(
					rabbitmqHost, rabbitmqPort, rabbitmqUsername, rabbitmqPassword,
					queryService, concurrency);

			// ── Graceful shutdown hook ────────────────────────────────────────
			// Capture final references for the lambda (must be effectively final)
			final QueryEngine      engineRef    = engine;
			final JobStatusService statusRef    = jobStatusService;
			final SchemaCacheService cacheRef   = schemaCache;
			final MessageConsumer  consumerRef  = consumer;

			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				log.info("Shutdown signal received — draining consumer...");
				try { consumerRef.close(); } catch (Exception e) { log.warn("Error closing consumer", e); }
				try { engineRef.close();   } catch (Exception e) { log.warn("Error closing engine", e); }
				try { statusRef.close();   } catch (Exception e) { log.warn("Error closing status service", e); }
				try { cacheRef.close();    } catch (Exception e) { log.warn("Error closing schema cache", e); }
				log.info("Query Worker stopped cleanly.");
			}, "shutdown-hook"));
			// ─────────────────────────────────────────────────────────────────

			consumer.start();
			log.info("Query Worker is running — waiting for messages.");

			// Block the main thread so the JVM stays alive
			Thread.currentThread().join();

		} catch (Exception e) {
			log.error("Fatal error in Query Worker", e);
			System.exit(1);
		}
	}

	private static String getEnv(String name, String defaultValue) {
		String value = System.getenv(name);
		return value != null ? value : defaultValue;
	}
}
