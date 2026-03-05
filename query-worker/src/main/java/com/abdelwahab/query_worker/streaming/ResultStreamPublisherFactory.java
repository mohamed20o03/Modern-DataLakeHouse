package com.abdelwahab.query_worker.streaming;

import com.abdelwahab.query_worker.streaming.redis.RedisResultStreamPublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating {@link ResultStreamPublisher} instances.
 *
 * <p>Follows the same factory pattern used by
 * {@link com.abdelwahab.query_worker.status.JobStatusServiceFactory} and
 * {@link com.abdelwahab.query_worker.storage.StorageConfigFactory} — the concrete
 * implementation is selected at startup without coupling callers to a specific
 * streaming backend.
 */
public class ResultStreamPublisherFactory {

    private static final Logger log = LoggerFactory.getLogger(ResultStreamPublisherFactory.class);

    private ResultStreamPublisherFactory() { /* utility class */ }

    /**
     * Creates a Redis Streams–backed publisher.
     *
     * @param redisHost     hostname or IP of the Redis server
     * @param redisPort     port (default 6379)
     * @param redisPassword auth password; empty string for no auth
     * @return a ready-to-use {@link ResultStreamPublisher}
     */
    public static ResultStreamPublisher create(String redisHost, int redisPort, String redisPassword) {
        log.info("Creating RedisResultStreamPublisher — {}:{}", redisHost, redisPort);
        return new RedisResultStreamPublisher(redisHost, redisPort, redisPassword);
    }
}
