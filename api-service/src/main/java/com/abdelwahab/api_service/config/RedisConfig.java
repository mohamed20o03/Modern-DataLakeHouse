package com.abdelwahab.api_service.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Redis configuration for job status tracking.
 * <p>
 * This configuration sets up Redis connectivity using Lettuce (the default Spring Data Redis client)
 * and provides a StringRedisTemplate for Hash operations. Redis is used to store asynchronous
 * job status with automatic expiration (TTL).
 * </p>
 * <p>
 * Benefits of Lettuce:
 * </p>
 * <ul>
 *   <li>Thread-safe connection sharing for better resource utilization</li>
 *   <li>Built-in connection pooling with automatic recovery</li>
 *   <li>Async/reactive API support (not used currently but available for future)</li>
 *   <li>Lower memory footprint compared to Jedis</li>
 * </ul>
 * <p>
 * Configuration properties:
 * </p>
 * <ul>
 *   <li><b>redis.host:</b> Redis server hostname (default: localhost)</li>
 *   <li><b>redis.port:</b> Redis server port (default: 6379)</li>
 * </ul>
 */
@Configuration
public class RedisConfig {

    @Value("${redis.host:localhost}")
    private String redisHost;

    @Value("${redis.port:6379}")
    private int redisPort;

    @Value("${redis.password:}")
    private String redisPassword;

    /**
     * Creates Lettuce-based Redis connection factory.
     * <p>
     * Lettuce provides a thread-safe, non-blocking connection that can be shared
     * across multiple threads. Connection pooling and retry logic are handled
     * automatically.
     * </p>
     *
     * @return RedisConnectionFactory using Lettuce client
     */
    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName(redisHost);
        config.setPort(redisPort);
        if (redisPassword != null && !redisPassword.isEmpty()) {
            config.setPassword(redisPassword);
        }
        return new LettuceConnectionFactory(config);
    }

    /**
     * Creates StringRedisTemplate for Hash operations.
     * <p>
     * StringRedisTemplate is optimized for String keys and values, avoiding
     * Java serialization overhead. Used by {@link com.abdelwahab.api_service.jobstatus.repository.RedisJobStatusRepository}
     * to store job status as Redis Hashes.
     * </p>
     * <p>
     * Why StringRedisTemplate:
     * </p>
     * <ul>
     *   <li>No Java serialization - stores data as plain strings</li>
     *   <li>Human-readable in Redis CLI (useful for debugging)</li>
     *   <li>Compatible with non-Java Redis clients</li>
     *   <li>Better performance than default RedisTemplate</li>
     * </ul>
     *
     * @param connectionFactory Lettuce connection factory
     * @return StringRedisTemplate for Hash operations
     */
    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
    }

    /**
     * Creates a Redis Pub/Sub message listener container.
     * <p>
     * This container manages subscriptions for job-completion events published
     * by the workers on {@code job-done:{jobId}} channels.  Listeners are added
     * and removed dynamically by {@link com.abdelwahab.api_service.jobstatus.listener.RedisJobCompletionListener}.
     * </p>
     *
     * @param connectionFactory Lettuce connection factory
     * @return RedisMessageListenerContainer for Pub/Sub subscriptions
     */
    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory connectionFactory) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        return container;
    }
}
