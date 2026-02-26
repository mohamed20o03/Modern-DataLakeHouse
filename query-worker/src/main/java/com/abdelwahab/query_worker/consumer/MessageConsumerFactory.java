package com.abdelwahab.query_worker.consumer;

import com.abdelwahab.query_worker.consumer.rabbitmq.RabbitMQConsumer;
import com.abdelwahab.query_worker.engine.QueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating MessageConsumer instances.
 * Reads CONSUMER_TYPE environment variable to decide which implementation to use.
 *
 * <p>Supported types:
 * <ul>
 *   <li>{@code rabbitmq} (default) - RabbitMQ consumer</li>
 *   <li>{@code kafka} - Kafka consumer (future implementation)</li>
 * </ul>
 *
 * <p>Usage: set env variable {@code CONSUMER_TYPE=rabbitmq} or {@code CONSUMER_TYPE=kafka}
 */
public class MessageConsumerFactory {

    private static final Logger log = LoggerFactory.getLogger(MessageConsumerFactory.class);

    private MessageConsumerFactory() {}

    /**
     * Creates a {@link MessageConsumer} wired to the given broker credentials.
     *
     * <p>The implementation is chosen by the {@code CONSUMER_TYPE} environment variable:
     * <ul>
     *   <li>{@code rabbitmq} (default) — {@link RabbitMQConsumer}</li>
     *   <li>{@code kafka}             — not yet implemented</li>
     * </ul>
     *
     * <p>The queue / topic name is read from {@code RABBITMQ_QUEUE}
     * (default: {@code "query.queue"}).
     *
     * @param host             broker hostname
     * @param port             broker port
     * @param username         broker username
     * @param password         broker password
     * @param queryService     service that processes each received message
     * @param concurrency      number of parallel consumer channels / threads
     * @return a fully connected, ready-to-start consumer
     * @throws Exception if the broker connection cannot be established
     */
    public static MessageConsumer create(String host, int port, String username,
            String password, QueryService queryService, int concurrency) throws Exception {

        // Read which broker type to use — defaults to RabbitMQ for local/dev environments
        String type      = System.getenv().getOrDefault("CONSUMER_TYPE",  "rabbitmq").toLowerCase();

        // Queue name can be overridden per environment (staging vs. production)
        String queueName = System.getenv().getOrDefault("RABBITMQ_QUEUE", "query.queue");

        log.info("Creating MessageConsumer: type={}, queue={}", type, queueName);

        return switch (type) {
            // RabbitMQ: one TCP connection, N channels (one per concurrency slot)
            case "rabbitmq" -> new RabbitMQConsumer(host, port, username, password,
                    queryService, concurrency, queueName);

            // case "kafka" -> new KafkaConsumer(bootstrap, ingestionService, concurrency, queueName);

            default -> {
                // Unknown type — warn and fall back so the worker still starts
                log.warn("Unknown CONSUMER_TYPE '{}', falling back to rabbitmq", type);
                yield new RabbitMQConsumer(host, port, username, password,
                        queryService, concurrency, queueName);
            }
        };
    }
}
