package com.abdelwahab.api_service.common.messaging;

/**
 * Abstraction for message broker backends.
 * <p>
 * This interface provides a contract for publishing messages regardless of the underlying
 * message broker implementation (RabbitMQ, Kafka, AWS SQS, etc.). This abstraction enables
 * easy migration between message brokers without changing business logic.
 * </p>
 */
public interface MessageRepository {
    
    /**
     * Publishes a message to a specific exchange/topic with a routing key.
     * <p>
     * The message payload will be automatically serialized to JSON before publishing.
     * Implementations should handle connection management and error handling.
     * </p>
     *
     * @param exchange   exchange or topic name (RabbitMQ exchange, Kafka topic, etc.)
     * @param routingKey routing key (RabbitMQ) or partition key (Kafka)
     * @param message    message payload to be serialized and published
     * @throws RuntimeException if message publishing fails
     */
    void publish(String exchange, String routingKey, Object message);

    /**
     * Publishes a message with an explicit broker-level priority.
     *
     * <p>The target queue must have been declared with {@code x-max-priority} set;
     * otherwise the priority header is silently ignored by the broker.
     *
     * @param exchange   exchange or topic name
     * @param routingKey routing key or partition key
     * @param message    message payload to be serialized and published
     * @param priority   message priority (0 = lowest, up to x-max-priority value)
     * @throws RuntimeException if message publishing fails
     */
    void publish(String exchange, String routingKey, Object message, int priority);
}
