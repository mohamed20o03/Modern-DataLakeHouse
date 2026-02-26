package com.abdelwahab.api_service.common.messaging;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * RabbitMQ-backed implementation of {@link MessageRepository}.
 * <p>
 * This implementation uses Spring AMQP's RabbitTemplate for publishing messages.
 * Messages are automatically serialized to JSON using the configured message converter
 * (Jackson2JsonMessageConverter). Connection pooling and retry logic are handled by
 * the underlying RabbitTemplate configuration.
 * </p>
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class RabbitMessageRepository implements MessageRepository {
    
    private final RabbitTemplate rabbitTemplate;
    
    /**
     * Publishes a message to RabbitMQ exchange with specified routing key.
     *
     * @param exchange   RabbitMQ exchange name
     * @param routingKey routing key for message routing
     * @param message    message payload (will be serialized to JSON)
     * @throws RuntimeException if publishing fails
     */
    @Override
    public void publish(String exchange, String routingKey, Object message) {
        try {
            rabbitTemplate.convertAndSend(exchange, routingKey, message);
            log.debug("Published message to RabbitMQ: exchange={}, routingKey={}, messageType={}",
                      exchange, routingKey, message.getClass().getSimpleName());
        } catch (Exception e) {
            log.error("Failed to publish message to RabbitMQ: exchange={}, routingKey={}",
                      exchange, routingKey, e);
            throw new RuntimeException("Failed to publish message to RabbitMQ", e);
        }
    }

    /**
     * Publishes a message with an explicit RabbitMQ message priority.
     *
     * <p>Sets the AMQP {@code priority} header via a {@code MessagePostProcessor}
     * so the broker can re-order pending messages before dispatch.
     * The target queue must have been declared with {@code x-max-priority};
     * otherwise the header is silently ignored.
     *
     * @param exchange   RabbitMQ exchange name
     * @param routingKey routing key for message routing
     * @param message    message payload (will be serialized to JSON)
     * @param priority   AMQP priority value (0 = lowest, up to x-max-priority of the queue)
     * @throws RuntimeException if publishing fails
     */
    @Override
    public void publish(String exchange, String routingKey, Object message, int priority) {
        try {
            rabbitTemplate.convertAndSend(exchange, routingKey, message, msg -> {
                msg.getMessageProperties().setPriority(priority);
                return msg;
            });
            log.debug("Published priority-{} message to RabbitMQ: exchange={}, routingKey={}, messageType={}",
                      priority, exchange, routingKey, message.getClass().getSimpleName());
        } catch (Exception e) {
            log.error("Failed to publish priority-{} message to RabbitMQ: exchange={}, routingKey={}",
                      priority, exchange, routingKey, e);
            throw new RuntimeException("Failed to publish message to RabbitMQ", e);
        }
    }
}
