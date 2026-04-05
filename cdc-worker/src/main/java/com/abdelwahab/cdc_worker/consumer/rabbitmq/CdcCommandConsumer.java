package com.abdelwahab.cdc_worker.consumer.rabbitmq;

import com.abdelwahab.cdc_worker.config.CdcConfig;
import com.abdelwahab.cdc_worker.registry.ConnectionRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMQ consumer for CDC connection lifecycle commands (CREATE / DELETE).
 *
 * <p>Runs in a background daemon thread, listening on {@code cdc.connections.queue}.
 * When a message arrives:
 * <ul>
 *   <li><b>CREATE:</b> Registers the connection in {@link ConnectionRegistry}
 *       and updates Redis status to SNAPSHOTTING</li>
 *   <li><b>DELETE:</b> Unregisters the connection from the registry and
 *       updates Redis status to DELETED</li>
 * </ul>
 *
 * <p>This consumer does NOT manage Kafka subscriptions — Kafka topic discovery
 * is handled automatically by {@code subscribePattern("cdc\\..*")} in the
 * streaming query.
 */
public class CdcCommandConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CdcCommandConsumer.class);
    private static final String CDC_QUEUE = "cdc.connections.queue";

    private final CdcConfig config;
    private final ConnectionRegistry registry;
    private final ObjectMapper objectMapper;

    private Connection rabbitConnection;
    private Channel channel;
    private volatile boolean running = false;

    public CdcCommandConsumer(CdcConfig config, ConnectionRegistry registry) {
        this.config = config;
        this.registry = registry;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Starts consuming messages in a daemon background thread.
     */
    public void start() {
        Thread consumerThread = new Thread(this::consume, "cdc-command-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
        log.info("CdcCommandConsumer started in background thread");
    }

    private void consume() {
        try {
            String rabbitHost = config.getRabbitHost();
            int rabbitPort = config.getRabbitPort();
            String rabbitUser = config.getRabbitUsername();
            String rabbitPass = config.getRabbitPassword();

            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(rabbitHost);
            factory.setPort(rabbitPort);
            factory.setUsername(rabbitUser);
            factory.setPassword(rabbitPass);
            factory.setAutomaticRecoveryEnabled(true);

            log.info("Connecting to RabbitMQ: {}:{}", rabbitHost, rabbitPort);
            this.rabbitConnection = factory.newConnection("cdc-worker-command-consumer");
            this.channel = rabbitConnection.createChannel();

            // Declare queue (idempotent — no-op if already exists with same args)
            channel.queueDeclare(CDC_QUEUE, true, false, false,
                    java.util.Map.of("x-max-priority", 10));

            // Prefetch 1 message at a time
            channel.basicQos(1);

            running = true;
            log.info("CdcCommandConsumer listening on queue: {}", CDC_QUEUE);

            channel.basicConsume(CDC_QUEUE, false, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) {
                    try {
                        String json = new String(body, StandardCharsets.UTF_8);
                        log.debug("Received CDC command message: {}", json);

                        processMessage(json);

                        channel.basicAck(envelope.getDeliveryTag(), false);
                    } catch (Exception e) {
                        log.error("Error processing CDC command message", e);
                        try {
                            // Reject and requeue on failure
                            channel.basicNack(envelope.getDeliveryTag(), false, true);
                        } catch (IOException ioEx) {
                            log.error("Failed to nack message", ioEx);
                        }
                    }
                }
            });

        } catch (IOException | TimeoutException e) {
            log.error("Failed to start CdcCommandConsumer", e);
            throw new RuntimeException("Failed to start CDC command consumer", e);
        }
    }

    /**
     * Processes a single CDC command message.
     */
    private void processMessage(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            String action = root.path("action").asText();
            String connectionId = root.path("connectionId").asText();
            String topicName = root.path("topicName").asText();
            String targetTable = root.path("targetTable").asText();
            String primaryKeyColumn = root.path("primaryKeyColumn").asText();
            String sourceTable = root.path("sourceTable").asText();

            switch (action) {
                case "CREATE" -> {
                    log.info("Processing CREATE: connectionId={}, topic={}", connectionId, topicName);
                    registry.register(connectionId, topicName, targetTable,
                            primaryKeyColumn, sourceTable);
                    registry.updateStatus(connectionId, "SNAPSHOTTING",
                            "Connection registered in worker — awaiting first batch");
                }
                case "DELETE" -> {
                    log.info("Processing DELETE: connectionId={}, topic={}", connectionId, topicName);
                    registry.unregister(topicName);
                    registry.updateStatus(connectionId, "DELETED",
                            "Connection removed from worker");
                }
                default -> log.warn("Unknown CDC action: {}", action);
            }
        } catch (Exception e) {
            log.error("Failed to parse CDC command message: {}", json, e);
        }
    }

    @Override
    public void close() {
        running = false;
        try {
            if (channel != null && channel.isOpen()) channel.close();
            if (rabbitConnection != null && rabbitConnection.isOpen()) rabbitConnection.close();
            log.info("CdcCommandConsumer closed");
        } catch (Exception e) {
            log.error("Error closing CdcCommandConsumer", e);
        }
    }
}
