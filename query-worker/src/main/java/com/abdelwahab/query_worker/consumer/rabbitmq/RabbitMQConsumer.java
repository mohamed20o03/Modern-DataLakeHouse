package com.abdelwahab.query_worker.consumer.rabbitmq;

import com.abdelwahab.query_worker.consumer.MessageConsumer;
import com.abdelwahab.query_worker.dto.QueryMessage;
import com.abdelwahab.query_worker.engine.QueryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMQ implementation of {@link MessageConsumer}.
 *
 * <p><b>Concurrency model:</b>
 * One TCP {@link Connection} is shared (RabbitMQ best practice — connections are
 * expensive). Each concurrency slot gets its own lightweight {@link Channel},
 * so {@code N} messages can be processed in parallel without contention.
 *
 * <pre>
 *   TCP Connection (1)
 *     ├─ Channel-0  →  Thread-0  →  queryService.query()
 *     ├─ Channel-1  →  Thread-1  →  queryService.query()
 *     └─ Channel-N  →  Thread-N  →  queryService.query()
 * </pre>
 *
 * <p><b>Acknowledgement strategy:</b>
 * Manual ACK ({@code autoAck=false}) + {@code basicQos(1)} per channel.
 * A message is ACKed only after {@code query()} returns successfully.
 * On failure, the message is NACKed and requeued once; on redeliver it is
 * dead-lettered (permanently rejected) to avoid infinite retry loops.
 *
 * <p><b>Fault tolerance:</b>
 * Automatic connection recovery is enabled ({@code setAutomaticRecoveryEnabled(true)})
 * so transient network partitions are healed without restarting the worker.
 *
 * @see MessageConsumerFactory — selects this class via {@code CONSUMER_TYPE=rabbitmq}
 */
public class RabbitMQConsumer implements MessageConsumer {
    
    private static final Logger log = LoggerFactory.getLogger(RabbitMQConsumer.class);
    private final String queueName;

    private final com.rabbitmq.client.Connection connection;
    private final List<com.rabbitmq.client.Channel> channels;
    private final ExecutorService executorService;
    private final QueryService queryService;
    private final ObjectMapper objectMapper;
    private final int concurrency;
    
    /**
     * Constructor with custom concurrency level.
     * 
     * @param concurrency Number of parallel consumer threads (channels)
     */
    public RabbitMQConsumer(String host,
                            int port,
                            String username,
                            String password,
                            QueryService queryService,
                            int concurrency,
                            String queueName) throws IOException, TimeoutException {

        this.queryService = queryService;
        this.concurrency = concurrency;
        this.queueName = queueName;
        this.channels = new ArrayList<>(concurrency);

        // Jackson mapper for deserialising the JSON message body into QueryMessage.
        // JavaTimeModule adds support for java.time types (Instant, LocalDate, etc.).
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());

        // Fixed-size thread pool — one thread per channel so channels never block each other.
        // This executor is also passed to the RabbitMQ ConnectionFactory so the client
        // uses our pool for internal I/O dispatch.
        this.executorService = Executors.newFixedThreadPool(concurrency);

        // One TCP connection shared by all channels (RabbitMQ best practice).
        // AutomaticRecovery re-establishes the connection + re-declares consumers
        // after a network blip without any manual intervention.
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5000); // retry reconnect every 5 s

        // Pass the executor so RabbitMQ uses our pool for consumer dispatch threads
        this.connection = factory.newConnection(executorService);

        // Create one channel per concurrency slot
        for (int i = 0; i < concurrency; i++) {
            com.rabbitmq.client.Channel channel = connection.createChannel();

            // queueDeclare is idempotent — safe to call even if the queue already exists,
            // BUT only when all arguments match the existing declaration exactly.
            // durable=true: queue survives broker restarts.
            // exclusive=false: shared across connections.
            // autoDelete=false: queue stays alive when consumers disconnect.
            // x-max-priority=10: must match the declaration in api-service's RabbitMqConfig.
            //   Enables per-message priority so the broker delivers schema messages (priority 8)
            //   ahead of queued data queries (priority 1) when a consumer becomes free.
            java.util.Map<String, Object> queueArgs = new java.util.HashMap<>();
            queueArgs.put("x-max-priority", 10);
            channel.queueDeclare(queueName, true, false, false, queueArgs);

            // basicQos(1): this channel will not receive a second message until it
            // has ACKed or NACKed the first one — prevents one slow job from
            // starving the rest of the thread pool.
            channel.basicQos(1);

            channels.add(channel);
        }

        log.info("RabbitMQ consumer initialized: host={}, queue={}, concurrency={}",
                host, queueName, concurrency);
    }

    @Override
    public void start() throws IOException {
        log.info("Starting {} consumer threads for queue: {}", concurrency, queueName);

        // Start consumer on each channel
        for (int i = 0; i < channels.size(); i++) {
            final int channelIndex = i;
            final com.rabbitmq.client.Channel channel = channels.get(i);
            
            // DeliveryCallback: Invoked by RabbitMQ when a message is successfully delivered to this consumer.
            // This is where the actual message processing logic happens - deserializing, processing, and acknowledging.
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String messageBody = new String(delivery.getBody(), StandardCharsets.UTF_8);
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                
                log.info("[Channel-{}] Received message: {}", channelIndex, messageBody);
                
                try {
                    // Deserialise the raw JSON body into a typed QueryMessage
                    QueryMessage message = objectMapper.readValue(messageBody, QueryMessage.class);

                    // Hand off to the engine (Spark / Flink). This call blocks the
                    // channel's thread until query execution completes or throws.
                    queryService.query(message);

                    // Positive ACK — tells RabbitMQ the message was processed
                    // successfully and can be removed from the queue.
                    // multiple=false: ACK only this single delivery tag.
                    channel.basicAck(deliveryTag, false);
                    log.info("[Channel-{}] ACK — jobId={}", channelIndex, message.getJobId());

                } catch (Exception e) {
                    log.error("[Channel-{}] Query execution failed for message: {}", channelIndex, messageBody, e);

                    // NACK strategy: requeue only on the FIRST delivery attempt.
                    // isRedeliver()==true means we already retried once — send to
                    // the dead-letter queue instead to avoid an infinite retry loop.
                    boolean requeue = !delivery.getEnvelope().isRedeliver();
                    channel.basicNack(deliveryTag, false, requeue);

                    if (!requeue) {
                        log.error("[Channel-{}] NACK (no requeue) — message dead-lettered: {}",
                                channelIndex, messageBody);
                    }
                }
            };
            
            // CancelCallback: Invoked by RabbitMQ when the consumer is cancelled (unexpectedly or explicitly).
            // This can happen when the queue is deleted, the channel is closed, or the consumer is cancelled by the broker.
            CancelCallback cancelCallback = consumerTag -> {
                log.warn("[Channel-{}] Consumer cancelled: {}", channelIndex, consumerTag);
            };
            
            // Start consuming (auto-ack disabled, uses executor for parallel processing)
            channel.basicConsume(queueName, false, deliverCallback, cancelCallback);
            log.info("[Channel-{}] Consumer started", channelIndex);
        }
        
        log.info("All {} consumer threads are running", concurrency);
    }
    
    @Override
    public void close() {
        log.info("Shutting down RabbitMQ consumer...");
        
        try {
            // Shutdown executor service
            executorService.shutdown();
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
            
            // Close all channels
            for (int i = 0; i < channels.size(); i++) {
                com.rabbitmq.client.Channel channel = channels.get(i);
                if (channel != null && channel.isOpen()) {
                    channel.close();
                    log.info("[Channel-{}] Closed", i);
                }
            }
            
            // Close connection
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
            
            log.info("RabbitMQ consumer closed successfully");
        } catch (Exception e) {
            log.error("Error closing RabbitMQ consumer", e);
        }
    }
}
