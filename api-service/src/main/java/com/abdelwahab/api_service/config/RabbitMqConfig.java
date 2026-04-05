package com.abdelwahab.api_service.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ infrastructure configuration for asynchronous workflows.
 * <p>
 * This configuration defines the complete RabbitMQ topology including:
 * </p>
 * <ul>
 *   <li><b>3 Topic Exchanges:</b> One per workflow (ingestion, query, schema)</li>
 *   <li><b>3 Durable Queues:</b> Persist messages across broker restarts</li>
 *   <li><b>3 Bindings:</b> Route messages from exchanges to queues</li>
 *   <li><b>JSON Message Converter:</b> Automatic serialization using Jackson</li>
 * </ul>
 * <p>
 * Workflow Topology:
 * </p>
 * <pre>
 * API Service                          Workers
 * -----------                          -------
 * publishIngestionMessage() --&gt; ingestion.exchange --&gt; ingestion.queue --&gt; Ingestion Worker
 * publishQueryMessage()     --&gt; query.exchange     --&gt; query.queue     --&gt; Query Worker
 * publishSchemaMessage()    --&gt; schema.exchange    --&gt; schema.queue    --&gt; Schema Worker
 * </pre>
 * <p>
 * Topic exchanges allow flexible routing patterns for future expansion (e.g.,
 * routing by priority, user, or region using wildcard patterns).
 * </p>
 */
@Configuration
public class RabbitMqConfig {
    
    // Exchange names - used by MessagePublisherService
    public static final String INGESTION_EXCHANGE = "ingestion.exchange";
    public static final String QUERY_EXCHANGE = "query.exchange";
    public static final String CDC_EXCHANGE = "cdc.exchange";
    
    // Queue names - consumed by worker applications
    public static final String INGESTION_QUEUE = "ingestion.queue";
    public static final String QUERY_QUEUE = "query.queue";
    public static final String CDC_QUEUE = "cdc.connections.queue";
    // Note: schema jobs are routed through query.queue (SparkService dispatches on 'schema-' jobId prefix)
    
    // Routing keys - bind exchanges to queues
    public static final String INGESTION_ROUTING_KEY = "ingestion.upload";
    public static final String QUERY_ROUTING_KEY = "query.execute";
    public static final String CDC_ROUTING_KEY = "cdc.connection";
    
    // ==================== Exchanges ====================
    
    /**
     * Creates the ingestion topic exchange.
     * <p>
     * Messages published to this exchange with routing key "ingestion.upload"
     * will be routed to the ingestion queue.
     * </p>
     *
     * @return TopicExchange for ingestion workflow
     */
    @Bean
    public TopicExchange ingestionExchange() {
        return new TopicExchange(INGESTION_EXCHANGE);
    }
    
    /**
     * Creates the query topic exchange.
     * <p>
     * Messages published to this exchange with routing key "query.execute"
     * will be routed to the query queue.
     * </p>
     *
     * @return TopicExchange for query workflow
     */
    @Bean
    public TopicExchange queryExchange() {
        return new TopicExchange(QUERY_EXCHANGE);
    }

    /**
     * Creates the CDC topic exchange.
     * <p>
     * Messages published to this exchange with routing key "cdc.connection"
     * will be routed to the CDC connections queue.
     * </p>
     *
     * @return TopicExchange for CDC workflow
     */
    @Bean
    public TopicExchange cdcExchange() {
        return new TopicExchange(CDC_EXCHANGE);
    }
    
    // ==================== Queues ====================
    
    /**
     * Creates the ingestion queue (durable).
     * <p>
     * Durable queues survive broker restarts and ensure messages are not lost.
     * </p>
     *
     * @return durable Queue for ingestion messages
     */
    @Bean
    public Queue ingestionQueue() {
        return new Queue(INGESTION_QUEUE, true); // durable
    }
    
    /**
     * Creates the query queue (durable) with priority support.
     *
     * <p>{@code x-max-priority: 10} enables per-message priority on this queue.
     * RabbitMQ will reorder pending messages so higher-priority ones are delivered
     * to idle consumers first, regardless of arrival order.
     *
     * <p>Priority levels used by this system:
     * <ul>
     *   <li><b>8</b> — schema jobs (fast, user-facing, must not be blocked by data queries)</li>
     *   <li><b>1</b> — data queries (heavier, can wait behind schema requests)</li>
     * </ul>
     *
     * <p><b>Important:</b> if this queue already exists in RabbitMQ without
     * {@code x-max-priority}, it must be deleted once so RabbitMQ can re-declare
     * it with the new argument. Existing messages will be lost on that one deletion.
     *
     * @return durable, priority-enabled Queue for query and schema messages
     */
    @Bean
    public Queue queryQueue() {
        return QueueBuilder.durable(QUERY_QUEUE)
                .withArgument("x-max-priority", 10)  // 0 = lowest, 10 = highest
                .build();
    }

    /**
     * Creates the CDC connections queue (durable) with priority support.
     *
     * <p>CDC connection messages (CREATE/DELETE) are delivered to the cdc-worker.
     * Priority 10 allows urgent operations to be processed first.
     *
     * @return durable, priority-enabled Queue for CDC connection messages
     */
    @Bean
    public Queue cdcQueue() {
        return QueueBuilder.durable(CDC_QUEUE)
                .withArgument("x-max-priority", 10)
                .build();
    }
    
    // ==================== Bindings ====================
    
    /**
     * Binds ingestion queue to ingestion exchange.
     * <p>
     * Messages with routing key "ingestion.upload" will be routed to this queue.
     * </p>
     *
     * @param ingestionQueue    the target queue
     * @param ingestionExchange the source exchange
     * @return Binding configuration
     */
    @Bean
    public Binding ingestionBinding(Queue ingestionQueue, TopicExchange ingestionExchange) {
        return BindingBuilder.bind(ingestionQueue)
            .to(ingestionExchange)
            .with(INGESTION_ROUTING_KEY);
    }
    
    /**
     * Binds query queue to query exchange.
     *
     * @param queryQueue    the target queue
     * @param queryExchange the source exchange
     * @return Binding configuration
     */
    @Bean
    public Binding queryBinding(Queue queryQueue, TopicExchange queryExchange) {
        return BindingBuilder.bind(queryQueue)
            .to(queryExchange)
            .with(QUERY_ROUTING_KEY);
    }

    /**
     * Binds CDC queue to CDC exchange.
     *
     * @param cdcQueue    the target queue
     * @param cdcExchange the source exchange
     * @return Binding configuration
     */
    @Bean
    public Binding cdcBinding(Queue cdcQueue, TopicExchange cdcExchange) {
        return BindingBuilder.bind(cdcQueue)
            .to(cdcExchange)
            .with(CDC_ROUTING_KEY);
    }
    
    // ==================== Message Converter ====================
    
    /**
     * Configures JSON message converter for automatic serialization.
     * <p>
     * Uses Jackson to convert Java objects to JSON before publishing and
     * JSON to Java objects when consuming. This eliminates manual serialization
     * code and ensures consistent message format across all workflows.
     * </p>
     *
     * @return Jackson2JsonMessageConverter for automatic JSON serialization
     */
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
    
    /**
     * Configures RabbitTemplate with JSON message converter.
     * <p>
     * RabbitTemplate is used by {@link com.abdelwahab.api_service.common.messaging.RabbitMessageRepository}
     * to publish messages. The JSON converter ensures all messages are properly serialized.
     * </p>
     *
     * @param connectionFactory Spring AMQP connection factory
     * @param messageConverter JSON message converter bean
     * @return configured RabbitTemplate with JSON converter
     */
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, 
                                         MessageConverter messageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter);
        return template;
    }
}
