package com.abdelwahab.api_service.common.messaging;

import java.time.Instant;

import org.springframework.stereotype.Service;

import com.abdelwahab.api_service.common.dto.IngestionMessage;
import com.abdelwahab.api_service.common.dto.QueryMessage;
import com.abdelwahab.api_service.common.dto.SchemaMessage;
import com.abdelwahab.api_service.config.RabbitMqConfig;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Business logic layer for publishing messages to the message broker.
 * <p>
 * This service provides validation, enrichment, and error handling for messages
 * before they are published. It encapsulates workflow-specific logic and acts as
 * a facade over the message repository layer.
 * </p>
 * <p>
 * Available workflows:
 * </p>
 * <ul>
 *   <li><b>Ingestion:</b> File upload and data ingestion processing</li>
 *   <li><b>Query:</b> Data query execution and result retrieval</li>
 *   <li><b>Schema:</b> Schema metadata retrieval and synchronization</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessagePublisherService {
    
    private final MessageRepository repository;
    
    /**
     * Publishes an ingestion message to the ingestion workflow queue.
     * <p>
     * Automatically sets timestamp if not provided and validates required fields.
     * </p>
     * 
     * @param message ingestion message containing job ID, stored file path, and metadata
     * @throws IllegalArgumentException if jobId or storedPath are missing
     * @throws RuntimeException if publishing fails
     */
    public void publishIngestionMessage(IngestionMessage message) {
        try {
            // Enrich message with timestamp if not already set
            if (message.getTimestamp() == null) {
                message.setTimestamp(Instant.now().toString());
            }
            
            // Validate required fields
            if (message.getJobId() == null || message.getStoredPath() == null) {
                throw new IllegalArgumentException("jobId and storedPath are required");
            }
            
            repository.publish(
                RabbitMqConfig.INGESTION_EXCHANGE,
                RabbitMqConfig.INGESTION_ROUTING_KEY,
                message
            );
            
            log.info("Published ingestion message: jobId={}, storedPath={}", 
                     message.getJobId(), message.getStoredPath());
            
        } catch (Exception e) {
            log.error("Failed to publish ingestion message: jobId={}", 
                      message.getJobId(), e);
            throw new RuntimeException("Failed to publish ingestion message", e);
        }
    }
    
    /**
     * Publishes a query message to the query workflow queue.
     * <p>
     * Automatically sets timestamp if not provided and validates required fields.
     * </p>
     * 
     * @param message query message containing job ID, data source, and query definition
     * @throws IllegalArgumentException if jobId, source, or queryJson are missing
     * @throws RuntimeException if publishing fails
     */
    public void publishQueryMessage(QueryMessage message) {
        try {
            // Enrich message with timestamp if not already set
            if (message.getTimestamp() == null) {
                message.setTimestamp(Instant.now().toString());
            }
            
            // Validate required fields
            if (message.getJobId() == null || message.getSource() == null || message.getQueryJson() == null) {
                throw new IllegalArgumentException("jobId, source, and queryJson are required");
            }
            
            // Priority 1 — data queries are heavier jobs; schema requests (priority 8)
            // will overtake any pending query messages in the broker queue.
            repository.publish(
                RabbitMqConfig.QUERY_EXCHANGE,
                RabbitMqConfig.QUERY_ROUTING_KEY,
                message,
                1
            );

            log.info("Published query message (priority=1): jobId={}, source={}",
                     message.getJobId(), message.getSource());
            
        } catch (Exception e) {
            log.error("Failed to publish query message: jobId={}", 
                      message.getJobId(), e);
            throw new RuntimeException("Failed to publish query message", e);
        }
    }
    
    /**
     * Publishes a schema message to the schema workflow queue.
     * <p>
     * Automatically sets timestamp if not provided and validates required fields.
     * </p>
     * 
     * @param message schema message containing job ID and data source
     * @throws IllegalArgumentException if jobId or source are missing
     * @throws RuntimeException if publishing fails
     */
    public void publishSchemaMessage(SchemaMessage message) {
        try {
            // Enrich message with timestamp if not already set
            if (message.getTimestamp() == null) {
                message.setTimestamp(Instant.now().toString());
            }
            
            // Validate required fields
            if (message.getJobId() == null || message.getSource() == null) {
                throw new IllegalArgumentException("jobId and source are required");
            }
            
            // Schema jobs are handled by the query-worker (SparkService dispatches
            // on the 'schema-' jobId prefix), so we route through the query queue.
            // Priority 8 — schema requests are fast (~1 s) and user-facing; they must
            // not be blocked behind a queue of slow data queries (5+ s each).
            repository.publish(
                RabbitMqConfig.QUERY_EXCHANGE,
                RabbitMqConfig.QUERY_ROUTING_KEY,
                message,
                8
            );

            log.info("Published schema message (priority=8) via query queue: jobId={}, source={}",
                     message.getJobId(), message.getSource());
            
        } catch (Exception e) {
            log.error("Failed to publish schema message: jobId={}", 
                      message.getJobId(), e);
            throw new RuntimeException("Failed to publish schema message", e);
        }
    }
    
    /**
     * Publishes a generic message to the specified exchange and routing key.
     * <p>
     * Use this method for custom workflows or when more control is needed over
     * message routing.
     * </p>
     * 
     * @param exchange   exchange or topic name
     * @param routingKey routing key or partition key
     * @param message    message payload (will be serialized to JSON)
     * @throws RuntimeException if publishing fails
     */
    public void publishMessage(String exchange, String routingKey, Object message) {
        try {
            repository.publish(exchange, routingKey, message);
            log.info("Published generic message: exchange={}, routingKey={}, messageType={}", 
                     exchange, routingKey, message.getClass().getSimpleName());
        } catch (Exception e) {
            log.error("Failed to publish generic message: exchange={}, routingKey={}", 
                      exchange, routingKey, e);
            throw new RuntimeException("Failed to publish message", e);
        }
    }
}
