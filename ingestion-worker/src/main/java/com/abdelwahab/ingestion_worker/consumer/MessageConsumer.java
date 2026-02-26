package com.abdelwahab.ingestion_worker.consumer;

import java.io.IOException;

/**
 * Contract for all message-queue consumers in the ingestion worker.
 *
 * <p>Implementations hide every broker-specific detail (RabbitMQ channels,
 * Kafka partitions, SQS long-polling, etc.) behind two lifecycle methods so
 * that {@link com.abdelwahab.ingestion_worker.IngestionWorkerMain} stays
 * completely broker-agnostic.
 *
 * <p>The expected lifecycle is:
 * <pre>
 *   consumer = MessageConsumerFactory.create(...)   // wires connections
 *   consumer.start()                                 // begins delivery loop
 *   // ... application runs ...
 *   consumer.close()                                 // graceful shutdown
 * </pre>
 *
 * @see com.abdelwahab.ingestion_worker.consumer.MessageConsumerFactory
 * @see com.abdelwahab.ingestion_worker.consumer.rabbitmq.RabbitMQConsumer
 */
public interface MessageConsumer extends AutoCloseable {

    /**
     * Begin receiving messages from the broker.
     *
     * <p>Implementations may either:
     * <ul>
     *   <li><b>block</b> the calling thread (poll loop), or</li>
     *   <li><b>register async callbacks</b> and return immediately.</li>
     * </ul>
     * Each received message is deserialised into an
     * {@link com.abdelwahab.ingestion_worker.dto.IngestionMessage} and forwarded
     * to {@link com.abdelwahab.ingestion_worker.engine.IngestionService#ingest}.
     *
     * @throws IOException if the broker connection or channel setup fails
     */
    void start() throws IOException;

    /**
     * Stop consuming and release all broker resources.
     *
     * <p>Implementations must:
     * <ul>
     *   <li>stop accepting new deliveries,</li>
     *   <li>wait for in-flight messages to finish (with a reasonable timeout),</li>
     *   <li>close channels / partitions / connections.</li>
     * </ul>
     * Called automatically by the JVM shutdown hook registered in
     * {@link com.abdelwahab.ingestion_worker.IngestionWorkerMain}.
     */
    @Override
    void close();
}
