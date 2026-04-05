package com.abdelwahab.api_service.module.cdc.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * RabbitMQ message published by api-service and consumed by cdc-worker.
 *
 * <p>Carries the necessary metadata for the cdc-worker to set up
 * streaming for a new CDC connection or tear down an existing one.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CdcConnectionMessage {

    /** UUID of the CDC connection. */
    private String connectionId;

    /** Action to perform: CREATE or DELETE. */
    private String action;

    /** Source table in {@code schema.table} format. */
    private String sourceTable;

    /** Kafka topic name (e.g., {@code cdc.public.orders}). */
    private String topicName;

    /** Target Iceberg table (e.g., {@code iceberg.cdc_namespace.orders}). */
    private String targetTable;

    /** Primary key column name for MERGE INTO. */
    private String primaryKeyColumn;
}
