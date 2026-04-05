package com.abdelwahab.api_service.module.cdc.service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import com.abdelwahab.api_service.common.messaging.MessagePublisherService;
import com.abdelwahab.api_service.config.RabbitMqConfig;
import com.abdelwahab.api_service.jobstatus.service.JobStatusService;
import com.abdelwahab.api_service.module.cdc.dto.CdcConnectionMessage;
import com.abdelwahab.api_service.module.cdc.dto.CdcConnectionRequest;
import com.abdelwahab.api_service.module.cdc.dto.CdcConnectionResponse;
import com.abdelwahab.api_service.module.cdc.dto.CdcConnectionStatus;
import com.abdelwahab.api_service.module.cdc.security.CredentialEncryptor;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Orchestrates the CDC connection lifecycle.
 *
 * <p><b>Sequence for {@link #createConnection}:</b>
 * <ol>
 *   <li>Generate connectionId (UUID)</li>
 *   <li>Write PENDING status to Redis</li>
 *   <li>Validate source DB connectivity (JDBC)</li>
 *   <li>Encrypt password and store connection metadata in Redis</li>
 *   <li>Register Debezium connector</li>
 *   <li>Publish connection message to RabbitMQ</li>
 *   <li>Return connectionId and status URL</li>
 * </ol>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CdcConnectionService {

    private static final String CDC_CONN_PREFIX = "cdc-conn:";
    private static final String CDC_TARGET_NAMESPACE = "cdc_namespace";

    private final CredentialEncryptor credentialEncryptor;
    private final SourceDatabaseValidator sourceValidator;
    private final DebeziumRegistrationService debeziumService;
    private final MessagePublisherService messagePublisher;
    private final JobStatusService jobStatusService;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Creates a new CDC connection — validates, registers Debezium, publishes to RabbitMQ.
     *
     * @param request source DB credentials and table info
     * @return response with connectionId and status URL
     */
    public CdcConnectionResponse createConnection(CdcConnectionRequest request) {
        String connectionId = UUID.randomUUID().toString();
        String now = Instant.now().toString();
        String sourceTable = request.getSchema() + "." + request.getTable();
        String targetTable = "iceberg." + CDC_TARGET_NAMESPACE + "." + request.getTable();
        String topicName = "cdc." + sourceTable;
        String connectorName = "cdc-conn-" + connectionId;
        String redisKey = CDC_CONN_PREFIX + connectionId;

        log.info("Creating CDC connection: connectionId={}, sourceTable={}", connectionId, sourceTable);

        // --- Step 1: Check for duplicate (idempotency) ---
        String existingId = findExistingConnection(request);
        if (existingId != null) {
            log.info("Duplicate connection detected — returning existing connectionId={}", existingId);
            Map<Object, Object> existing = redisTemplate.opsForHash().entries(CDC_CONN_PREFIX + existingId);
            return CdcConnectionResponse.builder()
                    .connectionId(existingId)
                    .status(String.valueOf(existing.getOrDefault("status", "UNKNOWN")))
                    .sourceTable(sourceTable)
                    .targetTable(String.valueOf(existing.getOrDefault("targetTable", targetTable)))
                    .checkStatusAt("/api/v1/cdc/connections/" + existingId)
                    .createdAt(String.valueOf(existing.getOrDefault("createdAt", now)))
                    .build();
        }

        // --- Step 2: Set PENDING status ---
        updateRedisStatus(redisKey, "PENDING", null, now, now);

        // --- Step 3: Validate source DB ---
        updateRedisStatus(redisKey, "VALIDATING", null, null, null);
        SourceDatabaseValidator.ValidationResult validation = sourceValidator.validate(
                request.getHost(), request.getPort(), request.getDatabase(),
                request.getSchema(), request.getTable(),
                request.getUsername(), request.getPassword()
        );

        if (!validation.isValid()) {
            log.warn("Source DB validation failed: connectionId={}, error={}",
                    connectionId, validation.getErrorMessage());
            updateRedisStatus(redisKey, "VALIDATION_FAILED", validation.getErrorMessage(), null, null);
            return CdcConnectionResponse.builder()
                    .connectionId(connectionId)
                    .status("VALIDATION_FAILED")
                    .sourceTable(sourceTable)
                    .targetTable(targetTable)
                    .checkStatusAt("/api/v1/cdc/connections/" + connectionId)
                    .createdAt(now)
                    .build();
        }

        // --- Step 4: Encrypt password and store full metadata in Redis ---
        String encryptedPassword = credentialEncryptor.encrypt(request.getPassword());
        storeConnectionMetadata(redisKey, connectionId, request, encryptedPassword,
                validation, connectorName, targetTable, topicName, now);

        // --- Step 5: Register Debezium connector ---
        updateRedisStatus(redisKey, "REGISTERING", null, null, null);
        try {
            debeziumService.registerConnector(
                    connectionId,
                    request.getHost(), request.getPort(), request.getDatabase(),
                    request.getUsername(), request.getPassword(),
                    request.getSchema(), request.getTable()
            );
        } catch (Exception e) {
            log.error("Debezium registration failed: connectionId={}", connectionId, e);
            updateRedisStatus(redisKey, "REGISTRATION_FAILED", e.getMessage(), null, null);
            return CdcConnectionResponse.builder()
                    .connectionId(connectionId)
                    .status("REGISTRATION_FAILED")
                    .sourceTable(sourceTable)
                    .targetTable(targetTable)
                    .checkStatusAt("/api/v1/cdc/connections/" + connectionId)
                    .createdAt(now)
                    .build();
        }

        // --- Step 6: Publish to RabbitMQ ---
        updateRedisStatus(redisKey, "SNAPSHOTTING", null, null, null);
        CdcConnectionMessage message = CdcConnectionMessage.builder()
                .connectionId(connectionId)
                .action("CREATE")
                .sourceTable(sourceTable)
                .topicName(topicName)
                .targetTable(targetTable)
                .primaryKeyColumn(validation.getPrimaryKeyColumn())
                .build();

        messagePublisher.publishMessage(
                RabbitMqConfig.CDC_EXCHANGE,
                RabbitMqConfig.CDC_ROUTING_KEY,
                message
        );

        log.info("CDC connection created: connectionId={}, connector={}", connectionId, connectorName);

        // --- Step 7: Return response ---
        return CdcConnectionResponse.builder()
                .connectionId(connectionId)
                .status("SNAPSHOTTING")
                .sourceTable(sourceTable)
                .targetTable(targetTable)
                .checkStatusAt("/api/v1/cdc/connections/" + connectionId)
                .createdAt(now)
                .build();
    }

    /**
     * Returns the detailed status of a CDC connection.
     */
    public CdcConnectionStatus getStatus(String connectionId) {
        String redisKey = CDC_CONN_PREFIX + connectionId;
        Map<Object, Object> fields = redisTemplate.opsForHash().entries(redisKey);

        if (fields.isEmpty()) {
            return null;
        }

        // Merge with live Debezium connector status
        String connectorName = (String) fields.get("connectorName");
        String connectorStatus = null;
        if (connectorName != null) {
            try {
                connectorStatus = debeziumService.getConnectorStatus(connectorName);
            } catch (Exception e) {
                log.debug("Could not fetch Debezium status for {}: {}", connectorName, e.getMessage());
            }
        }

        return CdcConnectionStatus.builder()
                .connectionId(connectionId)
                .status((String) fields.get("status"))
                .sourceHost((String) fields.get("sourceHost"))
                .sourceDatabase((String) fields.get("sourceDatabase"))
                .sourceTable((String) fields.get("sourceTable"))
                .targetTable((String) fields.get("targetTable"))
                .connectorStatus(connectorStatus)
                .errorMessage((String) fields.get("errorMessage"))
                .createdAt((String) fields.get("createdAt"))
                .updatedAt((String) fields.get("updatedAt"))
                .build();
    }

    /**
     * Lists all CDC connections with their current statuses.
     */
    public List<CdcConnectionStatus> listConnections() {
        Set<String> keys = redisTemplate.keys(CDC_CONN_PREFIX + "*");
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyList();
        }

        List<CdcConnectionStatus> result = new ArrayList<>();
        for (String key : keys) {
            String connId = key.substring(CDC_CONN_PREFIX.length());
            CdcConnectionStatus status = getStatus(connId);
            if (status != null) {
                result.add(status);
            }
        }
        return result;
    }

    /**
     * Deletes a CDC connection — removes Debezium connector, updates status.
     */
    public void deleteConnection(String connectionId) {
        String redisKey = CDC_CONN_PREFIX + connectionId;
        Map<Object, Object> fields = redisTemplate.opsForHash().entries(redisKey);

        if (fields.isEmpty()) {
            log.warn("Connection not found for deletion: connectionId={}", connectionId);
            return;
        }

        // Update status to DELETING
        updateRedisStatus(redisKey, "DELETING", null, null, null);

        // Delete Debezium connector
        String connectorName = (String) fields.get("connectorName");
        if (connectorName != null) {
            try {
                debeziumService.deleteConnector(connectorName);
            } catch (Exception e) {
                log.error("Failed to delete Debezium connector {}: {}", connectorName, e.getMessage());
            }
        }

        // Publish DELETE message to RabbitMQ
        CdcConnectionMessage message = CdcConnectionMessage.builder()
                .connectionId(connectionId)
                .action("DELETE")
                .sourceTable((String) fields.get("sourceTable"))
                .topicName((String) fields.get("topicName"))
                .targetTable((String) fields.get("targetTable"))
                .primaryKeyColumn((String) fields.get("primaryKeyColumn"))
                .build();

        messagePublisher.publishMessage(
                RabbitMqConfig.CDC_EXCHANGE,
                RabbitMqConfig.CDC_ROUTING_KEY,
                message
        );

        // Update status to DELETED
        updateRedisStatus(redisKey, "DELETED", null, null, null);

        log.info("CDC connection deleted: connectionId={}", connectionId);
    }

    /**
     * Long-polls until the connection reaches STREAMING or a *_FAILED state.
     */
    public CompletableFuture<Map<Object, Object>> waitForCompletion(String connectionId, Duration timeout) {
        // Use the existing JobStatusService pattern — the CDC connection key follows
        // the same Redis hash pattern as job status
        return jobStatusService.waitForCompletion(CDC_CONN_PREFIX + connectionId, timeout);
    }

    // ── Private helpers ─────────────────────────────────────────────────────

    /**
     * Checks for an existing connection with the same source DB + table (idempotency).
     * Returns the existing connectionId or null.
     */
    private String findExistingConnection(CdcConnectionRequest request) {
        Set<String> keys = redisTemplate.keys(CDC_CONN_PREFIX + "*");
        if (keys == null) return null;

        String lookupTable = request.getSchema() + "." + request.getTable();

        for (String key : keys) {
            Map<Object, Object> fields = redisTemplate.opsForHash().entries(key);
            String existingHost = (String) fields.get("sourceHost");
            String existingPort = (String) fields.get("sourcePort");
            String existingDb = (String) fields.get("sourceDatabase");
            String existingTable = (String) fields.get("sourceTable");
            String existingStatus = (String) fields.get("status");

            // Skip deleted connections
            if ("DELETED".equals(existingStatus)) continue;

            if (request.getHost().equals(existingHost)
                    && String.valueOf(request.getPort()).equals(existingPort)
                    && request.getDatabase().equals(existingDb)
                    && lookupTable.equals(existingTable)) {
                return (String) fields.get("connectionId");
            }
        }
        return null;
    }

    /**
     * Stores full connection metadata in Redis hash.
     */
    private void storeConnectionMetadata(String redisKey, String connectionId,
                                          CdcConnectionRequest request, String encryptedPassword,
                                          SourceDatabaseValidator.ValidationResult validation,
                                          String connectorName, String targetTable,
                                          String topicName, String createdAt) {

        Map<String, String> fields = Map.ofEntries(
                Map.entry("connectionId", connectionId),
                Map.entry("sourceHost", request.getHost()),
                Map.entry("sourcePort", String.valueOf(request.getPort())),
                Map.entry("sourceDatabase", request.getDatabase()),
                Map.entry("sourceSchema", request.getSchema()),
                Map.entry("sourceTable", request.getSchema() + "." + request.getTable()),
                Map.entry("sourceUsername", request.getUsername()),
                Map.entry("sourcePassword", encryptedPassword),
                Map.entry("primaryKeyColumn", validation.getPrimaryKeyColumn()),
                Map.entry("columns", serializeColumns(validation.getColumns())),
                Map.entry("connectorName", connectorName),
                Map.entry("slotName", "slot_" + connectionId.substring(0, Math.min(8, connectionId.length()))),
                Map.entry("topicPrefix", "cdc"),
                Map.entry("topicName", topicName),
                Map.entry("targetTable", targetTable),
                Map.entry("createdAt", createdAt),
                Map.entry("updatedAt", createdAt)
        );

        redisTemplate.opsForHash().putAll(redisKey, fields);
    }

    /**
     * Updates status (and optionally error message, timestamps) in Redis.
     */
    private void updateRedisStatus(String redisKey, String status, String errorMessage,
                                    String createdAt, String updatedAt) {
        redisTemplate.opsForHash().put(redisKey, "status", status);
        String now = Instant.now().toString();
        redisTemplate.opsForHash().put(redisKey, "updatedAt", updatedAt != null ? updatedAt : now);

        if (createdAt != null) {
            redisTemplate.opsForHash().put(redisKey, "createdAt", createdAt);
        }
        if (errorMessage != null) {
            redisTemplate.opsForHash().put(redisKey, "errorMessage", errorMessage);
        }
    }

    /**
     * Serializes column info to JSON string for Redis storage.
     */
    private String serializeColumns(List<SourceDatabaseValidator.ColumnInfo> columns) {
        try {
            return objectMapper.writeValueAsString(
                    columns.stream()
                            .map(c -> Map.of("name", c.getName(), "type", c.getType()))
                            .collect(Collectors.toList())
            );
        } catch (Exception e) {
            log.warn("Could not serialize columns: {}", e.getMessage());
            return "[]";
        }
    }
}
