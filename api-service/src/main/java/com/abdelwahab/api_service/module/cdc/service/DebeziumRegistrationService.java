package com.abdelwahab.api_service.module.cdc.service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import lombok.extern.slf4j.Slf4j;

/**
 * Manages Debezium PostgreSQL connectors via the Kafka Connect REST API.
 *
 * <p><b>Responsibilities:</b>
 * <ul>
 *   <li>Register new connectors ({@code POST /connectors})</li>
 *   <li>Check if a connector exists ({@code GET /connectors/{name}})</li>
 *   <li>Get connector status ({@code GET /connectors/{name}/status})</li>
 *   <li>Delete connectors ({@code DELETE /connectors/{name}})</li>
 * </ul>
 *
 * <p>Uses Spring WebClient (non-blocking HTTP client) as recommended by
 * Spring Boot 3 (RestTemplate is in maintenance mode).
 */
@Slf4j
@Service
public class DebeziumRegistrationService {

    private final WebClient webClient;

    public DebeziumRegistrationService(
            @Value("${cdc.debezium.connect.url:http://localhost:8083}") String connectUrl) {

        this.webClient = WebClient.builder()
                .baseUrl(connectUrl)
                .build();
        log.info("DebeziumRegistrationService initialised — connectUrl={}", connectUrl);
    }

    /**
     * Registers a new Debezium PostgreSQL connector.
     *
     * @param connectionId UUID of the CDC connection
     * @param host         source DB hostname
     * @param port         source DB port
     * @param database     source DB name
     * @param username     source DB username
     * @param password     source DB password (plaintext — only used in the connector config POST)
     * @param schema       source schema
     * @param table        source table name
     * @return the connector name that was registered
     * @throws RuntimeException if registration fails
     */
    public String registerConnector(String connectionId, String host, int port,
                                     String database, String username, String password,
                                     String schema, String table) {

        String connectorName = "cdc-conn-" + connectionId;
        String shortId = connectionId.substring(0, Math.min(8, connectionId.length()));
        String slotName = "slot_" + shortId;

        log.info("Registering Debezium connector: name={}, slot={}, table={}.{}",
                connectorName, slotName, schema, table);

        Map<String, Object> config = new HashMap<>();
        config.put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        config.put("database.hostname", host);
        config.put("database.port", String.valueOf(port));
        config.put("database.user", username);
        config.put("database.password", password);
        config.put("database.dbname", database);
        config.put("topic.prefix", "cdc");
        config.put("table.include.list", schema + "." + table);
        config.put("plugin.name", "pgoutput");
        config.put("slot.name", slotName);
        config.put("publication.autocreate.mode", "all_tables");
        config.put("snapshot.mode", "initial");
        config.put("tombstones.on.delete", "false");
        config.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("key.converter.schemas.enable", "false");
        config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("value.converter.schemas.enable", "false");

        Map<String, Object> payload = new HashMap<>();
        payload.put("name", connectorName);
        payload.put("config", config);

        webClient.post()
                .uri("/connectors")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(payload)
                .retrieve()
                .onStatus(HttpStatusCode::isError, response ->
                        response.bodyToMono(String.class)
                                .map(body -> new RuntimeException(
                                        "Debezium registration failed (HTTP " + response.statusCode() + "): " + body)))
                .bodyToMono(String.class)
                .block();

        log.info("Debezium connector registered successfully: {}", connectorName);
        return connectorName;
    }

    /**
     * Checks if a connector with the given name already exists.
     */
    public boolean connectorExists(String connectorName) {
        try {
            webClient.get()
                    .uri("/connectors/{name}", connectorName)
                    .retrieve()
                    .onStatus(HttpStatusCode::is4xxClientError, response ->
                            response.bodyToMono(String.class)
                                    .map(body -> new RuntimeException("NOT_FOUND")))
                    .bodyToMono(String.class)
                    .block();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Gets the live status of a Debezium connector.
     *
     * @return status string (e.g., "RUNNING", "FAILED", "UNASSIGNED") or "UNKNOWN"
     */
    public String getConnectorStatus(String connectorName) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> response = webClient.get()
                    .uri("/connectors/{name}/status", connectorName)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .block();

            if (response != null) {
                @SuppressWarnings("unchecked")
                Map<String, String> connector = (Map<String, String>) response.get("connector");
                if (connector != null) {
                    return connector.getOrDefault("state", "UNKNOWN");
                }
            }
            return "UNKNOWN";
        } catch (Exception e) {
            log.warn("Could not get status for connector {}: {}", connectorName, e.getMessage());
            return "UNKNOWN";
        }
    }

    /**
     * Deletes a Debezium connector.
     *
     * @param connectorName the connector name to delete
     * @throws RuntimeException if deletion fails (other than 404)
     */
    public void deleteConnector(String connectorName) {
        try {
            webClient.delete()
                    .uri("/connectors/{name}", connectorName)
                    .retrieve()
                    .onStatus(HttpStatusCode::isError, response ->
                            response.bodyToMono(String.class)
                                    .map(body -> new RuntimeException(
                                            "Debezium deletion failed (HTTP " + response.statusCode() + "): " + body)))
                    .bodyToMono(Void.class)
                    .block();

            log.info("Debezium connector deleted: {}", connectorName);
        } catch (Exception e) {
            // 404 is acceptable — connector may already be deleted
            if (e.getMessage() != null && e.getMessage().contains("404")) {
                log.warn("Connector {} was already deleted", connectorName);
            } else {
                throw e;
            }
        }
    }
}
