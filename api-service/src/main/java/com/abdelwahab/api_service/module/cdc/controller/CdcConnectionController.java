package com.abdelwahab.api_service.module.cdc.controller;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import com.abdelwahab.api_service.module.cdc.dto.CdcConnectionRequest;
import com.abdelwahab.api_service.module.cdc.dto.CdcConnectionResponse;
import com.abdelwahab.api_service.module.cdc.dto.CdcConnectionStatus;
import com.abdelwahab.api_service.module.cdc.service.CdcConnectionService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * REST controller for CDC connection management.
 *
 * <p>Handles HTTP only — all workflow logic lives in {@link CdcConnectionService}.
 * Follows the same controller patterns as {@code IngestionController}.
 *
 * <p><b>Endpoints:</b>
 * <ul>
 *   <li>{@code POST   /api/v1/cdc/connections}               — create a connection</li>
 *   <li>{@code GET    /api/v1/cdc/connections}               — list all connections</li>
 *   <li>{@code GET    /api/v1/cdc/connections/{connectionId}} — get connection status</li>
 *   <li>{@code GET    /api/v1/cdc/connections/{connectionId}/wait} — long-poll for status</li>
 *   <li>{@code DELETE /api/v1/cdc/connections/{connectionId}} — delete a connection</li>
 * </ul>
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/cdc/connections")
@RequiredArgsConstructor
public class CdcConnectionController {

    private final CdcConnectionService cdcConnectionService;

    /**
     * Creates a new CDC connection.
     *
     * <p>Accepts source database credentials and table name, then orchestrates
     * validation, Debezium connector registration, and worker notification.
     *
     * @param request validated connection request DTO
     * @return 201 Created with connection ID and status URL
     */
    @PostMapping
    public ResponseEntity<CdcConnectionResponse> createConnection(
            @Valid @RequestBody CdcConnectionRequest request) {

        log.info("POST /api/v1/cdc/connections — table={}.{}", request.getSchema(), request.getTable());

        CdcConnectionResponse response = cdcConnectionService.createConnection(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * Lists all CDC connections with their current statuses.
     *
     * @return 200 OK with list of connection statuses
     */
    @GetMapping
    public ResponseEntity<List<CdcConnectionStatus>> listConnections() {
        log.debug("GET /api/v1/cdc/connections");

        List<CdcConnectionStatus> connections = cdcConnectionService.listConnections();
        return ResponseEntity.ok(connections);
    }

    /**
     * Returns the detailed status of a specific CDC connection.
     *
     * @param connectionId the connection UUID
     * @return 200 OK with status, or 404 if not found
     */
    @GetMapping("/{connectionId}")
    public ResponseEntity<CdcConnectionStatus> getConnectionStatus(
            @PathVariable String connectionId) {

        log.debug("GET /api/v1/cdc/connections/{}", connectionId);

        CdcConnectionStatus status = cdcConnectionService.getStatus(connectionId);
        if (status == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(status);
    }

    /**
     * Long-poll endpoint — blocks until the CDC connection reaches STREAMING
     * or a *_FAILED state, or the timeout expires.
     *
     * @param connectionId the connection UUID
     * @param timeoutSec   maximum wait in seconds (default: 60)
     * @return deferred result resolving to connection status
     */
    @GetMapping("/{connectionId}/wait")
    public DeferredResult<ResponseEntity<Map<Object, Object>>> waitForConnection(
            @PathVariable String connectionId,
            @RequestParam(defaultValue = "60") int timeoutSec) {

        log.debug("GET /api/v1/cdc/connections/{}/wait — timeout={}s", connectionId, timeoutSec);

        DeferredResult<ResponseEntity<Map<Object, Object>>> deferred =
                new DeferredResult<>((long) timeoutSec * 1000);

        deferred.onTimeout(() -> {
            CdcConnectionStatus current = cdcConnectionService.getStatus(connectionId);
            if (current != null) {
                deferred.setResult(ResponseEntity.ok(Map.of(
                        "connectionId", current.getConnectionId(),
                        "status", current.getStatus()
                )));
            } else {
                deferred.setResult(ResponseEntity.status(404)
                        .body(Map.of("error", "Connection not found", "connectionId", connectionId)));
            }
        });

        cdcConnectionService.waitForCompletion(connectionId, java.time.Duration.ofSeconds(timeoutSec))
                .thenAccept(details -> {
                    if (details != null && !details.isEmpty()) {
                        deferred.setResult(ResponseEntity.ok(details));
                    }
                });

        return deferred;
    }

    /**
     * Deletes a CDC connection — removes the Debezium connector and stops streaming.
     *
     * @param connectionId the connection UUID
     * @return 204 No Content on success
     */
    @DeleteMapping("/{connectionId}")
    public ResponseEntity<Void> deleteConnection(@PathVariable String connectionId) {
        log.info("DELETE /api/v1/cdc/connections/{}", connectionId);

        cdcConnectionService.deleteConnection(connectionId);
        return ResponseEntity.noContent().build();
    }
}
