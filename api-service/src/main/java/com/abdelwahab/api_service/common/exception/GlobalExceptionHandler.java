package com.abdelwahab.api_service.common.exception;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.multipart.MaxUploadSizeExceededException;

import lombok.extern.slf4j.Slf4j;

/**
 * Global exception handler for REST API endpoints.
 * <p>
 * This handler intercepts exceptions thrown from controllers and converts them
 * into consistent, well-structured API error responses. It handles:
 * </p>
 * <ul>
 *   <li><b>Validation Errors:</b> Bean validation failures from @Valid</li>
 *   <li><b>Storage Exceptions:</b> File storage and MinIO errors</li>
 *   <li><b>Upload Size Errors:</b> File size exceeds configured limit</li>
 *   <li><b>Generic Exceptions:</b> Unexpected runtime errors</li>
 * </ul>
 * <p>
 * All error responses follow a consistent format:
 * </p>
 * <pre>
 * {
 *   "timestamp": "2026-02-14T10:30:00Z",
 *   "status": 400,
 *   "error": "Bad Request",
 *   "message": "Validation failed",
 *   "path": "/api/v1/ingestion/upload",
 *   "details": { ... }  // Optional field-specific errors
 * }
 * </pre>
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * Handles validation errors from @Valid annotations.
     * <p>
     * Triggered when request body validation fails (e.g., @NotBlank, @NotEmpty).
     * Returns field-level error details for client-side form validation.
     * </p>
     *
     * @param ex      validation exception containing all field errors
     * @param request web request context
     * @return ResponseEntity with 400 status and field-level error details
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidationExceptions(
            MethodArgumentNotValidException ex,
            WebRequest request) {
        
        // Extract field-level validation errors
        Map<String, String> fieldErrors = ex.getBindingResult()
            .getFieldErrors()
            .stream()
            .collect(Collectors.toMap(
                FieldError::getField,
                error -> error.getDefaultMessage() != null ? error.getDefaultMessage() : "Invalid value",
                (existing, replacement) -> existing // Keep first error if multiple errors for same field
            ));
        
        log.warn("Validation failed: path={}, errors={}", request.getDescription(false), fieldErrors);
        
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("timestamp", Instant.now().toString());
        errorResponse.put("status", HttpStatus.BAD_REQUEST.value());
        errorResponse.put("error", HttpStatus.BAD_REQUEST.getReasonPhrase());
        errorResponse.put("message", "Validation failed");
        errorResponse.put("path", request.getDescription(false).replace("uri=", ""));
        errorResponse.put("details", fieldErrors);
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }

    /**
     * Handles storage-related exceptions.
     * <p>
     * Triggered when file storage operations fail (MinIO connection errors,
     * bucket creation failures, file upload errors, etc.).
     * </p>
     *
     * @param ex      storage exception with error details
     * @param request web request context
     * @return ResponseEntity with 500 status and error message
     */
    @ExceptionHandler(StorageException.class)
    public ResponseEntity<Map<String, Object>> handleStorageException(
            StorageException ex,
            WebRequest request) {
        
        log.error("Storage operation failed: path={}, message={}", 
                  request.getDescription(false), ex.getMessage(), ex);
        
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("timestamp", Instant.now().toString());
        errorResponse.put("status", HttpStatus.INTERNAL_SERVER_ERROR.value());
        errorResponse.put("error", HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase());
        errorResponse.put("message", "File storage operation failed");
        errorResponse.put("path", request.getDescription(false).replace("uri=", ""));
        errorResponse.put("details", ex.getMessage());
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }

    /**
     * Handles file upload size exceeded exceptions.
     * <p>
     * Triggered when uploaded file size exceeds the configured maximum
     * (spring.servlet.multipart.max-file-size).
     * </p>
     *
     * @param ex      max upload size exception
     * @param request web request context
     * @return ResponseEntity with 413 status and size limit details
     */
    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public ResponseEntity<Map<String, Object>> handleMaxUploadSizeExceededException(
            MaxUploadSizeExceededException ex,
            WebRequest request) {
        
        log.warn("File upload size exceeded: path={}, message={}", 
                 request.getDescription(false), ex.getMessage());
        
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("timestamp", Instant.now().toString());
        errorResponse.put("status", HttpStatus.PAYLOAD_TOO_LARGE.value());
        errorResponse.put("error", HttpStatus.PAYLOAD_TOO_LARGE.getReasonPhrase());
        errorResponse.put("message", "File size exceeds maximum allowed limit");
        errorResponse.put("path", request.getDescription(false).replace("uri=", ""));
        errorResponse.put("details", ex.getMessage());
        
        return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).body(errorResponse);
    }

    /**
     * Handles illegal argument exceptions.
     * <p>
     * Triggered when invalid arguments are passed to methods (e.g., null jobId,
     * missing required fields in message publishing).
     * </p>
     *
     * @param ex      illegal argument exception
     * @param request web request context
     * @return ResponseEntity with 400 status and error message
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgumentException(
            IllegalArgumentException ex,
            WebRequest request) {
        
        log.warn("Invalid argument: path={}, message={}", 
                 request.getDescription(false), ex.getMessage());
        
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("timestamp", Instant.now().toString());
        errorResponse.put("status", HttpStatus.BAD_REQUEST.value());
        errorResponse.put("error", HttpStatus.BAD_REQUEST.getReasonPhrase());
        errorResponse.put("message", ex.getMessage() != null ? ex.getMessage() : "Invalid argument");
        errorResponse.put("path", request.getDescription(false).replace("uri=", ""));
        
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }

    /**
     * Handles all other unhandled exceptions.
     * <p>
     * This is the catch-all handler for any exceptions not explicitly handled above.
     * Prevents exposing internal error details to clients while logging full stack traces.
     * </p>
     *
     * @param ex      any runtime exception
     * @param request web request context
     * @return ResponseEntity with 500 status and generic error message
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGlobalException(
            Exception ex,
            WebRequest request) {
        
        log.error("Unhandled exception: path={}, type={}, message={}", 
                  request.getDescription(false), ex.getClass().getSimpleName(), ex.getMessage(), ex);
        
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("timestamp", Instant.now().toString());
        errorResponse.put("status", HttpStatus.INTERNAL_SERVER_ERROR.value());
        errorResponse.put("error", HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase());
        errorResponse.put("message", "An unexpected error occurred");
        errorResponse.put("path", request.getDescription(false).replace("uri=", ""));
        // Don't expose internal error details in production
        // errorResponse.put("details", ex.getMessage());
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }
}
