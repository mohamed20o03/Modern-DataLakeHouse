package com.abdelwahab.api_service.common.exception;

/**
 * Signals storage failures in the persistence layer.
 */
public class StorageException extends RuntimeException {
    /**
     * Creates a storage exception with a message and root cause.
     *
     * @param message error message
     * @param cause   root cause
     */
    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
