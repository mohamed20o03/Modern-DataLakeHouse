package com.abdelwahab.api_service.jobstatus.repository;

import java.time.Duration;
import java.util.Map;

/**
 * Abstraction for job status storage backends.
 * <p>
 * This interface provides a contract for storing and retrieving job execution status
 * regardless of the underlying storage implementation (Redis, database, cache, etc.).
 * Job status is stored as a key-value map with automatic expiration support.
 * </p>
 * <p>
 * Standard fields:
 * </p>
 * <ul>
 *   <li><b>status:</b> Current job state (PENDING, PROCESSING, COMPLETED, FAILED)</li>
 *   <li><b>message:</b> Optional status description or error message</li>
 *   <li><b>createdAt:</b> Job creation timestamp (ISO-8601 format)</li>
 *   <li><b>updatedAt:</b> Last update timestamp (ISO-8601 format)</li>
 * </ul>
 */
public interface JobStatusRepository {
    
    /**
     * Writes or updates job status with message and automatic expiration.
     * <p>
     * If the job doesn't exist, creates it with createdAt timestamp.
     * If the job exists, updates status, message, and updatedAt timestamp.
     * </p>
     *
     * @param jobId   unique job identifier
     * @param status  job status (PENDING, PROCESSING, COMPLETED, FAILED, etc.)
     * @param message optional status message or error description
     * @param ttl     time-to-live duration for automatic expiration (null for no expiration)
     */
    void writeStatus(String jobId, String status, String message, Duration ttl);
    
    /**
     * Reads only the status field for quick status checks.
     * <p>
     * This is optimized for performance and should be used when only the status
     * is needed, avoiding the overhead of fetching all fields.
     * </p>
     *
     * @param jobId unique job identifier
     * @return current status or null if job not found
     */
    String readStatus(String jobId);
    
    /**
     * Reads all job fields as a map.
     * <p>
     * Returns all stored fields including status, message, createdAt, updatedAt,
     * and any custom fields that may have been added.
     * </p>
     *
     * @param jobId unique job identifier
     * @return map of all fields (empty map if job not found, null on error)
     */
    Map<Object, Object> readJobDetails(String jobId);
    
    /**
     * Updates a specific field atomically without affecting other fields.
     * <p>
     * Automatically updates the updatedAt timestamp. This is useful for
     * adding custom fields or updating specific values without loading
     * and storing the entire job object.
     * </p>
     *
     * @param jobId unique job identifier
     * @param field field name to update
     * @param value new field value
     */
    void updateField(String jobId, String field, String value);
    
    /**
     * Checks if a job exists in storage.
     *
     * @param jobId unique job identifier
     * @return true if job exists, false otherwise
     */
    boolean exists(String jobId);
    
    /**
     * Deletes a job and all its associated data from storage.
     *
     * @param jobId unique job identifier
     */
    void deleteJob(String jobId);
}
