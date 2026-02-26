package com.abdelwahab.api_service.jobstatus.repository;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Redis-backed implementation of {@link JobStatusRepository}.
 * <p>
 * This implementation uses Redis Hash data structure for optimal performance:
 * </p>
 * <ul>
 *   <li>No JSON serialization/deserialization overhead</li>
 *   <li>Atomic field-level updates using HSET</li>
 *   <li>Selective field reads using HGET (fetch only what's needed)</li>
 *   <li>Better memory efficiency for sparse objects</li>
 *   <li>Native TTL support for automatic job expiration</li>
 * </ul>
 * <p>
 * Key format: {@code job:{jobId}}
 * </p>
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class RedisJobStatusRepository implements JobStatusRepository {
    
    private final StringRedisTemplate redis;
    
    private static final String JOB_PREFIX = "job:";
    
    /**
     * Writes job status to Redis Hash structure.
     * <p>
     * Creates new job if doesn't exist, updates existing job otherwise.
     * Automatically manages createdAt and updatedAt timestamps.
     * </p>
     *
     * @param jobId   unique job identifier
     * @param status  job status (PENDING, PROCESSING, COMPLETED, FAILED)
     * @param message optional status message
     * @param ttl     time-to-live for automatic expiration
     */
    @Override
    public void writeStatus(String jobId, String status, String message, Duration ttl) {
        try {
            String key = JOB_PREFIX + jobId;
            String timestamp = Instant.now().toString();
            
            // Update status and timestamp atomically
            redis.opsForHash().put(key, "status", status);
            redis.opsForHash().put(key, "updatedAt", timestamp);
            
            if (message != null) {
                redis.opsForHash().put(key, "message", message);
            }
            
            // Set creation time only for new jobs — putIfAbsent maps to HSETNX (atomic)
            redis.opsForHash().putIfAbsent(key, "createdAt", timestamp);
            
            // Apply TTL if specified
            if (ttl != null) {
                redis.expire(key, ttl.getSeconds(), TimeUnit.SECONDS);
            }
            
            log.debug("Job status written to Redis: jobId={}, status={}", jobId, status);
            
        } catch (Exception e) {
            log.error("Failed to write job status to Redis: jobId={}", jobId, e);
        }
    }
    
    /**
     * Reads only the status field from Redis.
     * <p>
     * Optimized for quick status checks without fetching entire job object.
     * </p>
     *
     * @param jobId unique job identifier
     * @return status string or null if not found
     */
    @Override
    public String readStatus(String jobId) {
        try {
            String key = JOB_PREFIX + jobId;
            Object status = redis.opsForHash().get(key, "status");
            return status != null ? status.toString() : null;
        } catch (Exception e) {
            log.error("Failed to read job status from Redis: jobId={}", jobId, e);
            return null;
        }
    }
    
    /**
     * Reads all job fields from Redis Hash.
     *
     * @param jobId unique job identifier
     * @return map of all fields or null if not found
     */
    @Override
    public Map<Object, Object> readJobDetails(String jobId) {
        try {
            String key = JOB_PREFIX + jobId;
            Map<Object, Object> job = redis.opsForHash().entries(key);
            
            if (job.isEmpty()) {
                log.warn("Job not found in Redis: jobId={}", jobId);
                return null;
            }
            
            return job;
            
        } catch (Exception e) {
            log.error("Failed to read job details from Redis: jobId={}", jobId, e);
            return null;
        }
    }
    
    /**
     * Updates a specific field atomically.
     * <p>
     * Automatically updates the updatedAt timestamp.
     * </p>
     *
     * @param jobId unique job identifier
     * @param field field name to update
     * @param value new field value
     */
    @Override
    public void updateField(String jobId, String field, String value) {
        try {
            String key = JOB_PREFIX + jobId;
            redis.opsForHash().put(key, field, value);
            redis.opsForHash().put(key, "updatedAt", Instant.now().toString());
            
            log.debug("Job field updated: jobId={}, field={}", jobId, field);
            
        } catch (Exception e) {
            log.error("Failed to update job field: jobId={}, field={}", jobId, field, e);
        }
    }
    
    /**
     * Checks if job exists in Redis.
     *
     * @param jobId unique job identifier
     * @return true if key exists
     */
    @Override
    public boolean exists(String jobId) {
        String key = JOB_PREFIX + jobId;
        return Boolean.TRUE.equals(redis.hasKey(key));
    }
    
    /**
     * Deletes job from Redis.
     *
     * @param jobId unique job identifier
     */
    @Override
    public void deleteJob(String jobId) {
        try {
            String key = JOB_PREFIX + jobId;
            redis.delete(key);
            log.debug("Job deleted from Redis: jobId={}", jobId);
        } catch (Exception e) {
            log.error("Failed to delete job from Redis: jobId={}", jobId, e);
        }
    }
}
