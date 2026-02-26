package com.abdelwahab.api_service.common.storage;

import org.springframework.web.multipart.MultipartFile;

/**
 * Abstraction for file storage backends.
 * <p>
 * This interface provides a contract for storing files regardless of the underlying
 * storage implementation (MinIO, S3, filesystem, etc.). Implementations should handle
 * connection management, error handling, and storage-specific details.
 * </p>
 */
public interface StorageRepository {
    /**
     * Stores a file for a specific job and returns the object path.
     *
     * @param file  multipart file upload payload
     * @param jobId job identifier used for object organization
     * @return object path in the backing store
     * @throws com.abdelwahab.api_service.common.exception.StorageException if storage operation fails
     */
    String storeFile(MultipartFile file, String jobId);
}
