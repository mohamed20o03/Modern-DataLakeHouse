package com.abdelwahab.api_service.common.storage;

import java.io.InputStream;
import java.util.Objects;

import org.springframework.stereotype.Repository;
import org.springframework.web.multipart.MultipartFile;

import com.abdelwahab.api_service.common.exception.StorageException;
import com.abdelwahab.api_service.config.MinioProperties;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * MinIO-backed implementation of {@link StorageRepository}.
 * <p>
 * This implementation stores files in MinIO object storage using a hierarchical path
 * structure: {@code ingestion/{jobId}/{fileName}}. It automatically creates buckets
 * if they don't exist and handles connection pooling through the injected MinioClient.
 * </p>
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class MinioStorageRepository implements StorageRepository {
    private final MinioClient minioClient;
    private final MinioProperties properties;

    /**
     * Stores a file in MinIO object storage.
     * <p>
     * Object path format: {@code ingestion/{jobId}/{fileName}}
     * </p>
     *
     * @param file  multipart file upload payload
     * @param jobId job identifier used for organizing objects
     * @return the object path in MinIO
     * @throws StorageException if file upload fails
     */
    @Override
    public String storeFile(MultipartFile file, String jobId) {
        // Build a predictable object name using the job id and original file name.
        String fileName = Objects.requireNonNullElse(file.getOriginalFilename(), "upload");
        String objectName = "ingestion/" + jobId + "/" + fileName;
        String bucket = properties.getBucket();

        log.debug("Storing file in MinIO: bucket={}, object={}, size={}", bucket, objectName, file.getSize());

        try (InputStream inputStream = file.getInputStream()) {
            // Ensure the bucket exists before uploading.
            ensureBucket(bucket);

            PutObjectArgs args = PutObjectArgs.builder()
                    .bucket(bucket)
                    .object(objectName)
                    .stream(inputStream, file.getSize(), -1) // Use -1 for unknown part size
                    .contentType(file.getContentType())
                    .build();

            minioClient.putObject(args);
            log.info("Successfully stored file: bucket={}, object={}", bucket, objectName);
            return objectName;
        } catch (Exception ex) {
            log.error("Failed to store file in MinIO: bucket={}, object={}, jobId={}", bucket, objectName, jobId, ex);
            throw new StorageException("Failed to store file in MinIO", ex);
        }
    }

    /**
     * Ensures a bucket exists in MinIO, creating it if necessary.
     *
     * @param bucket the bucket name to check/create
     * @throws Exception if bucket operations fail
     */
    private void ensureBucket(String bucket) throws Exception {
        boolean exists = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucket).build());
        if (!exists) {
            log.info("Creating MinIO bucket: {}", bucket);
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
        }
    }
}
