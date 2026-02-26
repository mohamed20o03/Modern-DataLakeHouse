package com.abdelwahab.api_service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Getter;
import lombok.Setter;

/**
 * Type-safe configuration properties for MinIO object storage.
 * <p>
 * This class binds configuration from {@code application.properties} with prefix "minio"
 * to strongly-typed Java properties. Spring Boot automatically validates and injects
 * these values at startup.
 * </p>
 * <p>
 * Example configuration in application.properties:
 * </p>
 * <pre>
 * minio.endpoint=http://localhost:9000
 * minio.access-key=minioadmin
 * minio.secret-key=minioadmin
 * minio.bucket=data-ingestion
 * minio.region=us-east-1
 * </pre>
 * <p>
 * Used by {@link MinioConfig} to create {@link io.minio.MinioClient} bean.
 * </p>
 *
 * @see MinioConfig
 * @see com.abdelwahab.api_service.common.storage.MinioStorageRepository
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "minio")
public class MinioProperties {
    
    /** Base MinIO endpoint URL (e.g., http://localhost:9000) */
    private String endpoint;
    
    /** Access key for MinIO authentication */
    private String accessKey;
    
    /** Secret key for MinIO authentication */
    private String secretKey;
    
    /** Default bucket name for file storage operations */
    private String bucket;
    
    /** Optional region for S3-compatible deployments (not required for MinIO) */
    private String region;
}
