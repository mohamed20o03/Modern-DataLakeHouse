package com.abdelwahab.api_service.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.minio.MinioClient;

/**
 * MinIO object storage configuration.
 * <p>
 * This configuration creates a MinioClient bean for interacting with MinIO object storage.
 * MinIO provides S3-compatible API and is used to store uploaded files before ingestion
 * processing.
 * </p>
 * <p>
 * Configuration properties are loaded from {@link MinioProperties} which uses
 * Spring Boot's {@code @ConfigurationProperties} for type-safe configuration binding.
 * </p>
 * <p>
 * Properties (from application.properties):
 * </p>
 * <ul>
 *   <li><b>minio.endpoint:</b> MinIO server URL (e.g., http://localhost:9000)</li>
 *   <li><b>minio.access-key:</b> Access key for authentication</li>
 *   <li><b>minio.secret-key:</b> Secret key for authentication</li>
 *   <li><b>minio.bucket:</b> Default bucket name for file storage</li>
 * </ul>
 *
 * @see MinioProperties
 * @see com.abdelwahab.api_service.common.storage.MinioStorageRepository
 */
@Configuration
@EnableConfigurationProperties(MinioProperties.class)
public class MinioConfig {

    /**
     * Creates MinIO client with configured credentials.
     * <p>
     * The client uses HTTP connection pooling managed by OkHttp internally.
     * All MinIO operations (putObject, getObject, etc.) use this shared client.
     * </p>
     *
     * @param properties MinIO connection and authentication properties
     * @return configured MinioClient for object storage operations
     */
    @Bean
    public MinioClient minioClient(MinioProperties properties) {
        // Build client with endpoint and credentials from properties
        return MinioClient.builder()
                .endpoint(properties.getEndpoint())
                .credentials(properties.getAccessKey(), properties.getSecretKey())
                .build();
    }
}
