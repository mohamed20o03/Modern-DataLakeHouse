package com.abdelwahab.query_worker.storage;

import com.abdelwahab.query_worker.storage.minio.MinioStorageConfig;
import com.abdelwahab.query_worker.storage.s3.S3StorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating StorageConfig instances.
 * Reads STORAGE_TYPE environment variable to decide which implementation to use.
 *
 * <p>Supported types:
 * <ul>
 *   <li>{@code minio} (default) - MinIO local/dev with explicit endpoint and static credentials</li>
 *   <li>{@code s3} - AWS S3 production with IAM role-based authentication</li>
 * </ul>
 *
 * <p>Usage: set env variable {@code STORAGE_TYPE=minio} or {@code STORAGE_TYPE=s3}
 */
public class StorageConfigFactory {

    private static final Logger log = LoggerFactory.getLogger(StorageConfigFactory.class);

    /** Utility class — no instances. */
    private StorageConfigFactory() {}

    /**
     * Creates a {@link StorageConfig} from the given parameters and the
     * {@code STORAGE_TYPE} environment variable.
     *
     * <p>Supported types:
     * <ul>
     *   <li>{@code minio} (default) — explicit endpoint + static credentials;
     *       all six parameters are used.</li>
     *   <li>{@code s3} — IAM role auth; {@code endpoint}, {@code accessKey}, and
     *       {@code secretKey} are ignored and may be passed as empty strings.</li>
     * </ul>
     *
     * @param endpoint         MinIO endpoint URL (ignored for S3)
     * @param accessKey        MinIO access key (ignored for S3)
     * @param secretKey        MinIO secret key (ignored for S3)
     * @param region           AWS / MinIO region
     * @param uploadsBucket    bucket for files uploaded by the API service
     * @param warehouseBucket  bucket used as the Iceberg warehouse root
     * @return an immutable {@link StorageConfig} ready to pass to an engine
     */
    public static StorageConfig create(String endpoint, String accessKey, String secretKey,
                                       String region, String uploadsBucket, String warehouseBucket) {

        // Read which storage backend to use from the environment
        String type = System.getenv().getOrDefault("STORAGE_TYPE", "minio").toLowerCase();

        log.info("Creating StorageConfig: type={}", type);

        return switch (type) {
            // MinIO: pass all credentials; endpoint required for path-style access
            case "minio" -> new MinioStorageConfig(endpoint, accessKey, secretKey,
                    region, uploadsBucket, warehouseBucket);

            // AWS S3: credentials come from the EC2/ECS IAM role — no keys in config
            case "s3" -> {
                if (endpoint != null && !endpoint.isEmpty()) {
                    // Warn so developers know these env vars have no effect in S3 mode
                    log.warn("STORAGE_TYPE=s3: MINIO_ENDPOINT, AWS_ACCESS_KEY_ID and "
                            + "AWS_SECRET_ACCESS_KEY are ignored — IAM role auth is used");
                }
                yield new S3StorageConfig(region, uploadsBucket, warehouseBucket);
            }

            default -> {
                // Unknown type — fall back to MinIO so the worker can still start
                log.warn("Unknown STORAGE_TYPE '{}', falling back to minio", type);
                yield new MinioStorageConfig(endpoint, accessKey, secretKey,
                        region, uploadsBucket, warehouseBucket);
            }
        };
    }
}
