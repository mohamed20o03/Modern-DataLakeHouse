package com.abdelwahab.ingestion_worker.storage;

/**
 * Immutable data contract for object-storage configuration.
 *
 * <p>This interface is deliberately engine-agnostic — it carries only
 * credentials and bucket names. Each engine applies them in its own way:
 * <ul>
 *   <li>Spark → {@link com.abdelwahab.ingestion_worker.engine.spark.SparkStorageConfigurer}</li>
 *   <li>Flink → its own configurer (future)</li>
 * </ul>
 *
 * <p>Two concrete implementations exist:
 * <ul>
 *   <li>{@link com.abdelwahab.ingestion_worker.storage.minio.MinioStorageConfig} —
 *       local / dev with an explicit endpoint and static key-pair.</li>
 *   <li>{@link com.abdelwahab.ingestion_worker.storage.s3.S3StorageConfig} —
 *       production AWS S3 with IAM role auth (no credentials in config).</li>
 * </ul>
 *
 * @see StorageConfigFactory — selects the implementation via {@code STORAGE_TYPE}
 */
public interface StorageConfig {

    /**
     * Backend identifier used by factories and configurers to switch behaviour.
     * @return {@code "minio"} or {@code "s3"}
     */
    String getType();

    /**
     * HTTP(S) endpoint URL for the MinIO server
     * (e.g. {@code http://minio:9000}).
     * Returns {@code null} for AWS S3 where the endpoint is inferred from the region.
     */
    String getEndpoint();

    /**
     * Static access key for MinIO authentication.
     * Returns {@code null} when IAM role auth is used (AWS S3 production).
     */
    String getAccessKey();

    /**
     * Static secret key paired with {@link #getAccessKey()}.
     * Returns {@code null} when IAM role auth is used.
     */
    String getSecretKey();

    /**
     * AWS / MinIO region identifier (e.g. {@code us-east-1}).
     * Required for both MinIO and S3 to set the S3A endpoint region.
     */
    String getRegion();

    /**
     * Name of the bucket that holds files uploaded by the API service
     * (staging area before ingestion into Iceberg).
     * Mapped to {@code s3a://<uploadsBucket>/<storedPath>} when reading.
     */
    String getUploadsBucket();

    /**
     * Name of the bucket used as the Iceberg warehouse root.
     * Iceberg stores table data and metadata here under
     * {@code s3://<warehouseBucket>/<namespace>/<table>/}.
     */
    String getWarehouseBucket();
}
