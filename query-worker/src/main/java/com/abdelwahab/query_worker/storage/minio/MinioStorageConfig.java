package com.abdelwahab.query_worker.storage.minio;

import com.abdelwahab.query_worker.storage.StorageConfig;

/**
 * {@link StorageConfig} implementation for MinIO (and any S3-compatible store).
 *
 * <p>Used in local development and on-premise deployments where a MinIO server
 * is running with static access-key / secret-key credentials.
 * The {@code endpoint} field is required because MinIO does not use the AWS
 * regional endpoint discovery — you must point directly at the server
 * (e.g. {@code http://minio:9000}).
 *
 * <p>All six fields are passed through verbatim to
 * {@link com.abdelwahab.query_worker.engine.spark.SparkStorageConfigurer}
 * which writes them as {@code spark.hadoop.fs.s3a.*} Spark config keys.
 *
 * @see com.abdelwahab.query_worker.storage.s3.S3StorageConfig — IAM-based AWS S3 variant
 * @see com.abdelwahab.query_worker.storage.StorageConfigFactory — selects this class
 */
public class MinioStorageConfig implements StorageConfig {

    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String region;
    private final String uploadsBucket;
    private final String warehouseBucket;

    public MinioStorageConfig(String endpoint, String accessKey, String secretKey,
                               String region, String uploadsBucket, String warehouseBucket) {
        this.endpoint = endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;
        this.uploadsBucket = uploadsBucket;
        this.warehouseBucket = warehouseBucket;
    }

    @Override public String getType() { return "minio"; }
    @Override public String getEndpoint() { return endpoint; }
    @Override public String getAccessKey() { return accessKey; }
    @Override public String getSecretKey() { return secretKey; }
    @Override public String getRegion() { return region; }
    @Override public String getUploadsBucket() { return uploadsBucket; }
    @Override public String getWarehouseBucket() { return warehouseBucket; }
}
