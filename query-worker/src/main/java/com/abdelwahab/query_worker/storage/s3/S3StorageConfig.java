package com.abdelwahab.query_worker.storage.s3;

import com.abdelwahab.query_worker.storage.StorageConfig;

/**
 * {@link StorageConfig} implementation for AWS S3 with IAM role authentication.
 *
 * <p>Used in production deployments on EC2 or ECS where the instance / task is
 * assigned an IAM role that grants read access to the uploads bucket and
 * read/write access to the warehouse bucket. No explicit credentials are stored.
 *
 * <p>{@link #getEndpoint()}, {@link #getAccessKey()}, and {@link #getSecretKey()}
 * all return {@code null}; the Hadoop {@code S3AFileSystem} falls back to the
 * {@code DefaultAWSCredentialsProviderChain} which reads the IAM role
 * automatically from the EC2 instance metadata service.
 *
 * @see com.abdelwahab.query_worker.storage.minio.MinioStorageConfig — static-credential variant
 * @see com.abdelwahab.query_worker.storage.StorageConfigFactory — selects this class
 */
public class S3StorageConfig implements StorageConfig {

    private final String region;
    private final String uploadsBucket;
    private final String warehouseBucket;

    public S3StorageConfig(String region, String uploadsBucket, String warehouseBucket) {
        this.region = region;
        this.uploadsBucket = uploadsBucket;
        this.warehouseBucket = warehouseBucket;
    }

    @Override public String getType() { return "s3"; }
    @Override public String getEndpoint() { return null; }   // not needed for AWS S3
    @Override public String getAccessKey() { return null; }  // uses IAM roles
    @Override public String getSecretKey() { return null; }  // uses IAM roles
    @Override public String getRegion() { return region; }
    @Override public String getUploadsBucket() { return uploadsBucket; }
    @Override public String getWarehouseBucket() { return warehouseBucket; }
}
