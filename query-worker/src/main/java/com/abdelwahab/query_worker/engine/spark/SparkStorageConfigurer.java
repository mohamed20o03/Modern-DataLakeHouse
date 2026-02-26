package com.abdelwahab.query_worker.engine.spark;

import com.abdelwahab.query_worker.storage.StorageConfig;
import org.apache.spark.sql.SparkSession;

/**
 * Applies a {@link StorageConfig} to a {@link SparkSession.Builder}.
 *
 * <p>This is the <em>only</em> place in the codebase that knows about both Spark
 * configuration keys and the {@link StorageConfig} abstraction.  By isolating
 * this mapping here:
 * <ul>
 *   <li>{@link StorageConfig} stays pure — no Spark imports.</li>
 *   <li>{@link com.abdelwahab.query_worker.engine.spark.SparkService} stays
 *       clean — no low-level S3A key strings.</li>
 *   <li>Adding a new storage backend (GCS, ADLS) means adding one method here.</li>
 * </ul>
 *
 * <p>Two config layers must be set for MinIO:
 * <ol>
 *   <li><b>Iceberg S3FileIO</b> ({@code spark.sql.catalog.iceberg.s3.*}) —
 *       used when Iceberg reads/writes table metadata and data files.</li>
 *   <li><b>Hadoop S3AFileSystem</b> ({@code spark.hadoop.fs.s3a.*}) —
 *       used when Spark's DataFrame reader/writer streams raw file bytes.</li>
 * </ol>
 */
public class SparkStorageConfigurer {

    /** Utility class — no instances. */
    private SparkStorageConfigurer() {}

    /**
     * Dispatches to the correct configuration method based on
     * {@link StorageConfig#getType()} ({@code "minio"} or {@code "s3"}).
     *
     * @param builder an in-progress SparkSession builder
     * @param config  storage backend to configure
     * @return the same builder with storage credentials applied
     */
    public static SparkSession.Builder apply(SparkSession.Builder builder, StorageConfig config) {
        return switch (config.getType()) {
            case "minio" -> applyMinio(builder, config);
            case "s3"    -> applyS3(builder, config);
            default      -> applyMinio(builder, config); // safe fallback for unknown types
        };
    }

    /**
     * Configures the builder for a MinIO-compatible endpoint using static
     * access-key / secret-key credentials.
     *
     * <p>Path-style access ({@code /{bucket}/{key}}) is required for MinIO;
     * virtual-hosted style ({@code {bucket}.host/{key}}) is the AWS default
     * but MinIO does not support it by default.
     */
    private static SparkSession.Builder applyMinio(SparkSession.Builder builder, StorageConfig config) {
        return builder
                // ── Iceberg S3FileIO layer ──────────────────────────────────────────
                // Tells Iceberg's S3FileIO where MinIO lives and to use path-style URLs
                .config("spark.sql.catalog.iceberg.s3.endpoint", config.getEndpoint())
                .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")

                // ── Hadoop S3AFileSystem layer ──────────────────────────────────────
                // Tells Spark's DataFrame I/O (spark.read / df.write) how to reach MinIO
                .config("spark.hadoop.fs.s3a.endpoint", config.getEndpoint())
                .config("spark.hadoop.fs.s3a.access.key", config.getAccessKey())
                .config("spark.hadoop.fs.s3a.secret.key", config.getSecretKey())
                // path.style.access=true disables virtual-hosting (required for MinIO)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                // Use the S3AFileSystem implementation for all s3a:// URIs
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                // SimpleAWSCredentialsProvider reads the static accessKey/secretKey above
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .config("spark.hadoop.fs.s3a.endpoint.region", config.getRegion());
    }

    /**
     * Configures the builder for AWS S3 using IAM role-based authentication.
     *
     * <p>No explicit credentials are set — Spark delegates to the standard AWS
     * credentials provider chain (EC2 instance profile, ECS task role, etc.).
     * The {@code endpoint} and {@code accessKey}/{@code secretKey} fields of
     * the config are intentionally ignored here.
     */
    private static SparkSession.Builder applyS3(SparkSession.Builder builder, StorageConfig config) {
        return builder
                // Use the S3AFileSystem for all s3a:// URIs
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                // IAM role chain: tries EC2 instance profile first, then default chain
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "com.amazonaws.auth.InstanceProfileCredentialsProvider," +
                        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
                .config("spark.hadoop.fs.s3a.endpoint.region", config.getRegion());
    }
}
