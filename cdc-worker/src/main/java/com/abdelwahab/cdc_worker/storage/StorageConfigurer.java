package com.abdelwahab.cdc_worker.storage;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configures S3A (MinIO) file system properties on a {@link SparkSession}.
 *
 * <p>Reuses the same pattern as the existing ingestion-worker and query-worker
 * StorageConfigurer classes. In the CDC worker, the SparkSession is created
 * with all Iceberg + S3A settings via the entrypoint.sh spark-submit command,
 * so this class exists as a programmatic fallback or for any additional
 * runtime config that cannot be set at submit time.
 */
public final class StorageConfigurer {

    private static final Logger log = LoggerFactory.getLogger(StorageConfigurer.class);

    private StorageConfigurer() { /* utility class */ }

    /**
     * Applies S3A / MinIO configuration to a SparkSession's Hadoop configuration.
     *
     * @param spark    active SparkSession
     * @param endpoint MinIO endpoint URL (e.g. {@code http://minio:9000})
     * @param accessKey S3 access key
     * @param secretKey S3 secret key
     * @param region   AWS region (e.g. {@code us-east-1})
     */
    public static void configure(SparkSession spark,
                                  String endpoint,
                                  String accessKey,
                                  String secretKey,
                                  String region) {
        var hadoopConf = spark.sparkContext().hadoopConfiguration();
        hadoopConf.set("fs.s3a.endpoint", endpoint);
        hadoopConf.set("fs.s3a.access.key", accessKey);
        hadoopConf.set("fs.s3a.secret.key", secretKey);
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.connection.ssl.enabled", "false");
        hadoopConf.set("fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        hadoopConf.set("fs.s3a.region", region);

        log.info("S3A storage configured — endpoint={}", endpoint);
    }
}
