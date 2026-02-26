package com.abdelwahab.query_worker.engine.spark;

import com.abdelwahab.query_worker.cache.SchemaCacheService;
import com.abdelwahab.query_worker.dto.QueryMessage;
import com.abdelwahab.query_worker.dto.query.QueryRequest;
import com.abdelwahab.query_worker.dto.query.QueryResult;
import com.abdelwahab.query_worker.engine.QueryService;
import com.abdelwahab.query_worker.status.JobStatusService;
import com.abdelwahab.query_worker.storage.StorageConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Apache Spark implementation of {@link QueryService}.
 *
 * <p><b>Responsibility (SRP):</b> orchestrate one job — parse the message,
 * delegate to specialised collaborators, and report status to Redis.
 * It does NOT build DataFrames (→ {@link SparkQueryBuilder}) or write results
 * (→ {@link QueryResultWriter}).
 *
 * <p><b>Supported job types</b> (detected from {@code jobId} prefix):
 * <ul>
 *   <li>{@code query-*} — executes a structured query against an Iceberg table.</li>
 *   <li>{@code schema-*} — reads the table schema and returns it as result rows.</li>
 * </ul>
 *
 * <p><b>Data flow:</b>
 * <pre>
 *   RabbitMQ message
 *        │
 *        ▼
 *   SparkService.query(message)
 *        │
 *        ├─ 1. Redis  ← PROCESSING
 *        ├─ 2. Resolve source → iceberg.{projectId}.{tableName}
 *        ├─ 3a. [query-*]  SparkQueryBuilder.build()  ──► Dataset
 *        │      QueryResultWriter.write() ──► Parquet on MinIO
 *        ├─ 3b. [schema-*] Read StructType ──► schema rows (no data scan)
 *        └─ 4. Redis  ← COMPLETED (with result metadata) | FAILED
 * </pre>
 *
 * @see SparkEngine           — creates the SparkSession and instantiates this class
 * @see SparkQueryBuilder     — translates {@link QueryRequest} into a Spark Dataset
 * @see QueryResultWriter     — persists the result Dataset to MinIO as Parquet
 * @see SparkStorageConfigurer — injects S3/MinIO credentials into the session
 */
public class SparkService implements QueryService {

    private static final Logger log = LoggerFactory.getLogger(SparkService.class);

    /** Iceberg catalog name as registered in {@code spark.sql.catalog.<name>} */
    private static final String ICEBERG_CATALOG   = "iceberg";
    private static final String JOB_PREFIX_SCHEMA = "schema-";

    private final SparkSession     spark;
    private final JobStatusService jobStatusService;
    private final SchemaCacheService schemaCache;
    private final SparkQueryBuilder queryBuilder;
    private final QueryResultWriter resultWriter;
    private final ObjectMapper      objectMapper;

    /**
     * Constructs a ready-to-use SparkService with all collaborators wired.
     *
     * <p>Called by {@link SparkEngine#initialize} once the SparkSession is fully
     * configured (Iceberg extensions + S3A credentials already applied).
     *
     * @param spark            active SparkSession with Iceberg catalog and S3A configured
     * @param jobStatusService async Redis client for reporting job progress
     * @param storageConfig    storage credentials and bucket names
     */
    public SparkService(SparkSession spark,
                        JobStatusService jobStatusService,
                        StorageConfig storageConfig,
                        SchemaCacheService schemaCache) {
        this.spark            = spark;
        this.jobStatusService = jobStatusService;
        this.schemaCache      = schemaCache;
        this.queryBuilder     = new SparkQueryBuilder(spark);
        this.resultWriter     = new QueryResultWriter(spark, storageConfig);
        this.objectMapper     = new ObjectMapper().registerModule(new JavaTimeModule());

        log.info("SparkService initialized — warehouse: {}", storageConfig.getWarehouseBucket());
    }

    /**
     * Executes one query or schema-retrieval job end-to-end.
     *
     * @param message job descriptor from RabbitMQ; must have {@code jobId} and {@code source}
     * @throws RuntimeException wrapping any Spark / S3 / Iceberg error
     */
    @Override
    public void query(QueryMessage message) {
        String jobId = message.getJobId();
        log.info("Starting job: jobId={}, source={}", jobId, message.getSource());

        try {
            // 1. Mark PROCESSING — fire-and-forget (does not block Spark thread)
            jobStatusService.writeStatus(jobId, "PROCESSING", null);

            // 2. Resolve fully-qualified Iceberg table name and project identifier
            String icebergTable = resolveIcebergTable(message.getSource());
            String projectId    = extractProjectId(message.getSource());
            log.info("Resolved table: {}", icebergTable);

            // 3a. Schema job — check Redis cache before touching Spark
            if (isSchemaJob(jobId)) {
                String cached = schemaCache.get(icebergTable);
                if (cached != null) {
                    int fields = countJsonArraySize(cached);
                    jobStatusService.writeResult(
                            jobId, "COMPLETED",
                            "Schema retrieved from cache: " + fields + " columns from table " + icebergTable,
                            null, fields, 0L, cached
                    ).join();
                    log.info("Job completed (schema cache): jobId={}, fields={}", jobId, fields);
                    return;
                }
            }

            // 3b. Execute via Spark — cache miss or regular query
            QueryResult result = isSchemaJob(jobId)
                    ? executeSchemaJob(icebergTable)
                    : executeQueryJob(message, icebergTable, projectId);

            // 3c. Populate cache after first Spark schema fetch
            if (isSchemaJob(jobId)) {
                schemaCache.put(icebergTable, serializeResultData(result.getResultData()));
            }

            // 4. Mark COMPLETED — block (.join()) to guarantee Redis persistence
            jobStatusService.writeResult(
                    jobId,
                    "COMPLETED",
                    buildCompletionMessage(result, icebergTable),
                    result.getResultPath(),
                    result.getRowCount(),
                    result.getFileSizeBytes(),
                    serializeResultData(result.getResultData())
            ).join();

            log.info("Job completed: jobId={}, rows={}", jobId, result.getRowCount());

        } catch (Exception e) {
            log.error("Job failed: jobId={}", jobId, e);
            jobStatusService.writeStatus(jobId, "FAILED", e.getMessage()).join();
            throw new RuntimeException("Query job failed: " + jobId, e);
        }
    }

    // ── Job type handlers ──────────────────────────────────────────────────────

    /**
     * Parses {@code queryJson} → {@link QueryRequest}, then delegates to
     * {@link SparkQueryBuilder} and {@link QueryResultWriter}.
     */
    private QueryResult executeQueryJob(QueryMessage message,
                                        String icebergTable,
                                        String projectId) throws Exception {
        QueryRequest request = parseQueryRequest(message.getQueryJson());
        log.info("Executing query — table: {}, selectCols: {}, filters: {}",
                icebergTable,
                request.getSelect() != null ? request.getSelect().size() : 0,
                request.getFilters() != null ? request.getFilters().size() : 0);

        Dataset<Row> result = queryBuilder.build(icebergTable, request);
        return resultWriter.write(result, projectId);
    }

    /**
     * Reads the Iceberg table schema without loading any data rows, then
     * packages it as a {@link QueryResult} whose {@code resultData} matches
     * the API spec shape: {@code [{"name": "id", "type": "int", "nullable": false}, ...]}
     */
    private QueryResult executeSchemaJob(String icebergTable) {
        log.info("Retrieving schema — table: {}", icebergTable);

        // limit(0) reads only Iceberg metadata — no data scan is performed.
        // CachingCatalog is disabled (cache-enabled=false in SparkEngine), so
        // this call always fetches the current snapshot from the REST catalog,
        // guaranteeing that columns added by the ingestion-worker are visible
        // immediately without an explicit REFRESH TABLE.
        StructField[] fields = spark.read()
                .format("iceberg")
                .load(icebergTable)
                .limit(0)
                .schema()
                .fields();

        List<Map<String, Object>> schemaRows = Arrays.stream(fields)
                .map(f -> {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("name",     f.name());
                    row.put("type",     f.dataType().simpleString());
                    row.put("nullable", f.nullable());
                    return row;
                })
                .collect(Collectors.toList());

        return QueryResult.builder()
                .resultPath(null)
                .rowCount(schemaRows.size())
                .fileSizeBytes(0L)
                .resultData(schemaRows)
                .build();
    }

    // ── Private helpers ────────────────────────────────────────────────────────

    /**
     * Resolves the source string to a fully-qualified Iceberg table identifier.
     * <ul>
     *   <li>{@code "myproject.sales"} → {@code "iceberg.myproject.sales"}</li>
     *   <li>{@code "iceberg.myproject.sales"} → unchanged</li>
     * </ul>
     */
    private String resolveIcebergTable(String source) {
        return source.startsWith(ICEBERG_CATALOG + ".")
                ? source
                : ICEBERG_CATALOG + "." + source;
    }

    /** Extracts the {@code projectId} from {@code "{projectId}.{tableName}"}. */
    private String extractProjectId(String source) {
        int dot = source.indexOf('.');
        return dot > 0 ? source.substring(0, dot) : source;
    }

    /** Returns {@code true} when the jobId belongs to a schema-retrieval job. */
    private boolean isSchemaJob(String jobId) {
        return jobId != null && jobId.startsWith(JOB_PREFIX_SCHEMA);
    }

    /**
     * Deserialises the {@code queryJson} string into a typed {@link QueryRequest}.
     *
     * @throws IllegalArgumentException if {@code queryJson} is null/blank or malformed
     */
    private QueryRequest parseQueryRequest(String queryJson) throws Exception {
        if (queryJson == null || queryJson.isBlank()) {
            throw new IllegalArgumentException("queryJson is required for query jobs");
        }
        return objectMapper.readValue(queryJson, QueryRequest.class);
    }

    /** Builds the human-readable status message stored in Redis. */
    private String buildCompletionMessage(QueryResult result, String icebergTable) {
        if (result.getResultPath() == null) {
            // Schema job — no Parquet file written
            return String.format("Schema retrieved: %d columns from table %s",
                    result.getRowCount(), icebergTable);
        }
        return String.format("Query completed: %d rows, result stored at %s",
                result.getRowCount(), result.getResultPath());
    }

    /** Serialises the inline result data to a JSON string for Redis storage. */
    private String serializeResultData(List<Map<String, Object>> resultData) {
        try {
            return objectMapper.writeValueAsString(resultData);
        } catch (Exception e) {
            log.warn("Failed to serialize resultData — storing empty array", e);
            return "[]";
        }
    }

    /**
     * Counts the elements in a JSON array string without deserialising the full structure.
     * Used to determine the column count from a cached schema string.
     *
     * @param json a JSON array, e.g. {@code [{...}, {...}]}
     * @return number of elements, or {@code 0} if parsing fails
     */
    private int countJsonArraySize(String json) {
        try {
            return objectMapper.readTree(json).size();
        } catch (Exception e) {
            log.warn("Failed to count JSON array size — defaulting to 0", e);
            return 0;
        }
    }
}
