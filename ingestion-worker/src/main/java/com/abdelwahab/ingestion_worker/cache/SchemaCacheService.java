package com.abdelwahab.ingestion_worker.cache;

/**
 * Contract for caching Iceberg table schemas in a fast key-value store.
 *
 * <p>Entries are keyed by the fully-qualified Iceberg table name
 * (e.g. {@code iceberg.myproject.sales_data}) and hold the JSON-serialised
 * column list returned by the schema API.
 *
 * <p><b>Invalidation strategy — no TTL, event-driven:</b>
 * A table's schema only changes when new data is ingested. The ingestion worker
 * calls {@link #invalidate} immediately after a successful table write so that
 * the next schema request always fetches fresh metadata from Iceberg.
 *
 * @see RedisSchemaCacheService
 */
public interface SchemaCacheService extends AutoCloseable {

    /**
     * Returns the cached schema JSON for the given table, or {@code null} on a miss.
     *
     * @param icebergTable fully-qualified Iceberg table name
     * @return JSON array of column descriptors, or {@code null} if not cached
     */
    String get(String icebergTable);

    /**
     * Stores the schema JSON for the given table.
     *
     * @param icebergTable fully-qualified Iceberg table name
     * @param schemaJson   JSON-serialised column list,
     *                     e.g. {@code [{"name":"id","type":"int","nullable":true}, ...]}
     */
    void put(String icebergTable, String schemaJson);

    /**
     * Removes the cached entry so the next schema request re-fetches from Iceberg.
     *
     * <p>Called by the ingestion worker after every successful table write,
     * ensuring clients never observe a stale schema after columns are added.
     *
     * @param icebergTable fully-qualified Iceberg table name
     */
    void invalidate(String icebergTable);

    @Override
    void close();
}
