package dev.brikk.ducklake.doris.plugin;

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

/**
 * Opaque DuckLake table handle. FE passes this back to us for every per-table
 * operation (getTableSchema, getColumnHandles, applyFilter, plan-scan, …),
 * so it carries enough state to resolve the table without another catalog
 * round-trip.
 *
 * <p>{@code snapshotId} pins the read to a specific DuckLake snapshot — captured at
 * {@code getTableHandle} time so subsequent calls in the same query see a
 * consistent view even if other writers commit. Time-travel overloads (FOR
 * VERSION/TIMESTAMP AS OF) plug into the same field.
 */
record DuckLakeTableHandle(
        String database,
        String table,
        long schemaId,
        long tableId,
        long snapshotId)
        implements ConnectorTableHandle {
}
