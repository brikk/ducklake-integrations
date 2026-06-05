package dev.brikk.ducklake.doris.plugin

import org.apache.doris.connector.api.handle.ConnectorTableHandle

/**
 * Opaque DuckLake table handle. FE passes this back to us for every per-table
 * operation (getTableSchema, getColumnHandles, applyFilter, plan-scan, …),
 * so it carries enough state to resolve the table without another catalog
 * round-trip.
 *
 * [snapshotId] pins the read to a specific DuckLake snapshot — captured at
 * `getTableHandle` time so subsequent calls in the same query see a
 * consistent view even if other writers commit. Time-travel overloads (FOR
 * VERSION/TIMESTAMP AS OF) plug into the same field.
 */
@JvmRecord
internal data class DuckLakeTableHandle(
    val database: String,
    val table: String,
    val schemaId: Long,
    val tableId: Long,
    val snapshotId: Long,
) : ConnectorTableHandle
