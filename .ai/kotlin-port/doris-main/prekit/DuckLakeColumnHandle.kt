package dev.brikk.ducklake.doris.plugin

import org.apache.doris.connector.api.ConnectorType
import org.apache.doris.connector.api.handle.ConnectorColumnHandle

/**
 * Opaque DuckLake column handle. [columnId] is the stable DuckLake
 * identifier — it survives column rename and tracks Parquet field-id
 * resolution; [ordinalPosition] is the column's index in the table
 * schema (used by the planner for projection ordering).
 */
@JvmRecord
internal data class DuckLakeColumnHandle(
    val columnId: Long,
    val columnName: String,
    val columnType: ConnectorType,
    val ordinalPosition: Int,
) : ConnectorColumnHandle
