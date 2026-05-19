package dev.brikk.ducklake.doris.plugin;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;

/**
 * Opaque DuckLake column handle. {@code columnId} is the stable DuckLake
 * identifier — it survives column rename and tracks Parquet field-id
 * resolution; {@code ordinalPosition} is the column's index in the table
 * schema (used by the planner for projection ordering).
 */
record DuckLakeColumnHandle(
        long columnId,
        String columnName,
        ConnectorType columnType,
        int ordinalPosition)
        implements ConnectorColumnHandle {
}
