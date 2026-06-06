package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeColumn
import org.apache.doris.connector.api.ConnectorColumn
import org.apache.doris.connector.api.ConnectorDatabaseMetadata
import org.apache.doris.connector.api.ConnectorMetadata
import org.apache.doris.connector.api.ConnectorSession
import org.apache.doris.connector.api.ConnectorTableSchema
import org.apache.doris.connector.api.handle.ConnectorColumnHandle
import org.apache.doris.connector.api.handle.ConnectorTableHandle
import java.util.Optional

/**
 * Read-side metadata for DuckLake. Today: schema + table listing. Pushdown,
 * statistics, time travel, and writes layer on top per `ducklake-doris-todo.md`.
 */
internal class DuckLakeConnectorMetadata(
    private val catalog: DucklakeCatalog,
) : ConnectorMetadata {

    override fun listDatabaseNames(session: ConnectorSession?): List<String> {
        val snapshotId = catalog.currentSnapshotId
        val schemas = catalog.listSchemas(snapshotId)
        val names = ArrayList<String>(schemas.size)
        for (schema in schemas) {
            names.add(schema.schemaName)
        }
        return names
    }

    override fun databaseExists(session: ConnectorSession?, database: String): Boolean {
        val snapshotId = catalog.currentSnapshotId
        return catalog.getSchema(database, snapshotId).isPresent
    }

    override fun getDatabase(session: ConnectorSession?, database: String): ConnectorDatabaseMetadata {
        val snapshotId = catalog.currentSnapshotId
        val schema = catalog.getSchema(database, snapshotId)
            .orElseThrow {
                IllegalArgumentException(
                    "database '$database' not found in DuckLake catalog",
                )
            }
        return ConnectorDatabaseMetadata(schema.schemaName, emptyMap<String, String>())
    }

    override fun listTableNames(session: ConnectorSession?, database: String): List<String> {
        val snapshotId = catalog.currentSnapshotId
        val schema = catalog.getSchema(database, snapshotId)
        if (schema.isEmpty) {
            return emptyList()
        }
        val tables = catalog.listTables(schema.get().schemaId, snapshotId)
        val names = ArrayList<String>(tables.size)
        for (table in tables) {
            names.add(table.tableName)
        }
        return names
    }

    override fun getTableHandle(
        session: ConnectorSession?,
        database: String,
        table: String,
    ): Optional<ConnectorTableHandle> {
        val snapshotId = catalog.currentSnapshotId
        val schema = catalog.getSchema(database, snapshotId)
        if (schema.isEmpty) {
            return Optional.empty()
        }
        val ducklakeTable = catalog.getTable(database, table, snapshotId)
        return ducklakeTable.map { t ->
            DuckLakeTableHandle(
                database, table, schema.get().schemaId, t.tableId, snapshotId,
            )
        }
    }

    override fun getTableSchema(
        session: ConnectorSession?,
        tableHandle: ConnectorTableHandle,
    ): ConnectorTableSchema {
        val handle = tableHandle.asDuckLakeHandle<DuckLakeTableHandle>()
        val rows = loadTopLevelColumns(handle)
        val connectorColumns = ArrayList<ConnectorColumn>(rows.size)
        for (col in rows) {
            connectorColumns.add(
                ConnectorColumn(
                    col.columnName,
                    DuckLakeTypeMapping.fromDucklakeType(col.columnType),
                    "", // comment — DuckLake doesn't track per-column comments yet
                    col.nullsAllowed,
                    null, // defaultValue — DuckLake doesn't surface column defaults yet
                ),
            )
        }
        return ConnectorTableSchema(
            handle.table,
            connectorColumns,
            DuckLakeConnectorProvider.TYPE,
            emptyMap<String, String>(),
        )
    }

    override fun getColumnHandles(
        session: ConnectorSession?,
        tableHandle: ConnectorTableHandle,
    ): Map<String, ConnectorColumnHandle> {
        val handle = tableHandle.asDuckLakeHandle<DuckLakeTableHandle>()
        val rows = loadTopLevelColumns(handle)
        // Doris's planner iterates this map for projection ordering; LinkedHashMap
        // preserves the catalog's column_order so DESC and SELECT * match.
        val out = LinkedHashMap<String, ConnectorColumnHandle>(rows.size)
        var ordinal = 0
        for (col in rows) {
            out[col.columnName] = DuckLakeColumnHandle(
                col.columnId,
                col.columnName,
                DuckLakeTypeMapping.fromDucklakeType(col.columnType),
                ordinal++,
            )
        }
        return out
    }

    private fun loadTopLevelColumns(handle: DuckLakeTableHandle): List<DucklakeColumn> {
        // Skip nested-field rows (struct/list/map children carry parentColumn);
        // the top-level column's columnType already encodes the nested shape as
        // a type string the mapper parses.
        val rows = catalog.getTableColumns(handle.tableId, handle.snapshotId)
        val top = ArrayList<DucklakeColumn>(rows.size)
        for (col in rows) {
            if (col.parentColumn == null) {
                top.add(col)
            }
        }
        top.sortWith { a, b -> a.columnOrder.compareTo(b.columnOrder) }
        return top
    }
}
