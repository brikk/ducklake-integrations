package dev.brikk.ducklake.doris.plugin;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import dev.brikk.ducklake.catalog.DucklakeCatalog;
import dev.brikk.ducklake.catalog.DucklakeColumn;
import dev.brikk.ducklake.catalog.DucklakeSchema;
import dev.brikk.ducklake.catalog.DucklakeTable;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorDatabaseMetadata;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;

/**
 * Read-side metadata for DuckLake. Today: schema + table listing. Pushdown,
 * statistics, time travel, and writes layer on top per {@code ducklake-doris-todo.md}.
 */
final class DuckLakeConnectorMetadata implements ConnectorMetadata {

    private final DucklakeCatalog catalog;

    DuckLakeConnectorMetadata(DucklakeCatalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        long snapshotId = catalog.getCurrentSnapshotId();
        List<DucklakeSchema> schemas = catalog.listSchemas(snapshotId);
        List<String> names = new ArrayList<>(schemas.size());
        for (DucklakeSchema schema : schemas) {
            names.add(schema.schemaName());
        }
        return names;
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String database) {
        long snapshotId = catalog.getCurrentSnapshotId();
        return catalog.getSchema(database, snapshotId).isPresent();
    }

    @Override
    public ConnectorDatabaseMetadata getDatabase(ConnectorSession session, String database) {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeSchema schema = catalog.getSchema(database, snapshotId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "database '" + database + "' not found in DuckLake catalog"));
        return new ConnectorDatabaseMetadata(schema.schemaName(), Map.of());
    }

    @Override
    public List<String> listTableNames(ConnectorSession session, String database) {
        long snapshotId = catalog.getCurrentSnapshotId();
        Optional<DucklakeSchema> schema = catalog.getSchema(database, snapshotId);
        if (schema.isEmpty()) {
            return List.of();
        }
        List<DucklakeTable> tables = catalog.listTables(schema.get().schemaId(), snapshotId);
        List<String> names = new ArrayList<>(tables.size());
        for (DucklakeTable table : tables) {
            names.add(table.tableName());
        }
        return names;
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String database, String table) {
        long snapshotId = catalog.getCurrentSnapshotId();
        Optional<DucklakeSchema> schema = catalog.getSchema(database, snapshotId);
        if (schema.isEmpty()) {
            return Optional.empty();
        }
        Optional<DucklakeTable> ducklakeTable = catalog.getTable(database, table, snapshotId);
        return ducklakeTable.map(t -> new DuckLakeTableHandle(
                database, table, schema.get().schemaId(), t.tableId(), snapshotId));
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle tableHandle) {
        DuckLakeTableHandle handle = asDuckLakeHandle(tableHandle);
        List<DucklakeColumn> rows = loadTopLevelColumns(handle);
        List<ConnectorColumn> connectorColumns = new ArrayList<>(rows.size());
        for (DucklakeColumn col : rows) {
            connectorColumns.add(new ConnectorColumn(
                    col.columnName(),
                    DuckLakeTypeMapping.fromDucklakeType(col.columnType()),
                    "",                    // comment — DuckLake doesn't track per-column comments yet
                    col.nullsAllowed(),
                    null));                // defaultValue — DuckLake doesn't surface column defaults yet
        }
        return new ConnectorTableSchema(
                handle.table(),
                connectorColumns,
                DuckLakeConnectorProvider.TYPE,
                Map.of());
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle tableHandle) {
        DuckLakeTableHandle handle = asDuckLakeHandle(tableHandle);
        List<DucklakeColumn> rows = loadTopLevelColumns(handle);
        // Doris's planner iterates this map for projection ordering; LinkedHashMap
        // preserves the catalog's column_order so DESC and SELECT * match.
        Map<String, ConnectorColumnHandle> out = new LinkedHashMap<>(rows.size());
        int ordinal = 0;
        for (DucklakeColumn col : rows) {
            out.put(col.columnName(), new DuckLakeColumnHandle(
                    col.columnId(),
                    col.columnName(),
                    DuckLakeTypeMapping.fromDucklakeType(col.columnType()),
                    ordinal++));
        }
        return out;
    }

    private List<DucklakeColumn> loadTopLevelColumns(DuckLakeTableHandle handle) {
        // Skip nested-field rows (struct/list/map children carry parentColumn);
        // the top-level column's columnType already encodes the nested shape as
        // a type string the mapper parses.
        List<DucklakeColumn> rows = catalog.getTableColumns(handle.tableId(), handle.snapshotId());
        List<DucklakeColumn> top = new ArrayList<>(rows.size());
        for (DucklakeColumn col : rows) {
            if (col.parentColumn().isEmpty()) {
                top.add(col);
            }
        }
        top.sort((a, b) -> Long.compare(a.columnOrder(), b.columnOrder()));
        return top;
    }

    private static DuckLakeTableHandle asDuckLakeHandle(ConnectorTableHandle handle) {
        if (handle instanceof DuckLakeTableHandle dlh) {
            return dlh;
        }
        throw new IllegalArgumentException(
                "Expected DuckLakeTableHandle, got " + handle.getClass().getName());
    }
}
