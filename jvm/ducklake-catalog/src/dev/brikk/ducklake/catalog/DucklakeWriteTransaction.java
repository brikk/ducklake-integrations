/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.brikk.ducklake.catalog;

import org.jooq.DSLContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Transactional context for write operations against the DuckLake catalog.
 * Handles snapshot state, ID allocation, and change tracking.
 * <p>
 * Instances are created by {@link JdbcDucklakeCatalog#executeWriteTransaction}
 * and must not be used outside the callback scope.
 */
class DucklakeWriteTransaction
{
    private final Connection connection;
    private final DSLContext dsl;
    private final long currentSnapshotId;
    private final long newSnapshotId;
    private long schemaVersion;
    private long nextCatalogId;
    private long nextFileId;
    private long schemaVersionTableId = -1;
    private final List<String> changes = new ArrayList<>();

    DucklakeWriteTransaction(Connection connection, DSLContext dsl, long currentSnapshotId,
            long schemaVersion, long nextCatalogId, long nextFileId)
    {
        this.connection = connection;
        this.dsl = dsl;
        this.currentSnapshotId = currentSnapshotId;
        this.newSnapshotId = currentSnapshotId + 1;
        this.schemaVersion = schemaVersion;
        this.nextCatalogId = nextCatalogId;
        this.nextFileId = nextFileId;
    }

    /**
     * Returns the connection-scoped {@link DSLContext} for building jOOQ statements that
     * must run inside this write transaction. All mutations go through this context so
     * they inherit the transaction's {@link Connection} and commit/rollback semantics.
     */
    public DSLContext dsl()
    {
        return dsl;
    }

    public long getCurrentSnapshotId()
    {
        return currentSnapshotId;
    }

    public long getNewSnapshotId()
    {
        return newSnapshotId;
    }

    public long getSchemaVersion()
    {
        return schemaVersion;
    }

    /**
     * Allocates a catalog ID for a new object (view, table, schema, etc.).
     * Returns the current value and advances for the next caller.
     */
    public long allocateCatalogId()
    {
        return nextCatalogId++;
    }

    /**
     * Allocates a file ID for a new data file.
     * Returns the current value and advances for the next caller.
     */
    public long allocateFileId()
    {
        return nextFileId++;
    }

    /**
     * Records a snapshot change description (e.g. "created_view:my_view").
     */
    public void addChange(String changeDescription)
    {
        changes.add(changeDescription);
    }

    /**
     * Resolves a schema name to its schema_id within the current snapshot.
     */
    public long resolveSchemaId(String schemaName)
            throws SQLException
    {
        try (PreparedStatement stmt = connection.prepareStatement(
                "SELECT schema_id FROM ducklake_schema " +
                        "WHERE schema_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)")) {
            stmt.setString(1, schemaName);
            stmt.setLong(2, currentSnapshotId);
            stmt.setLong(3, currentSnapshotId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("Schema not found: " + schemaName);
                }
                return rs.getLong("schema_id");
            }
        }
    }

    /**
     * Resolves a table name to its table_id within a schema at the current snapshot.
     */
    public long resolveTableId(long schemaId, String tableName)
            throws SQLException
    {
        try (PreparedStatement stmt = connection.prepareStatement(
                "SELECT table_id FROM ducklake_table " +
                        "WHERE schema_id = ? AND table_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)")) {
            stmt.setLong(1, schemaId);
            stmt.setString(2, tableName);
            stmt.setLong(3, currentSnapshotId);
            stmt.setLong(4, currentSnapshotId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("Table not found: " + tableName);
                }
                return rs.getLong("table_id");
            }
        }
    }

    /**
     * Checks whether a schema has any active tables at the current snapshot.
     */
    public boolean hasTablesInSchema(long schemaId)
            throws SQLException
    {
        try (PreparedStatement stmt = connection.prepareStatement(
                "SELECT 1 FROM ducklake_table " +
                        "WHERE schema_id = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL) LIMIT 1")) {
            stmt.setLong(1, schemaId);
            stmt.setLong(2, currentSnapshotId);
            stmt.setLong(3, currentSnapshotId);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    /**
     * Increments the schema version. Called for DDL operations that change
     * the table/column structure (create table, drop table, alter table, etc.).
     *
     * @param tableId the table being modified (used in ducklake_schema_versions)
     */
    public void incrementSchemaVersion(long tableId)
    {
        schemaVersion++;
        schemaVersionTableId = tableId;
    }

    /**
     * Increments the schema version for a non-table-scoped DDL operation
     * (create/drop view, create/drop schema). Leaves {@code table_id} as
     * SQL NULL in {@code ducklake_schema_versions}. Upstream
     * {@code DuckLakeTransaction::SchemaChangesMade()} flips on new/dropped
     * views and schemas the same way it flips on table DDL, so a DuckDB
     * reader that caches by {@code schema_version} must see the bump.
     */
    public void incrementSchemaVersion()
    {
        schemaVersion++;
    }

    long getSchemaVersionTableId()
    {
        return schemaVersionTableId;
    }

    /**
     * Returns the JDBC connection for preparing mutation statements.
     * The caller must not commit, rollback, or close this connection.
     */
    public Connection getConnection()
    {
        return connection;
    }

    // Package-private accessors for the framework to read final state

    long getFinalNextCatalogId()
    {
        return nextCatalogId;
    }

    long getFinalNextFileId()
    {
        return nextFileId;
    }

    List<String> getChanges()
    {
        return changes;
    }
}
