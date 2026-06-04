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
package dev.brikk.ducklake.catalog

import org.jooq.DSLContext
import java.sql.Connection
import java.sql.SQLException

/**
 * Transactional context for write operations against the DuckLake catalog.
 * Handles snapshot state, ID allocation, and change tracking.
 *
 * Instances are created by [JdbcDucklakeCatalog.executeWriteTransaction]
 * and must not be used outside the callback scope.
 *
 * Visibility note: Java's package-private (the original modifier on this
 * class and several of its methods) has no Kotlin equivalent. Kotlin's
 * `internal` mangles symbol names for Java callers (notably constructors
 * pick up a `DefaultConstructorMarker` parameter), which would break the
 * Java-side `JdbcDucklakeCatalog` callers. The class and its members are
 * therefore left at Kotlin's default `public` visibility; the package
 * remains the single-source-of-truth boundary the original design relied on.
 */
class DucklakeWriteTransaction(
    private val connection: Connection,
    private val dsl: DSLContext,
    private val currentSnapshotId: Long,
    private var schemaVersion: Long,
    private var nextCatalogId: Long,
    private var nextFileId: Long,
) {
    private val newSnapshotId: Long = currentSnapshotId + 1
    private var schemaVersionTableId: Long = -1
    private val changes: MutableList<WriteChange> = mutableListOf()

    /**
     * Returns the connection-scoped [DSLContext] for building jOOQ statements that
     * must run inside this write transaction. All mutations go through this context so
     * they inherit the transaction's [Connection] and commit/rollback semantics.
     */
    fun dsl(): DSLContext = dsl

    fun getCurrentSnapshotId(): Long = currentSnapshotId

    fun getNewSnapshotId(): Long = newSnapshotId

    fun getSchemaVersion(): Long = schemaVersion

    /**
     * Allocates a catalog ID for a new object (view, table, schema, etc.).
     * Returns the current value and advances for the next caller.
     */
    fun allocateCatalogId(): Long = nextCatalogId++

    /**
     * Allocates a file ID for a new data file.
     * Returns the current value and advances for the next caller.
     */
    fun allocateFileId(): Long = nextFileId++

    /**
     * Records a typed snapshot-change entry. Both the
     * `ducklake_snapshot_changes.changes_made` text serializer and the
     * logical conflict check read this list.
     */
    fun recordChange(change: WriteChange) {
        changes.add(change)
    }

    /**
     * Resolves a schema name to its schema_id within the current snapshot.
     */
    @Throws(SQLException::class)
    fun resolveSchemaId(schemaName: String): Long {
        connection.prepareStatement(
            "SELECT schema_id FROM ducklake_schema " +
                "WHERE schema_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)",
        ).use { stmt ->
            stmt.setString(1, schemaName)
            stmt.setLong(2, currentSnapshotId)
            stmt.setLong(3, currentSnapshotId)
            stmt.executeQuery().use { rs ->
                if (!rs.next()) {
                    throw RuntimeException("Schema not found: $schemaName")
                }
                return rs.getLong("schema_id")
            }
        }
    }

    /**
     * Resolves a table name to its table_id within a schema at the current snapshot.
     */
    @Throws(SQLException::class)
    fun resolveTableId(schemaId: Long, tableName: String): Long {
        connection.prepareStatement(
            "SELECT table_id FROM ducklake_table " +
                "WHERE schema_id = ? AND table_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)",
        ).use { stmt ->
            stmt.setLong(1, schemaId)
            stmt.setString(2, tableName)
            stmt.setLong(3, currentSnapshotId)
            stmt.setLong(4, currentSnapshotId)
            stmt.executeQuery().use { rs ->
                if (!rs.next()) {
                    throw RuntimeException("Table not found: $tableName")
                }
                return rs.getLong("table_id")
            }
        }
    }

    /**
     * Checks whether a schema has any active tables at the current snapshot.
     */
    @Throws(SQLException::class)
    fun hasTablesInSchema(schemaId: Long): Boolean {
        connection.prepareStatement(
            "SELECT 1 FROM ducklake_table " +
                "WHERE schema_id = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL) LIMIT 1",
        ).use { stmt ->
            stmt.setLong(1, schemaId)
            stmt.setLong(2, currentSnapshotId)
            stmt.setLong(3, currentSnapshotId)
            stmt.executeQuery().use { rs ->
                return rs.next()
            }
        }
    }

    /**
     * Increments the schema version. Called for DDL operations that change
     * the table/column structure (create table, drop table, alter table, etc.).
     *
     * @param tableId the table being modified (used in ducklake_schema_versions)
     */
    fun incrementSchemaVersion(tableId: Long) {
        schemaVersion++
        schemaVersionTableId = tableId
    }

    /**
     * Increments the schema version for a non-table-scoped DDL operation
     * (create/drop view, create/drop schema). Leaves `table_id` as
     * SQL NULL in `ducklake_schema_versions`. Upstream
     * `DuckLakeTransaction::SchemaChangesMade()` flips on new/dropped
     * views and schemas the same way it flips on table DDL, so a DuckDB
     * reader that caches by `schema_version` must see the bump.
     */
    fun incrementSchemaVersion() {
        schemaVersion++
    }

    fun getSchemaVersionTableId(): Long = schemaVersionTableId

    /**
     * Returns the JDBC connection for preparing mutation statements.
     * The caller must not commit, rollback, or close this connection.
     */
    fun getConnection(): Connection = connection

    // Package-private accessors for the framework to read final state
    // (left public — see class-level visibility note)

    fun getFinalNextCatalogId(): Long = nextCatalogId

    fun getFinalNextFileId(): Long = nextFileId

    fun getChanges(): List<WriteChange> = changes
}
