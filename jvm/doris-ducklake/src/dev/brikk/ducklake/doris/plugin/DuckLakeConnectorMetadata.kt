package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import dev.brikk.ducklake.catalog.DucklakeSnapshot
import org.apache.doris.connector.api.ConnectorColumn
import org.apache.doris.connector.api.ConnectorDatabaseMetadata
import org.apache.doris.connector.api.ConnectorMetadata
import org.apache.doris.connector.api.ConnectorSession
import org.apache.doris.connector.api.ConnectorTableSchema
import org.apache.doris.connector.api.ConnectorTableStatistics
import org.apache.doris.connector.api.handle.ConnectorColumnHandle
import org.apache.doris.connector.api.handle.ConnectorTableHandle
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot
import org.apache.doris.connector.api.pushdown.ConnectorAnd
import org.apache.doris.connector.api.pushdown.ConnectorColumnAssignment
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef
import org.apache.doris.connector.api.pushdown.ConnectorComparison
import org.apache.doris.connector.api.pushdown.ConnectorExpression
import org.apache.doris.connector.api.pushdown.ConnectorFilterConstraint
import org.apache.doris.connector.api.pushdown.ConnectorLiteral
import org.apache.doris.connector.api.pushdown.FilterApplicationResult
import org.apache.doris.connector.api.pushdown.ProjectionApplicationResult
import java.time.Instant
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
        return catalog.getSchema(database, snapshotId) != null
    }

    override fun getDatabase(session: ConnectorSession?, database: String): ConnectorDatabaseMetadata {
        val snapshotId = catalog.currentSnapshotId
        val schema = catalog.getSchema(database, snapshotId)
            ?: throw IllegalArgumentException(
                "database '$database' not found in DuckLake catalog",
            )
        return ConnectorDatabaseMetadata(schema.schemaName, emptyMap<String, String>())
    }

    override fun listTableNames(session: ConnectorSession?, database: String): List<String> {
        val snapshotId = catalog.currentSnapshotId
        val schema = catalog.getSchema(database, snapshotId) ?: return emptyList()
        val tables = catalog.listTables(schema.schemaId, snapshotId)
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
        if (schema == null) {
            return Optional.empty()
        }
        val ducklakeTable = catalog.getTable(database, table, snapshotId)
        return Optional.ofNullable(
            ducklakeTable?.let { t ->
                DuckLakeTableHandle(
                    database, table, schema.schemaId, t.tableId, snapshotId,
                )
            },
        )
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

    // ---- Pushdown: projection (column pruning) ----

    override fun applyProjection(
        session: ConnectorSession?,
        handle: ConnectorTableHandle,
        projections: List<ConnectorColumnHandle>,
    ): Optional<ProjectionApplicationResult<ConnectorTableHandle>> {
        if (projections.isEmpty()) {
            return Optional.empty()
        }
        val dlHandle = handle.asDuckLakeHandle<DuckLakeTableHandle>()
        val projectedIds = projections.mapNotNull { (it as? DuckLakeColumnHandle)?.columnId }
        if (projectedIds.size != projections.size) {
            // Some handle wasn't one of ours — opt out rather than guess.
            return Optional.empty()
        }
        // Already projected to exactly this set: return empty so the engine's
        // fixed-point apply* loop terminates instead of re-applying forever.
        if (dlHandle.projectedColumnIds == projectedIds) {
            return Optional.empty()
        }

        val outProjections = ArrayList<ConnectorExpression>(projections.size)
        val outAssignments = ArrayList<ConnectorColumnAssignment>(projections.size)
        for (col in projections) {
            val ch = col as DuckLakeColumnHandle
            val ref = ConnectorColumnRef(ch.columnName, ch.columnType)
            outProjections.add(ref)
            outAssignments.add(ConnectorColumnAssignment(col, ref))
        }
        val newHandle = dlHandle.copy(projectedColumnIds = projectedIds)
        return Optional.of(
            ProjectionApplicationResult<ConnectorTableHandle>(newHandle, outProjections, outAssignments),
        )
    }

    // ---- Pushdown: filter (file-level statistics pruning) ----

    override fun applyFilter(
        session: ConnectorSession?,
        handle: ConnectorTableHandle,
        constraint: ConnectorFilterConstraint,
    ): Optional<FilterApplicationResult<ConnectorTableHandle>> {
        val dlHandle = handle.asDuckLakeHandle<DuckLakeTableHandle>()
        // Already applied on a prior fixed-point iteration → stop the loop.
        if (dlHandle.pushedFilter != null) {
            return Optional.empty()
        }
        val filter = constraint.expression ?: return Optional.empty()

        val columns = catalog.getTableColumns(dlHandle.tableId, dlHandle.snapshotId)

        // Two complementary prunings, intersected: column-stats ranges (cover
        // non-partition columns + IDENTITY/temporal partitions, whose source column
        // carries stats) and BUCKET-partition matching (which stats CAN'T do — each
        // bucket file holds a scattered value range).
        val statsKept = statsPrune(dlHandle, filter, columns.associate { it.columnName to it.columnId })
        val bucketKept = bucketPrune(dlHandle, filter, columns)

        val prunedFileIds = intersectNullable(statsKept, bucketKept)
            ?: return Optional.empty() // nothing pushable (functions / LIKE / OR only)

        val newHandle = dlHandle.copy(pushedFilter = filter, prunedFileIds = prunedFileIds)
        // Conservative: the BE re-evaluates the full predicate, so report the whole
        // expression as still-unenforced. File pruning is best-effort elimination.
        return Optional.of(FilterApplicationResult<ConnectorTableHandle>(newHandle, filter, false))
    }

    /** Stats-range pruning: a file survives only if its column stats overlap every pushed range. */
    private fun statsPrune(
        handle: DuckLakeTableHandle,
        filter: ConnectorExpression,
        columnIdByName: Map<String, Long>,
    ): Set<Long>? {
        val rangePredicates = DuckLakePredicateConverter.toColumnRangePredicates(filter, columnIdByName)
        if (rangePredicates.isEmpty()) {
            return null
        }
        var kept: Set<Long>? = null
        for (pred in rangePredicates) {
            val ids = catalog.findDataFileIdsInRange(handle.tableId, handle.snapshotId, pred).toHashSet()
            kept = kept?.intersect(ids) ?: ids
        }
        return kept
    }

    /** BUCKET-partition pruning: for `col = literal` on a bucket field, keep only files in bucket(literal). */
    private fun bucketPrune(
        handle: DuckLakeTableHandle,
        filter: ConnectorExpression,
        columns: List<DucklakeColumn>,
    ): Set<Long>? {
        val bucketFields = catalog.getPartitionSpecs(handle.tableId, handle.snapshotId)
            .flatMap { it.fields }
            .filter { it.transform == DucklakePartitionTransform.BUCKET }
        if (bucketFields.isEmpty()) {
            return null
        }
        val nameById = columns.associate { it.columnId to it.columnName }
        val equalities = equalityLiterals(filter)
        // partitionKeyIndex -> target bucket, for bucket fields with a usable `col = literal`.
        val targetBuckets = HashMap<Int, Int>()
        for (field in bucketFields) {
            val colName = nameById[field.columnId] ?: continue
            val arity = field.arity ?: continue
            val literal = equalities[colName] ?: continue
            val target = DuckLakeBucketTransform.bucket(literal, arity) ?: continue
            targetBuckets[field.partitionKeyIndex] = target
        }
        if (targetBuckets.isEmpty()) {
            return null
        }

        val fileValues = catalog.getFilePartitionValues(handle.tableId, handle.snapshotId)
        val kept = HashSet<Long>()
        for ((fileId, values) in fileValues) {
            val matches = targetBuckets.all { (keyIndex, target) ->
                val pv = values.find { it.partitionKeyIndex == keyIndex }?.partitionValue
                pv == null || pv.toIntOrNull() == target // null partition value → keep conservatively
            }
            if (matches) {
                kept.add(fileId)
            }
        }
        return kept
    }

    /** `col = literal` equalities with their TYPED literal value (needed for bucket hashing). */
    private fun equalityLiterals(filter: ConnectorExpression): Map<String, Any> {
        val conjuncts = when (filter) {
            is ConnectorAnd -> filter.conjuncts
            else -> listOf(filter)
        }
        val out = HashMap<String, Any>()
        for (conjunct in conjuncts) {
            if (conjunct is ConnectorComparison && conjunct.operator == ConnectorComparison.Operator.EQ) {
                val col = conjunct.left as? ConnectorColumnRef
                val lit = conjunct.right as? ConnectorLiteral
                val value = lit?.value
                if (col != null && lit != null && !lit.isNull && value != null) {
                    out[col.columnName] = value
                }
            }
        }
        return out
    }

    private fun intersectNullable(a: Set<Long>?, b: Set<Long>?): Set<Long>? = when {
        a == null -> b
        b == null -> a
        else -> a.intersect(b)
    }

    // ---- Statistics (planner cardinality) ----

    override fun getTableStatistics(
        session: ConnectorSession?,
        handle: ConnectorTableHandle,
    ): Optional<ConnectorTableStatistics> {
        val dlHandle = handle.asDuckLakeHandle<DuckLakeTableHandle>()
        // Table-level row count + on-disk size from ducklake_table_stats. v1 reports
        // whole-table stats; refining to the pushed-filter / pruned-file subset is a
        // later optimization (the planner applies filter selectivity on top).
        val stats = catalog.getTableStats(dlHandle.tableId) ?: return Optional.empty()
        return Optional.of(ConnectorTableStatistics(stats.recordCount, stats.fileSizeBytes))
    }

    // ---- MVCC snapshot pinning + time travel ----
    // Replaces the old FE→BE 24-byte snapshot codec: the P-series engine takes a
    // ConnectorMvccSnapshot from these methods and serializes it itself.

    override fun beginQuerySnapshot(
        session: ConnectorSession?,
        handle: ConnectorTableHandle,
    ): Optional<ConnectorMvccSnapshot> {
        // Pin the query to the snapshot already resolved on the table handle.
        val snapshotId = handle.asDuckLakeHandle<DuckLakeTableHandle>().snapshotId
        return Optional.of(toMvccSnapshot(snapshotId, catalog.getSnapshot(snapshotId)))
    }

    override fun getSnapshotAt(
        session: ConnectorSession?,
        handle: ConnectorTableHandle,
        timestampMillis: Long,
    ): Optional<ConnectorMvccSnapshot> {
        val snap = catalog.getSnapshotAtOrBefore(Instant.ofEpochMilli(timestampMillis))
            ?: return Optional.empty()
        return Optional.of(toMvccSnapshot(snap.snapshotId, snap))
    }

    override fun getSnapshotById(
        session: ConnectorSession?,
        handle: ConnectorTableHandle,
        snapshotId: Long,
    ): Optional<ConnectorMvccSnapshot> {
        val snap = catalog.getSnapshot(snapshotId) ?: return Optional.empty()
        return Optional.of(toMvccSnapshot(snap.snapshotId, snap))
    }

    private fun toMvccSnapshot(snapshotId: Long, snap: DucklakeSnapshot?): ConnectorMvccSnapshot {
        val builder = ConnectorMvccSnapshot.builder().snapshotId(snapshotId)
        if (snap != null) {
            builder.timestampMillis(snap.snapshotTime.toEpochMilli())
        }
        return builder.build()
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
