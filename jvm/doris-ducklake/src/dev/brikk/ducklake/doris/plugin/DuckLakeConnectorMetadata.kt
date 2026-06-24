package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeFilePartitionValue
import dev.brikk.ducklake.catalog.DucklakePartitionField
import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import dev.brikk.ducklake.catalog.DucklakeSnapshot
import dev.brikk.ducklake.catalog.TableColumnSpec
import org.apache.doris.connector.api.ConnectorColumn
import org.apache.doris.connector.api.ConnectorDatabaseMetadata
import org.apache.doris.connector.api.ConnectorMetadata
import org.apache.doris.connector.api.ConnectorSession
import org.apache.doris.connector.api.ConnectorTableSchema
import org.apache.doris.connector.api.ConnectorTableStatistics
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest
import org.apache.doris.connector.api.handle.ConnectorColumnHandle
import org.apache.doris.connector.api.handle.ConnectorTableHandle
import org.apache.doris.connector.api.handle.ConnectorTransaction
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec
import org.apache.doris.connector.api.pushdown.ConnectorAnd
import org.apache.doris.connector.api.pushdown.ConnectorColumnAssignment
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef
import org.apache.doris.connector.api.pushdown.ConnectorExpression
import org.apache.doris.connector.api.pushdown.ConnectorFilterConstraint
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

    /**
     * BUCKET-partition pruning: for a membership constraint (`col = x`, `col IN (…)`,
     * or same-column `col = a OR col = b …`) on a bucket field, keep only files whose
     * stored bucket is one of `{ bucket(v) : v ∈ candidates }`. Stats CAN'T do this —
     * each bucket file holds a scattered value range — so this is the one prune that
     * narrows bucket-partitioned scans.
     */
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
        val candidates = membershipCandidates(filter)
        val nameById = columns.associate { it.columnId to it.columnName }
        // partitionKeyIndex -> the set of buckets the candidate values hash to.
        val targetBuckets = bucketFields
            .mapNotNull { field -> targetBucketsFor(field, nameById, candidates) }
            .toMap()
        if (targetBuckets.isEmpty()) {
            return null
        }
        return catalog.getFilePartitionValues(handle.tableId, handle.snapshotId)
            .filterValues { values -> fileMatchesBuckets(values, targetBuckets) }
            .keys
    }

    /**
     * The `(partitionKeyIndex, target buckets)` a BUCKET field resolves to under its
     * column's membership candidates, or null to skip the field. Skips (keeps all
     * files) if any candidate can't be hashed — an unknown bucket means we can't
     * safely exclude any bucket for that field.
     */
    private fun targetBucketsFor(
        field: DucklakePartitionField,
        nameById: Map<Long, String>,
        candidates: Map<String, List<Any>>,
    ): Pair<Int, Set<Int>>? {
        val arity = field.arity ?: return null
        val values = nameById[field.columnId]?.let { candidates[it] } ?: return null
        val buckets = values.map { DuckLakeBucketTransform.bucket(it, arity) ?: return null }.toSet()
        return field.partitionKeyIndex to buckets
    }

    /** A file survives bucket pruning if, for every field, its stored bucket is a target (null = keep). */
    private fun fileMatchesBuckets(
        values: List<DucklakeFilePartitionValue>,
        targetBuckets: Map<Int, Set<Int>>,
    ): Boolean = targetBuckets.all { (keyIndex, targets) ->
        val pv = values.find { it.partitionKeyIndex == keyIndex }?.partitionValue
        pv == null || pv.toIntOrNull() in targets
    }

    /**
     * Per-column candidate values from every membership conjunct (`col = x`,
     * `col IN (…)`, same-column `col = a OR col = b …`), unioned across conjuncts.
     * Union stays safe: it can only widen a column's value set, never wrongly
     * narrow it, so bucket pruning never drops a file that could match.
     */
    private fun membershipCandidates(filter: ConnectorExpression): Map<String, List<Any>> {
        val conjuncts = if (filter is ConnectorAnd) filter.conjuncts else listOf(filter)
        val byColumn = LinkedHashMap<String, MutableList<Any>>()
        conjuncts.mapNotNull { DuckLakeMembership.of(it) }
            .forEach { (column, values) -> byColumn.getOrPut(column) { ArrayList() }.addAll(values) }
        return byColumn.mapValues { (_, values) -> values.distinct() }
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

    // The P-series SPI consolidated the former getSnapshotAt(timestampMillis) +
    // getSnapshotById(snapshotId) overrides into a single resolveTimeTravel that
    // receives a ConnectorTimeTravelSpec (FOR TIME AS OF / FOR VERSION AS OF, or
    // the @tag/@branch/@incr scan params). DuckLake supports only the linear-history
    // kinds — SNAPSHOT_ID (== old getSnapshotById) and TIMESTAMP (== old getSnapshotAt);
    // TAG/BRANCH/INCREMENTAL have no DuckLake equivalent and return empty so the engine
    // surfaces a clear "unsupported" user error.
    override fun resolveTimeTravel(
        session: ConnectorSession?,
        handle: ConnectorTableHandle,
        spec: ConnectorTimeTravelSpec,
    ): Optional<ConnectorMvccSnapshot> {
        val snap: DucklakeSnapshot? = when (spec.getKind()) {
            ConnectorTimeTravelSpec.Kind.SNAPSHOT_ID ->
                catalog.getSnapshot(spec.getStringValue().toLong())
            ConnectorTimeTravelSpec.Kind.TIMESTAMP ->
                catalog.getSnapshotAtOrBefore(Instant.ofEpochMilli(timestampMillisOf(spec)))
            else -> null // TAG / BRANCH / INCREMENTAL: unsupported by DuckLake
        }
        return if (snap == null) Optional.empty() else Optional.of(toMvccSnapshot(snap.snapshotId, snap))
    }

    // Derives epoch-millis from a TIMESTAMP spec: a digital value is the raw epoch-millis
    // (parity with the former getSnapshotAt(timestampMillis) caller), otherwise it is an
    // ISO-8601 / SQL datetime expression parsed as an Instant.
    private fun timestampMillisOf(spec: ConnectorTimeTravelSpec): Long =
        if (spec.isDigital()) {
            spec.getStringValue().toLong()
        } else {
            Instant.parse(spec.getStringValue()).toEpochMilli()
        }

    private fun toMvccSnapshot(snapshotId: Long, snap: DucklakeSnapshot?): ConnectorMvccSnapshot {
        val builder = ConnectorMvccSnapshot.builder().snapshotId(snapshotId)
        if (snap != null) {
            builder.timestampMillis(snap.snapshotTime.toEpochMilli())
        }
        return builder.build()
    }

    // ---- Write: INSERT via the connector-transaction model (P4 MaxCompute template) ----
    // The engine opens beginTransaction(), binds the resulting transaction to the
    // session, then DuckLakeWritePlanProvider.planWrite emits a TIcebergTableSink and
    // binds the target table onto the transaction; the BE writes Parquet and reports
    // TIcebergCommitData, which the transaction maps + commits. End-to-end validation
    // is the compose smoke (real BE), gated by fe-core's SPI_READY_TYPES + "ducklake".

    override fun supportsInsert(): Boolean = true

    override fun usesConnectorTransaction(): Boolean = true

    override fun beginTransaction(session: ConnectorSession?): ConnectorTransaction {
        val transactionId = requireNotNull(session) {
            "beginTransaction requires a session to allocate the transaction id"
        }.allocateTransactionId()
        return DuckLakeConnectorTransaction(transactionId, catalog)
    }

    // ---- DDL: CREATE/DROP DATABASE + TABLE (pure catalog metadata, no BE) ----
    // The FE (PluginDrivenExternalCatalog) resolves IF [NOT] EXISTS before calling these,
    // so they act unconditionally and let the catalog throw on a real conflict. Each
    // catalog op is its own atomic snapshot — the same primitives trino-ducklake drives.

    override fun supportsCreateDatabase(): Boolean = true

    override fun createDatabase(session: ConnectorSession?, database: String, properties: Map<String, String>?) {
        catalog.createSchema(database)
    }

    override fun dropDatabase(session: ConnectorSession?, database: String, ifExists: Boolean) {
        if (ifExists && catalog.getSchema(database, catalog.currentSnapshotId) == null) {
            return
        }
        // No CASCADE: the catalog refuses to drop a schema that still has tables.
        catalog.dropSchema(database)
    }

    override fun createTable(session: ConnectorSession?, request: ConnectorCreateTableRequest) {
        // Iceberg-style PARTITIONED BY (...) → DuckLake partition fields. A Doris
        // DISTRIBUTED BY clause (CRC32 hash, not murmur3) is rejected here rather than
        // mis-mapped — see DuckLakeCreatePartitionMapper.
        val partitionFields = DuckLakeCreatePartitionMapper.toPartitionFields(
            request.partitionSpec,
            request.bucketSpec,
        )
        val columns = request.columns.map { column ->
            TableColumnSpec(
                column.name,
                DuckLakeCreateTableMapper.toDucklakeType(column.type),
                column.isNullable,
                emptyList(), // scalar columns only in v1 (the type mapper rejects nested types)
            )
        }
        catalog.createTable(request.dbName, request.tableName, columns, partitionFields, null)
    }

    override fun dropTable(session: ConnectorSession?, tableHandle: ConnectorTableHandle) {
        val handle = tableHandle.asDuckLakeHandle<DuckLakeTableHandle>()
        catalog.dropTable(handle.database, handle.table)
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
