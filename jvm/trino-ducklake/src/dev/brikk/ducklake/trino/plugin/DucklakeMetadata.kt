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
package dev.brikk.ducklake.trino.plugin

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import io.airlift.json.JsonCodec
import io.airlift.json.JsonCodecFactory
import io.airlift.log.Logger
import io.airlift.slice.Slice
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeColumnStats
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import dev.brikk.ducklake.catalog.TransactionConflictException
import dev.brikk.ducklake.catalog.DucklakeDataFile
import dev.brikk.ducklake.catalog.DucklakeInlinedDataInfo
import dev.brikk.ducklake.catalog.DucklakePartitionField
import dev.brikk.ducklake.catalog.DucklakePartitionSpec
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.DucklakeTableStats
import dev.brikk.ducklake.catalog.DucklakeSortKey
import dev.brikk.ducklake.catalog.DucklakeView
import dev.brikk.ducklake.catalog.PartitionFieldSpec
import dev.brikk.ducklake.catalog.TableColumnSpec
import dev.brikk.ducklake.catalog.TableLocationSpec
import io.trino.spi.TrinoException
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.connector.ColumnMetadata
import io.trino.spi.connector.ColumnNotFoundException
import io.trino.spi.connector.ColumnPosition
import io.trino.spi.connector.ConnectorAnalyzeMetadata
import io.trino.spi.connector.ConnectorInsertTableHandle
import io.trino.spi.connector.ConnectorMergeTableHandle
import io.trino.spi.connector.ConnectorMetadata
import io.trino.spi.connector.ConnectorOutputMetadata
import io.trino.spi.connector.ConnectorOutputTableHandle
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.connector.ConnectorTableHandle
import io.trino.spi.connector.ConnectorTableLayout
import io.trino.spi.connector.ConnectorTableMetadata
import io.trino.spi.connector.ConnectorTableProperties
import io.trino.spi.connector.ConnectorTableVersion
import io.trino.spi.connector.PointerType
import io.trino.spi.connector.LocalProperty
import io.trino.spi.connector.ConnectorViewDefinition
import io.trino.spi.connector.Constraint
import io.trino.spi.connector.ConstraintApplicationResult
import io.trino.spi.connector.RetryMode
import io.trino.spi.connector.RowChangeParadigm
import io.trino.spi.connector.SaveMode
import io.trino.spi.connector.SchemaTableName
import io.trino.spi.connector.SchemaTablePrefix
import io.trino.spi.connector.SortItem
import io.trino.spi.connector.TableFunctionApplicationResult
import io.trino.spi.connector.TopNApplicationResult
import io.trino.spi.function.table.ConnectorTableFunctionHandle
import io.trino.spi.connector.ViewNotFoundException
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.security.TrinoPrincipal
import io.trino.spi.statistics.ColumnStatistics
import io.trino.spi.statistics.ComputedStatistics
import io.trino.spi.statistics.DoubleRange
import io.trino.spi.statistics.Estimate
import io.trino.spi.statistics.TableStatisticType
import io.trino.spi.statistics.TableStatistics
import io.trino.spi.statistics.TableStatisticsMetadata
import io.trino.spi.type.ArrayType
import io.trino.spi.type.LongTimestamp
import io.trino.spi.type.LongTimestampWithTimeZone
import io.trino.spi.type.MapType
import io.trino.spi.type.RowType
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.Type

import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.LinkedHashMap
import java.util.Locale
import java.util.Optional
import java.util.OptionalLong

import com.google.common.collect.ImmutableList.toImmutableList
import com.google.common.collect.ImmutableMap.toImmutableMap
import io.trino.spi.StandardErrorCode.ALREADY_EXISTS
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.DateTimeEncoding.unpackMillisUtc
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.SmallintType.SMALLINT
import io.trino.spi.type.TimestampType.TIMESTAMP_MICROS
import io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS
import io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS
import io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND
import io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND
import io.trino.spi.type.TinyintType.TINYINT
import io.trino.spi.type.VarcharType.VARCHAR
import java.lang.Math.floorDiv
import java.lang.Math.floorMod
import java.util.Objects.requireNonNull

/**
 * The active partition spec is the most-recently-added one (highest version), or empty if the
 * table has none. Shared by the metadata write paths and the add_files procedure.
 */
internal fun activePartitionSpecOf(specs: List<DucklakePartitionSpec>): Optional<DucklakePartitionSpec> =
        if (specs.isEmpty()) Optional.empty() else Optional.of(specs.last())

/**
 * Metadata implementation for Ducklake connector.
 * Provides access to Ducklake tables and views via SQL catalog.
 */
class DucklakeMetadata(
        catalog: DucklakeCatalog?,
        typeConverter: DucklakeTypeConverter?,
        snapshotResolver: DucklakeSnapshotResolver?,
        fragmentCodec: JsonCodec<DucklakeWriteFragment>?,
        deleteFragmentCodec: JsonCodec<DucklakeDeleteFragment>?,
        pathResolver: DucklakePathResolver?,
        temporalPartitionEncoding: DucklakeTemporalPartitionEncoding?)
        : ConnectorMetadata
{
    private val catalog: DucklakeCatalog = requireNonNull(catalog, "catalog is null")!!
    private val typeConverter: DucklakeTypeConverter = requireNonNull(typeConverter, "typeConverter is null")!!
    private val snapshotResolver: DucklakeSnapshotResolver = requireNonNull(snapshotResolver, "snapshotResolver is null")!!
    private val fragmentCodec: JsonCodec<DucklakeWriteFragment>? = fragmentCodec
    private val deleteFragmentCodec: JsonCodec<DucklakeDeleteFragment>? = deleteFragmentCodec
    private val pathResolver: DucklakePathResolver? = pathResolver
    private val temporalPartitionEncoding: DucklakeTemporalPartitionEncoding = requireNonNull(temporalPartitionEncoding, "temporalPartitionEncoding is null")!!

    constructor(catalog: DucklakeCatalog?, typeConverter: DucklakeTypeConverter?)
            : this(catalog, typeConverter, DucklakeSnapshotResolver(requireNonNull(catalog, "catalog is null")!!, null, null), null, null, null, DucklakeTemporalPartitionEncoding.CALENDAR)

    constructor(catalog: DucklakeCatalog?, typeConverter: DucklakeTypeConverter?, snapshotResolver: DucklakeSnapshotResolver?)
            : this(catalog, typeConverter, snapshotResolver, null, null, null, DucklakeTemporalPartitionEncoding.CALENDAR)

    override fun listSchemaNames(session: ConnectorSession): List<String>
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        return catalog.listSchemas(snapshotId).stream()
                .map(DucklakeSchema::schemaName)
                .collect(toImmutableList())
    }

    override fun getTableHandle(
            session: ConnectorSession,
            tableName: SchemaTableName,
            startVersion: Optional<ConnectorTableVersion>,
            endVersion: Optional<ConnectorTableVersion>): ConnectorTableHandle?
    {
        if (startVersion.isPresent && endVersion.isPresent) {
            throw TrinoException(NOT_SUPPORTED, "DuckLake does not support version ranges; provide only one table version bound")
        }

        val queryVersion: Optional<ConnectorTableVersion> = if (endVersion.isPresent) endVersion else startVersion
        var querySnapshotId: Long? = null
        var querySnapshotTimestamp: Instant? = null
        if (queryVersion.isPresent) {
            val version = queryVersion.get()
            when (version.pointerType) {
                PointerType.TARGET_ID -> querySnapshotId = getSnapshotIdFromVersion(version)
                PointerType.TEMPORAL -> querySnapshotTimestamp = getSnapshotTimestampFromVersion(session, version)
                else -> {}
            }
        }

        val metadataTable: MetadataTableName? = parseMetadataTableName(tableName)
        val baseTableName: SchemaTableName =
                if (metadataTable != null) SchemaTableName(tableName.schemaName, metadataTable.baseTableName)
                else tableName

        val snapshotId = snapshotResolver.resolveSnapshotId(session, querySnapshotId, querySnapshotTimestamp)

        val table: DucklakeTable = catalog.getTable(baseTableName.schemaName, baseTableName.tableName, snapshotId)
            ?: return null

        if (metadataTable != null) {
            val parsed = metadataTable
            return DucklakeMetadataTableHandle(
                    tableName.schemaName,
                    tableName.tableName,
                    parsed.baseTableName,
                    table.tableId,
                    snapshotId,
                    parsed.metadataTableType)
        }

        return DucklakeTableHandle(
                tableName.schemaName,
                tableName.tableName,
                table.tableId,
                snapshotId)
    }

    override fun getTableMetadata(session: ConnectorSession, tableHandle: ConnectorTableHandle): ConnectorTableMetadata
    {
        if (tableHandle is DucklakeMetadataTableHandle) {
            return ConnectorTableMetadata(
                    tableHandle.getSchemaTableName(),
                    getMetadataColumns(tableHandle.metadataTableType))
        }

        if (tableHandle is ChangeFeedTableHandle) {
            // PTF-scan handle (applyTableFunction). Reached by EXPLAIN / plan printing; the
            // synthetic name is the change-feed function, columns are the function output.
            return ConnectorTableMetadata(
                    SchemaTableName("system", tableHandle.feedType.functionName),
                    tableHandle.outputColumns().map { column ->
                        ColumnMetadata.builder()
                                .setName(column.columnName)
                                .setType(column.columnType)
                                .setNullable(column.nullable)
                                .build()
                    })
        }

        if (tableHandle is LanceSearchTableHandle) {
            // PTF-scan handle (applyTableFunction). Reached by EXPLAIN / plan printing; the
            // synthetic name is the backing DuckDB function, columns are the function output.
            return ConnectorTableMetadata(
                    SchemaTableName("system", LanceSearchSplitProcessor.scanFunctionFor(tableHandle.search())),
                    tableHandle.search().outputColumns.map { column ->
                        ColumnMetadata.builder()
                                .setName(column.columnName)
                                .setType(column.columnType)
                                .setNullable(column.nullable)
                                .build()
                    })
        }

        val ducklakeTableHandle = tableHandle as DucklakeTableHandle

        val columns: List<DucklakeColumn> = catalog.getTableColumns(
                ducklakeTableHandle.tableId,
                ducklakeTableHandle.snapshotId)
        val columnComments: Map<Long, String> = catalog.getColumnComments(
                ducklakeTableHandle.tableId,
                ducklakeTableHandle.snapshotId)

        val columnMetadata: ImmutableList.Builder<ColumnMetadata> = ImmutableList.builder()
        for (column in columns) {
            columnMetadata.add(ColumnMetadata.builder()
                    .setName(column.columnName)
                    .setType(typeConverter.toTrinoType(column.columnType))
                    .setNullable(column.nullsAllowed)
                    .setComment(Optional.ofNullable(columnComments[column.columnId]))
                    .build())
        }
        // Hidden virtual columns must be present here (with hidden=true) to be resolvable by
        // name — the hidden flag is what keeps them out of SELECT * / DESCRIBE, not their
        // absence. Keep this list in lockstep with getColumnHandles.
        for (kind in EXPOSED_VIRTUAL_COLUMNS) {
            columnMetadata.add(ColumnMetadata.builder()
                    .setName(kind.columnName)
                    .setType(kind.columnType)
                    .setNullable(true)
                    .setHidden(true)
                    .build())
        }

        return ConnectorTableMetadata(
                ducklakeTableHandle.getSchemaTableName(),
                columnMetadata.build(),
                mapOf(),
                Optional.ofNullable(catalog.getTableComment(
                        ducklakeTableHandle.tableId,
                        ducklakeTableHandle.snapshotId)))
    }

    override fun getTableProperties(session: ConnectorSession, table: ConnectorTableHandle): ConnectorTableProperties
    {
        if (table !is DucklakeTableHandle) {
            return ConnectorTableProperties()
        }
        val handle: DucklakeTableHandle = table
        val sortKeys: List<DucklakeSortKey> = catalog.getSortKeys(handle.tableId, handle.snapshotId)
        if (sortKeys.isEmpty()) {
            return ConnectorTableProperties()
        }
        val columnHandlesByLowercaseName: MutableMap<String, ColumnHandle> = mutableMapOf()
        for (column in catalog.getTableColumns(handle.tableId, handle.snapshotId)) {
            columnHandlesByLowercaseName[column.columnName.lowercase(Locale.ROOT)] = DucklakeColumnHandle(
                    column.columnId,
                    column.columnName,
                    typeConverter.toTrinoType(column.columnType),
                    column.nullsAllowed)
        }
        val localProperties: List<LocalProperty<ColumnHandle>> =
                DucklakeSortPropertyMapper.toLocalProperties(sortKeys, columnHandlesByLowercaseName)
        if (localProperties.isEmpty()) {
            return ConnectorTableProperties()
        }
        return ConnectorTableProperties(
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                localProperties)
    }

    override fun listTables(session: ConnectorSession, schemaName: Optional<String>): List<SchemaTableName>
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        val relations: ImmutableList.Builder<SchemaTableName> = ImmutableList.builder()

        if (schemaName.isPresent) {
            val schema: DucklakeSchema = catalog.getSchema(schemaName.get(), snapshotId)
                ?: return ImmutableList.of()
            for (table in catalog.listTables(schema.schemaId, snapshotId)) {
                relations.add(SchemaTableName(schemaName.get(), table.tableName))
            }
            for (view in catalog.listViews(schema.schemaId, snapshotId)) {
                if (isViewAccessible(view)) {
                    relations.add(SchemaTableName(schemaName.get(), view.viewName))
                }
            }
            return relations.build()
        }

        for (schema in catalog.listSchemas(snapshotId)) {
            for (table in catalog.listTables(schema.schemaId, snapshotId)) {
                relations.add(SchemaTableName(schema.schemaName, table.tableName))
            }
            for (view in catalog.listViews(schema.schemaId, snapshotId)) {
                if (isViewAccessible(view)) {
                    relations.add(SchemaTableName(schema.schemaName, view.viewName))
                }
            }
        }
        return relations.build()
    }

    override fun getColumnHandles(session: ConnectorSession, tableHandle: ConnectorTableHandle): Map<String, ColumnHandle>
    {
        if (tableHandle is DucklakeMetadataTableHandle) {
            return toColumnHandles(getMetadataColumns(tableHandle.metadataTableType))
        }

        if (tableHandle is LanceSearchTableHandle) {
            // PTF-scan handle: the column handles are fixed at analyze time (no virtuals).
            return tableHandle.search().outputColumns.associateBy { it.columnName }
        }

        if (tableHandle is ChangeFeedTableHandle) {
            // PTF-scan handle: output columns (snapshot_id/rowid[/change_type] + table columns).
            return tableHandle.outputColumns().associateBy { it.columnName }
        }

        val ducklakeTableHandle = tableHandle as DucklakeTableHandle

        val columns: List<DucklakeColumn> = catalog.getTableColumns(
                ducklakeTableHandle.tableId,
                ducklakeTableHandle.snapshotId)

        val columnHandles: ImmutableMap.Builder<String, ColumnHandle> = ImmutableMap.builder()
        for (column in columns) {
            columnHandles.put(
                    column.columnName,
                    DucklakeColumnHandle(
                            column.columnId,
                            column.columnName,
                            typeConverter.toTrinoType(column.columnType),
                            column.nullsAllowed))
        }
        // Append the hidden virtual columns ($path, $snapshot_id, $file_row_number, $row_id,
        // $file_size_bytes). They DO appear in getTableMetadata too (each setHidden(true)) — the
        // hidden flag, not absence, is what keeps them out of SELECT * / DESCRIBE while letting a
        // column reference resolve. Keep this list in lockstep with getTableMetadata. See
        // DESIGN-virtual-columns.md.
        for (kind in EXPOSED_VIRTUAL_COLUMNS) {
            columnHandles.put(kind.columnName, kind.columnHandle())
        }
        return columnHandles.buildOrThrow()
    }

    override fun getColumnMetadata(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle,
            columnHandle: ColumnHandle): ColumnMetadata
    {
        val ducklakeColumnHandle = columnHandle as DucklakeColumnHandle

        return ColumnMetadata.builder()
                .setName(ducklakeColumnHandle.columnName)
                .setType(ducklakeColumnHandle.columnType)
                .setNullable(ducklakeColumnHandle.nullable)
                .setHidden(ducklakeColumnHandle.isVirtual())
                .build()
    }

    override fun getTableStatistics(session: ConnectorSession, tableHandle: ConnectorTableHandle): TableStatistics
    {
        if (tableHandle is DucklakeMetadataTableHandle || tableHandle is LanceSearchTableHandle
                || tableHandle is ChangeFeedTableHandle) {
            return TableStatistics.empty()
        }

        val table = tableHandle as DucklakeTableHandle
        val dataFiles: List<DucklakeDataFile> = catalog.getDataFiles(table.tableId, table.snapshotId)
        val hasDeleteFiles = dataFiles.stream().anyMatch { dataFile -> dataFile.deleteFilePath != null }
        if (hasDeleteFiles) {
            // Conservative mode: delete-file snapshots can make row-level table stats stale across engines.
            // Prefer unknown over wrong.
            return TableStatistics.empty()
        }
        if (catalog.hasInlinedDeletes(table.tableId, table.snapshotId)) {
            // Same conservative policy for inlined deletes: file-level stats predate the
            // deletions, so any column min/max/null fractions can be wrong relative to the
            // surviving rows. Prefer unknown over wrong.
            return TableStatistics.empty()
        }

        val hasLiveInlinedRows = hasLiveInlinedRows(table)

        val tableStats: DucklakeTableStats? = catalog.getTableStats(table.tableId)
        val recordCount: Long
        if (tableStats != null) {
            recordCount = tableStats.recordCount
        }
        else {
            val fallbackRecordCount = getFallbackRecordCount(table)
                    ?: return TableStatistics.empty()
            recordCount = fallbackRecordCount
        }

        val stats: TableStatistics.Builder = TableStatistics.builder()
                .setRowCount(Estimate.of(recordCount.toDouble()))

        if (recordCount == 0L) {
            return stats.build()
        }

        val columnHandles: Map<String, ColumnHandle> = getColumnHandles(session, tableHandle)

        if (hasLiveInlinedRows) {
            // Conservative mode: when mixed inlined rows are present, file-level column statistics
            // cover only the Parquet portion and can become misleading. Keep only row-count stats.
            return stats.build()
        }

        val seenDataFileIds: MutableSet<Long> = mutableSetOf()
        var activeDataFileRowCount: Long = 0
        for (dataFile in dataFiles) {
            if (seenDataFileIds.add(dataFile.dataFileId)) {
                activeDataFileRowCount += dataFile.recordCount
            }
        }

        val columnStatsList: List<DucklakeColumnStats> = catalog.getColumnStats(table.tableId, table.snapshotId)

        // Index column stats by column ID
        val statsById: Map<Long, DucklakeColumnStats> = columnStatsList.stream()
                .collect(toImmutableMap(DucklakeColumnStats::columnId) { s -> s })

        for (handle in columnHandles.values) {
            val column = handle as DucklakeColumnHandle
            val colStats: DucklakeColumnStats? = statsById[column.columnId]
            if (colStats == null) {
                continue
            }

            val colBuilder: ColumnStatistics.Builder = ColumnStatistics.builder()

            val totalCount = colStats.totalValueCount + colStats.totalNullCount
            if (activeDataFileRowCount > 0 && totalCount != activeDataFileRowCount) {
                // If file-level stats for this column do not cover all active data-file rows
                // (e.g., column added via schema evolution), expose unknown instead of wrong.
                continue
            }
            if (totalCount > 0) {
                colBuilder.setNullsFraction(Estimate.of(colStats.totalNullCount.toDouble() / totalCount.toDouble()))
            }

            if (colStats.totalSizeBytes > 0) {
                colBuilder.setDataSize(Estimate.of(colStats.totalSizeBytes.toDouble()))
            }

            toDoubleRange(column.columnType, colStats)?.let { colBuilder.setRange(it) }

            stats.setColumnStatistics(column, colBuilder.build())
        }

        return stats.build()
    }

    // ANALYZE — refresh DuckLake's cached table-level statistics. The engine scans the table to
    // produce an authoritative live ROW_COUNT (net of deletes/inlined rows); the connector then
    // rebuilds the per-column aggregates from the authoritative per-file stats in the catalog
    // (DucklakeCatalog.analyzeTable), which also tightens any min/max that incremental maintenance
    // left stale after a delete. We deliberately do NOT declare column statistics here: doing so
    // would force the engine to hand back typed min/max blocks we'd have to re-encode into
    // DuckLake's string form, duplicating logic the per-file aggregation already gets right.
    override fun getStatisticsCollectionMetadata(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle,
            analyzeProperties: Map<String, Any>): ConnectorAnalyzeMetadata
    {
        if (tableHandle is DucklakeMetadataTableHandle || tableHandle is LanceSearchTableHandle
                || tableHandle is ChangeFeedTableHandle) {
            throw TrinoException(NOT_SUPPORTED, "ANALYZE is not supported for this table")
        }
        val statisticsMetadata = TableStatisticsMetadata(
                emptySet(),
                setOf(TableStatisticType.ROW_COUNT),
                emptyList())
        return ConnectorAnalyzeMetadata(tableHandle, statisticsMetadata)
    }

    override fun beginStatisticsCollection(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle): ConnectorTableHandle =
            // No scan-side preparation needed — the refresh runs entirely against the catalog in
            // finishStatisticsCollection. Pass the handle straight through.
            tableHandle

    override fun finishStatisticsCollection(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle,
            computedStatistics: Collection<ComputedStatistics>)
    {
        val table = tableHandle as DucklakeTableHandle
        // ANALYZE without GROUP BY yields a single ungrouped ComputedStatistics; sum defensively.
        var rowCount = 0L
        for (statistics in computedStatistics) {
            val block = statistics.tableStatistics[TableStatisticType.ROW_COUNT] ?: continue
            if (!block.isNull(0)) {
                rowCount += BIGINT.getLong(block, 0)
            }
        }
        translateCatalogExceptions { catalog.analyzeTable(table.tableId, rowCount) }
    }

    private fun hasLiveInlinedRows(table: DucklakeTableHandle): Boolean =
            // getInlinedDataInfos carries a per-descriptor live-rows flag (resolved in the same probe
            // that lists it), so no second per-table hasInlinedRows round trip is needed here.
            catalog.getInlinedDataInfos(table.tableId, table.snapshotId).any { it.hasLiveRows }

    private fun getFallbackRecordCount(table: DucklakeTableHandle): Long?
    {
        // Align with Iceberg/Delta behavior: if we can prove there is no data at this snapshot,
        // return row count 0 instead of unknown.
        if (!catalog.getDataFiles(table.tableId, table.snapshotId).isEmpty()) {
            // Data files exist but no table stats were found. Keep row count unknown.
            return null
        }

        val inlinedInfos: List<DucklakeInlinedDataInfo> = catalog.getInlinedDataInfos(table.tableId, table.snapshotId)
        if (inlinedInfos.isEmpty()) {
            return 0
        }

        // Count via SELECT COUNT(*) per schema version rather than materialising the inlined
        // row payload into memory just to call .size() (pg_ducklake #195 warns about OOM on
        // large inlined heaps; a count is negligible by comparison).
        return inlinedInfos.stream()
                .mapToLong { info -> catalog.countInlinedRows(info.tableId, info.schemaVersion, table.snapshotId) }
                .sum()
    }

    /**
     * Map a lance search table function to a [LanceSearchTableHandle] *scan*, so the engine's
     * `RewriteTableFunctionToTableScan` plans an ordinary TableScanNode over the function's
     * output and [applyFilter] / [applyTopN] compose on it (the table-function processor path
     * has no pushdown hooks — HANDOFF O2). The processor wiring stays as a fallback.
     */
    override fun applyTableFunction(
            session: ConnectorSession,
            handle: ConnectorTableFunctionHandle): Optional<TableFunctionApplicationResult<ConnectorTableHandle>>
    {
        if (handle is LanceSearchHandle) {
            return Optional.of(TableFunctionApplicationResult(
                    LanceSearchTableHandle(handle, TupleDomain.all(), null),
                    handle.outputColumns.toList()))
        }
        if (handle is ChangeFeedHandle) {
            return Optional.of(TableFunctionApplicationResult(
                    ChangeFeedTableHandle(
                            handle.feedType,
                            handle.schemaName,
                            handle.tableName,
                            handle.tableId,
                            handle.startSnapshot,
                            handle.endSnapshot,
                            handle.dataColumns),
                    handle.outputColumns()))
        }
        return Optional.empty()
    }

    /**
     * `ORDER BY <natural score column> LIMIT n` over a lance search scan — the documented
     * exact-global-top-k recipe — trims the per-fragment `k` to `n`: each fragment's top-n is
     * still a superset of the global top-n, and `topNGuaranteed=false` keeps the engine's own
     * sort+limit above the scan, so this is purely row-volume reduction.
     */
    override fun applyTopN(
            session: ConnectorSession,
            handle: ConnectorTableHandle,
            topNCount: Long,
            sortItems: List<SortItem>,
            assignments: Map<String, ColumnHandle>): Optional<TopNApplicationResult<ConnectorTableHandle>>
    {
        if (handle !is LanceSearchTableHandle || sortItems.size != 1 || topNCount <= 0) {
            return Optional.empty()
        }
        val sortColumn = assignments[sortItems[0].name] as? DucklakeColumnHandle
            ?: return Optional.empty()
        val search = handle.search()
        // Direction must match the function's natural order; nulls placement is irrelevant
        // (the order columns are never NULL in lance output — hybrid's nullable `_score` is
        // not an order column).
        if (sortColumn.columnName != search.scoreOrderColumn()
                || sortItems[0].sortOrder.isAscending != search.scoreOrderAscending()) {
            return Optional.empty()
        }
        val newLimitK: Long = minOf(handle.limitK ?: search.k, topNCount)
        if (newLimitK == (handle.limitK ?: search.k)) {
            return Optional.empty()
        }
        return Optional.of(TopNApplicationResult(handle.copy(limitK = newLimitK), false, false))
    }

    /**
     * Lance search PTF scan: push the TupleDomain into the WHERE the page source renders over
     * the lance_* call. Everything stays in the remaining filter: with prefilter => false
     * DuckDB applies the WHERE after the search (same rows the engine would keep), and with
     * prefilter => true lance filters BEFORE searching (more rows can come back) — either way
     * the engine's re-application above the scan is correct and cheap.
     */
    private fun applyLanceSearchFilter(
            handle: LanceSearchTableHandle,
            constraint: Constraint): Optional<ConstraintApplicationResult<ConnectorTableHandle>>
    {
        val pushed: TupleDomain<DucklakeColumnHandle> = extractDucklakePredicate(constraint.summary)
        val combined: TupleDomain<DucklakeColumnHandle> = handle.pushedPredicate.intersect(pushed)
        if (combined == handle.pushedPredicate) {
            return Optional.empty()
        }
        return Optional.of(ConstraintApplicationResult(
                handle.copy(pushedPredicate = combined),
                constraint.summary,
                constraint.expression,
                false))
    }

    /** Table handles with no TupleDomain pushdown surface (predicates stay in the engine filter). */
    private fun isFilterExemptHandle(handle: ConnectorTableHandle): Boolean =
            handle is DucklakeMetadataTableHandle || handle is ChangeFeedTableHandle

    override fun applyFilter(
            session: ConnectorSession,
            handle: ConnectorTableHandle,
            constraint: Constraint): Optional<ConstraintApplicationResult<ConnectorTableHandle>>
    {
        if (isFilterExemptHandle(handle)) {
            return Optional.empty()
        }

        if (handle is LanceSearchTableHandle) {
            return applyLanceSearchFilter(handle, constraint)
        }

        val table = handle as DucklakeTableHandle

        val summary: TupleDomain<ColumnHandle> = constraint.summary

        val newEnforced: TupleDomain<DucklakeColumnHandle>
        val newUnenforced: TupleDomain<DucklakeColumnHandle>
        if (summary.isAll) {
            // No TupleDomain pushdown to do — but we still need to check the
            // expression below for function-shape predicates.
            newEnforced = TupleDomain.all()
            newUnenforced = TupleDomain.all()
        }
        else {
            val newPredicate: TupleDomain<DucklakeColumnHandle> = extractDucklakePredicate(summary)

            // Classify predicates as enforced (partition-prunable) or unenforced (best-effort)
            val partitionSpecs: List<DucklakePartitionSpec> = catalog.getPartitionSpecs(
                    table.tableId, table.snapshotId)

            val enforced: ImmutableMap.Builder<DucklakeColumnHandle, Domain> = ImmutableMap.builder()
            val unenforced: ImmutableMap.Builder<DucklakeColumnHandle, Domain> = ImmutableMap.builder()

            if (!newPredicate.isNone) {
                for (entry in newPredicate.getDomains().orElse(emptyMap()).entries) {
                    when (classifyColumnConstraint(partitionSpecs, entry.key)) {
                        ConstraintEnforcement.FULLY_ENFORCED -> enforced.put(entry.key, entry.value)
                        ConstraintEnforcement.PARTIALLY_ENFORCED -> {
                            // Keep in both predicates: connector can use partition transforms for pruning,
                            // but engine must still evaluate original predicate for correctness.
                            enforced.put(entry.key, entry.value)
                            unenforced.put(entry.key, entry.value)
                        }
                        ConstraintEnforcement.NOT_ENFORCED -> unenforced.put(entry.key, entry.value)
                    }
                }
            }

            newEnforced = if (newPredicate.isNone)
                TupleDomain.none()
            else
                toTupleDomain(enforced.buildOrThrow())
            newUnenforced = if (newPredicate.isNone)
                TupleDomain.all()
            else
                toTupleDomain(unenforced.buildOrThrow())
        }

        val combinedEnforced: TupleDomain<DucklakeColumnHandle> = table.enforcedPredicate.intersect(newEnforced)
        val combinedUnenforced: TupleDomain<DucklakeColumnHandle> = table.unenforcedPredicate.intersect(newUnenforced)

        // Function-shape pushdown: anything DuckDbExpressionTranslator recognises in
        // constraint.getExpression() becomes a SQL fragment AND-ed into the WHERE clause
        // the .db reader sends to DuckDB. We also keep the same expression in
        // remainingExpression so Trino re-evaluates it above the scan — see
        // dev-docs/TODO-pushdown-duckdb.md, "Regime 1 — common-SQL functions". Double
        // evaluation on .db splits is cheap, parquet splits keep working unchanged.
        val newExpressionClauses: List<String> = DuckDbExpressionTranslator.translateConjuncts(
                constraint.expression, constraint.assignments, session)
        val combinedExpressions: List<String> = mergePushedExpressions(
                table.pushedExpressions, newExpressionClauses)

        if (combinedEnforced == table.enforcedPredicate
                && combinedUnenforced == table.unenforcedPredicate
                && combinedExpressions == table.pushedExpressions) {
            return Optional.empty()
        }

        val newHandle = DucklakeTableHandle(
                table.schemaName,
                table.tableName,
                table.tableId,
                table.snapshotId,
                combinedUnenforced,
                combinedEnforced,
                combinedExpressions)

        // Fully enforced predicates are omitted from remaining filter.
        // Partially enforced predicates (e.g. temporal transforms) remain so engine verifies exact semantics.
        val remainingFilter: TupleDomain<ColumnHandle> = newUnenforced.transformKeys(ColumnHandle::class.java::cast)

        return Optional.of(ConstraintApplicationResult(
                newHandle,
                remainingFilter,
                constraint.expression,
                false))
    }

    private enum class ConstraintEnforcement
    {
        FULLY_ENFORCED,
        PARTIALLY_ENFORCED,
        NOT_ENFORCED
    }

    override fun listTableColumns(
            session: ConnectorSession,
            prefix: SchemaTablePrefix): Map<SchemaTableName, List<ColumnMetadata>>
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        val columns: ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> = ImmutableMap.builder()

        val tables: List<SchemaTableName> = prefix.table
                .map { listOf(prefix.toSchemaTableName()) }
                .orElseGet { listTables(session, prefix.schema) }

        for (tableName in tables) {
            val metadataTable: MetadataTableName? = parseMetadataTableName(tableName)
            if (metadataTable != null) {
                val baseTable = SchemaTableName(tableName.schemaName, metadataTable.baseTableName)
                if (catalog.getTable(baseTable.schemaName, baseTable.tableName, snapshotId) != null) {
                    columns.put(tableName, getMetadataColumns(metadataTable.metadataTableType))
                }
                continue
            }

            val table: DucklakeTable? = catalog.getTable(tableName.schemaName, tableName.tableName, snapshotId)
            if (table != null) {
                val tableColumns: List<DucklakeColumn> = catalog.getTableColumns(table.tableId, snapshotId)
                columns.put(
                        tableName,
                        tableColumns.stream()
                                .map { column ->
                                    ColumnMetadata.builder()
                                            .setName(column.columnName)
                                            .setType(typeConverter.toTrinoType(column.columnType))
                                            .setNullable(column.nullsAllowed)
                                            .build()
                                }
                                .collect(toImmutableList()))
            }
        }

        return columns.buildOrThrow()
    }

    private data class MetadataTableName(val baseTableName: String, val metadataTableType: DucklakeMetadataTableType)

    // ==================== Schema DDL ====================

    override fun createSchema(session: ConnectorSession, schemaName: String, properties: Map<String, Any?>, owner: TrinoPrincipal)
    {
        translateCatalogExceptions { catalog.createSchema(schemaName) }
    }

    override fun dropSchema(session: ConnectorSession, schemaName: String, cascade: Boolean)
    {
        translateCatalogExceptions { catalog.dropSchema(schemaName) }
    }

    // ==================== Table DDL ====================

    override fun createTable(session: ConnectorSession, tableMetadata: ConnectorTableMetadata, saveMode: SaveMode)
    {
        if (saveMode == SaveMode.REPLACE) {
            throw TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables")
        }

        val tableName: SchemaTableName = tableMetadata.table

        // Convert columns to DuckLake column specs
        val columnSpecs: List<TableColumnSpec> = tableMetadata.columns.stream()
                .map { column -> toColumnSpec(column.name, column.type, column.isNullable) }
                .collect(toImmutableList())

        // Parse partition spec from table properties
        val partitionFields: List<PartitionFieldSpec> = DucklakeTableProperties.getPartitionFields(tableMetadata.properties)
        val partitionSpec: List<PartitionFieldSpec>? = partitionFields.ifEmpty { null }

        val location: TableLocationSpec? = DucklakeTableProperties.getLocation(tableMetadata.properties).orElse(null)

        translateCatalogExceptions {
            catalog.createTable(
                tableName.schemaName,
                tableName.tableName,
                columnSpecs,
                partitionSpec,
                location,
                DucklakeTableProperties.getDataFileFormat(tableMetadata.properties)
            )
        }
    }

    private fun toColumnSpec(name: String, trinoType: Type, nullable: Boolean): TableColumnSpec
    {
        val ducklakeType: String = typeConverter.toDucklakeType(trinoType)

        if (trinoType is ArrayType) {
            val children: List<TableColumnSpec> = ImmutableList.of(
                    toColumnSpec("element", trinoType.elementType, true))
            return TableColumnSpec(name, ducklakeType, nullable, children)
        }
        if (trinoType is RowType) {
            val children: List<TableColumnSpec> = trinoType.fields.stream()
                    .map { field -> toColumnSpec(
                            field.name.orElseThrow { TrinoException(NOT_SUPPORTED, "Anonymous row fields not supported") },
                            field.type,
                            true) }
                    .collect(toImmutableList())
            return TableColumnSpec(name, ducklakeType, nullable, children)
        }
        if (trinoType is MapType) {
            val children: List<TableColumnSpec> = ImmutableList.of(
                    toColumnSpec("key", trinoType.keyType, false),
                    toColumnSpec("value", trinoType.valueType, true))
            return TableColumnSpec(name, ducklakeType, nullable, children)
        }

        return TableColumnSpec.leaf(name, ducklakeType, nullable)
    }

    override fun dropTable(session: ConnectorSession, tableHandle: ConnectorTableHandle)
    {
        val handle = tableHandle as DucklakeTableHandle
        translateCatalogExceptions { catalog.dropTable(handle.schemaName, handle.tableName) }
    }

    override fun truncateTable(session: ConnectorSession, tableHandle: ConnectorTableHandle)
    {
        val handle = tableHandle as DucklakeTableHandle
        translateCatalogExceptions { catalog.truncateTable(handle.schemaName, handle.tableName) }
    }

    override fun renameTable(session: ConnectorSession, tableHandle: ConnectorTableHandle, newTableName: SchemaTableName)
    {
        val handle = tableHandle as DucklakeTableHandle
        if (!newTableName.schemaName.equals(handle.schemaName, ignoreCase = true)) {
            // Table data paths are schema-relative in DuckLake, so a cross-schema move would
            // leave the table's data files unreachable. Upstream has no cross-schema rename.
            throw TrinoException(NOT_SUPPORTED,
                    "Renaming a table across schemas is not supported: table data paths are schema-relative")
        }
        translateCatalogExceptions {
            catalog.renameTable(handle.tableId, newTableName.schemaName, newTableName.tableName)
        }
    }

    override fun renameSchema(session: ConnectorSession, source: String, target: String)
    {
        translateCatalogExceptions { catalog.renameSchema(source, target) }
    }

    override fun setTableComment(session: ConnectorSession, tableHandle: ConnectorTableHandle, comment: Optional<String>)
    {
        val handle = tableHandle as DucklakeTableHandle
        translateCatalogExceptions { catalog.setTableComment(handle.tableId, comment.orElse(null)) }
    }

    override fun setColumnComment(session: ConnectorSession, tableHandle: ConnectorTableHandle, column: ColumnHandle, comment: Optional<String>)
    {
        val handle = tableHandle as DucklakeTableHandle
        val columnHandle = column as DucklakeColumnHandle
        if (columnHandle.isVirtual() || columnHandle.isRowIdColumn()) {
            throw TrinoException(NOT_SUPPORTED, "Cannot comment on virtual column: ${columnHandle.columnName}")
        }
        translateCatalogExceptions {
            catalog.setColumnComment(handle.tableId, columnHandle.columnId, comment.orElse(null))
        }
    }

    // ==================== ALTER TABLE ====================

    override fun addColumn(session: ConnectorSession, tableHandle: ConnectorTableHandle, column: ColumnMetadata, position: ColumnPosition)
    {
        val handle = tableHandle as DucklakeTableHandle
        val columnSpec: TableColumnSpec = toColumnSpec(column.name, column.type, column.isNullable)
        translateCatalogExceptions { catalog.addColumn(handle.tableId, columnSpec) }
    }

    override fun dropColumn(session: ConnectorSession, tableHandle: ConnectorTableHandle, column: ColumnHandle)
    {
        val handle = tableHandle as DucklakeTableHandle
        val ducklakeColumn = column as DucklakeColumnHandle
        translateCatalogExceptions { catalog.dropColumn(handle.tableId, ducklakeColumn.columnId) }
    }

    override fun renameColumn(session: ConnectorSession, tableHandle: ConnectorTableHandle, source: ColumnHandle, target: String)
    {
        val handle = tableHandle as DucklakeTableHandle
        val ducklakeColumn = source as DucklakeColumnHandle
        translateCatalogExceptions { catalog.renameColumn(handle.tableId, ducklakeColumn.columnId, target) }
    }

    // Nested struct field DDL. `addField`'s parentPath includes the top-level column name (there is no
    // separate ColumnHandle); `dropField` supplies the column separately, so we prepend its name.
    // Reads of files written before a nested change are reconciled per file: parquet self-heals via
    // field name/id; non-parquet (DuckDB-engine) reshapes the struct with struct_pack (see
    // NestedFieldReshapePlanner / DuckDbSelectSqlBuilder).
    override fun addField(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle,
            parentPath: List<String>,
            fieldName: String,
            type: Type,
            ignoreExisting: Boolean)
    {
        val handle = tableHandle as DucklakeTableHandle
        val fieldSpec: TableColumnSpec = toColumnSpec(fieldName, type, true)
        translateCatalogExceptions { catalog.addField(handle.tableId, parentPath, fieldSpec, ignoreExisting) }
    }

    override fun dropField(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle,
            column: ColumnHandle,
            fieldPath: List<String>)
    {
        val handle = tableHandle as DucklakeTableHandle
        val ducklakeColumn = column as DucklakeColumnHandle
        val fullPath: List<String> = listOf(ducklakeColumn.columnName) + fieldPath
        translateCatalogExceptions { catalog.dropField(handle.tableId, fullPath) }
    }

    // ==================== INSERT ====================

    override fun beginInsert(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle,
            columns: List<ColumnHandle>,
            retryMode: RetryMode): ConnectorInsertTableHandle
    {
        val handle = tableHandle as DucklakeTableHandle

        rejectVirtualColumnWrites(columns)

        val ducklakeColumns: List<DucklakeColumnHandle> = columns.stream()
                .map(DucklakeColumnHandle::class.java::cast)
                .collect(toImmutableList())

        val allCatalogColumns: List<DucklakeColumn> = catalog.getAllColumnsWithParentage(handle.tableId, handle.snapshotId)

        val partitionSpecs: List<DucklakePartitionSpec> = catalog.getPartitionSpecs(handle.tableId, handle.snapshotId)
        val activePartitionSpec: Optional<DucklakePartitionSpec> = activePartitionSpecOf(partitionSpecs)

        val tableDataPath: String = resolveTableDataPath(handle.schemaName, handle.tableName, handle.snapshotId)

        return DucklakeWritableTableHandle(
                handle.schemaName,
                handle.tableName,
                handle.tableId,
                ducklakeColumns,
                allCatalogColumns,
                tableDataPath,
                activePartitionSpec,
                temporalPartitionEncoding,
                resolveWriteFormat(session, handle.tableId, handle.snapshotId),
                DucklakeSessionProperties.getDuckDbWriterMode(session))
    }

    override fun finishInsert(
            session: ConnectorSession,
            insertHandle: ConnectorInsertTableHandle,
            sourceTableHandles: List<ConnectorTableHandle>,
            fragments: Collection<Slice>,
            computedStatistics: Collection<ComputedStatistics>): Optional<ConnectorOutputMetadata>
    {
        val handle = insertHandle as DucklakeWritableTableHandle
        val writeFragments: List<DucklakeWriteFragment> = deserializeFragments(fragments)

        if (!writeFragments.isEmpty()) {
            translateCatalogExceptions { catalog.commitInsert(handle.tableId, writeFragments) }
        }

        return Optional.empty()
    }

    // ==================== CTAS ====================

    override fun beginCreateTable(
            session: ConnectorSession,
            tableMetadata: ConnectorTableMetadata,
            layout: Optional<ConnectorTableLayout>,
            retryMode: RetryMode,
            replace: Boolean): ConnectorOutputTableHandle
    {
        if (replace) {
            throw TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables")
        }

        val tableName: SchemaTableName = tableMetadata.table

        // Create the table structure (DDL snapshot)
        val columnSpecs: List<TableColumnSpec> = tableMetadata.columns.stream()
                .map { column -> toColumnSpec(column.name, column.type, column.isNullable) }
                .collect(toImmutableList())

        val partitionFields: List<PartitionFieldSpec> = DucklakeTableProperties.getPartitionFields(tableMetadata.properties)
        val partitionSpec: List<PartitionFieldSpec>? = partitionFields.ifEmpty { null }

        val location: TableLocationSpec? = DucklakeTableProperties.getLocation(tableMetadata.properties).orElse(null)

        // An explicit WITH (data_file_format = ...) is a declaration on the table itself, so
        // it persists as the table-scoped setting (consulted by resolveWriteFormat for every
        // later INSERT). A session-derived CTAS format is per-session intent and is NOT
        // persisted — those tables keep latest-data-file inheritance (testFlipStaysFlipped).
        val declaredFormat: String? = DucklakeTableProperties.getDataFileFormat(tableMetadata.properties)

        translateCatalogExceptions {
            catalog.createTable(
                tableName.schemaName,
                tableName.tableName,
                columnSpecs,
                partitionSpec,
                location,
                declaredFormat
            )
        }

        // Resolve the newly created table to get its ID and build a writable handle
        val snapshotId: Long = catalog.currentSnapshotId
        val table: DucklakeTable = catalog.getTable(tableName.schemaName, tableName.tableName, snapshotId)
            ?: throw TrinoException(NOT_SUPPORTED, "Table was not created: $tableName")

        // For the WRITE handle, a column's type must match the blocks the engine will stream
        // from the CTAS source — not the catalog round-trip. The two agree for every type
        // EXCEPT timestamptz: DuckLake stores it micros-only, so the round-trip always reports
        // precision 6 (a long / Fixed12 tstz), but a CTAS streams the source's actual precision
        // (e.g. the default precision-3 short / LongArray tstz). Feeding the writer the
        // precision-6 type then crashed the stats accumulator / Arrow conversion on the short
        // blocks (LongArrayBlock vs Fixed12Block). Use the originally-declared type for tstz so
        // the writer's isShort branch matches the block; the stored value still widens to micros
        // and reads back as precision 6.
        val declaredTstzByName: Map<String, Type> = tableMetadata.columns
                .filter { it.type is TimestampWithTimeZoneType }
                .associate { it.name to it.type }
        val catalogColumns: List<DucklakeColumn> = catalog.getTableColumns(table.tableId, snapshotId)
        val columnHandles: List<DucklakeColumnHandle> = catalogColumns.stream()
                .filter { col -> col.parentColumn == null }
                .map { col -> DucklakeColumnHandle(
                        col.columnId,
                        col.columnName,
                        declaredTstzByName[col.columnName] ?: typeConverter.toTrinoType(col.columnType),
                        col.nullsAllowed) }
                .collect(toImmutableList())

        val allCatalogColumns: List<DucklakeColumn> = catalog.getAllColumnsWithParentage(table.tableId, snapshotId)

        val partitionSpecs: List<DucklakePartitionSpec> = catalog.getPartitionSpecs(table.tableId, snapshotId)
        val activePartitionSpec: Optional<DucklakePartitionSpec> = activePartitionSpecOf(partitionSpecs)

        val tableDataPath: String = resolveTableDataPath(tableName.schemaName, tableName.tableName, snapshotId)

        // CTAS precedence: WITH clause > session property > connector default. The
        // "match latest existing data file" rule that drives INSERT inheritance can't
        // apply here because the table is fresh — there are no prior data files.
        // (See N1 in TODO-duckdb-lake-format.md.)
        val fileFormat: String = declaredFormat
                ?: DucklakeSessionProperties.getDataFileFormat(session)
                        .orElse(DucklakeSessionProperties.FORMAT_PARQUET)

        return DucklakeWritableTableHandle(
                tableName.schemaName,
                tableName.tableName,
                table.tableId,
                columnHandles,
                allCatalogColumns,
                tableDataPath,
                activePartitionSpec,
                temporalPartitionEncoding,
                fileFormat,
                DucklakeSessionProperties.getDuckDbWriterMode(session))
    }

    override fun finishCreateTable(
            session: ConnectorSession,
            tableHandle: ConnectorOutputTableHandle,
            fragments: Collection<Slice>,
            computedStatistics: Collection<ComputedStatistics>): Optional<ConnectorOutputMetadata>
    {
        val handle = tableHandle as DucklakeWritableTableHandle
        val writeFragments: List<DucklakeWriteFragment> = deserializeFragments(fragments)

        if (!writeFragments.isEmpty()) {
            translateCatalogExceptions { catalog.commitInsert(handle.tableId, writeFragments) }
        }

        return Optional.empty()
    }

    private fun deserializeFragments(fragments: Collection<Slice>): List<DucklakeWriteFragment>
    {
        return fragments.stream()
                .map { fragment -> fragmentCodec!!.fromJson(fragment.bytes) }
                .collect(toImmutableList())
    }

    /**
     * Resolve the data file format for an INSERT (or the insert leg of a MERGE/UPDATE).
     * Precedence (N1):
     *
     *   - Session property `ducklake.data_file_format`, when explicitly set.
     *   - The table's DECLARED format: the table-scoped `data_file_format` setting in
     *     `ducklake_metadata`, persisted when the table was created with an explicit
     *     `WITH (data_file_format = ...)`. This is what makes INSERT into a still-empty
     *     declared-format table write that format instead of the connector default.
     *   - Format of the most recent active data file already in the table — the rule that
     *     keeps the natural session-CTAS-then-INSERT workflow on a consistent format, and
     *     gives UNdeclared tables flip-stays-flipped semantics (testFlipStaysFlipped).
     *   - Connector default (`parquet`).
     */
    private fun resolveWriteFormat(session: ConnectorSession, tableId: Long, snapshotId: Long): String
    {
        val sessionFormat: Optional<String> = DucklakeSessionProperties.getDataFileFormat(session)
        if (sessionFormat.isPresent) {
            return sessionFormat.get()
        }
        val declaredFormat: String? = catalog.getTableDataFileFormat(tableId)
        if (declaredFormat != null) {
            return declaredFormat
        }
        val latestFormat: String? = catalog.getLatestDataFileFormat(tableId, snapshotId)
        if (latestFormat != null) {
            return latestFormat
        }
        return DucklakeSessionProperties.FORMAT_PARQUET
    }

    private fun resolveTableDataPath(schemaName: String, tableName: String, snapshotId: Long): String
    {
        val schema: DucklakeSchema? = catalog.getSchema(schemaName, snapshotId)
        val table: DucklakeTable? = catalog.getTable(schemaName, tableName, snapshotId)

        if (schema == null || table == null) {
            throw TrinoException(NOT_SUPPORTED, "Cannot resolve data path for $schemaName.$tableName")
        }

        return pathResolver!!.resolveTableDataPath(schema, table)
    }

    // ==================== DELETE / MERGE ====================

    override fun getRowChangeParadigm(session: ConnectorSession, tableHandle: ConnectorTableHandle): RowChangeParadigm =
            RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW

    override fun getMergeRowIdColumnHandle(session: ConnectorSession, tableHandle: ConnectorTableHandle): ColumnHandle =
            DucklakeColumnHandle.rowIdColumnHandle()

    override fun beginMerge(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle,
            updateCaseColumns: Map<Int, Collection<ColumnHandle>>,
            retryMode: RetryMode): ConnectorMergeTableHandle
    {
        val handle = tableHandle as DucklakeTableHandle

        updateCaseColumns.values.forEach { rejectVirtualColumnWrites(it) }

        // Gate DELETE/UPDATE/MERGE on tables that still hold live inlined rows. The merge scan
        // requests the $row_id of every scanned row, but inlined rows have no data file / file
        // position, and this connector's merge sink only writes parquet positional delete files
        // keyed by (data_file_id, position) — it has no way to tombstone an inlined row. Rather
        // than fail mid-scan with an opaque "Column not found: $row_id" from the inlined page
        // source, reject early with guidance. Flushing the inlined data to files (DuckLake's
        // flush_inlined_data, or data_inlining_row_limit = 0 then a rewrite) makes the rows
        // file-resident and DELETE/UPDATE/MERGE work normally.
        if (catalog.getInlinedDataInfos(handle.tableId, handle.snapshotId).any { it.hasLiveRows }) {
            throw TrinoException(NOT_SUPPORTED,
                    "DELETE/UPDATE/MERGE is not supported while ${handle.schemaName}.${handle.tableName} has " +
                            "inlined rows (rows written below DuckLake's data_inlining_row_limit). Flush the " +
                            "inlined data to files first (e.g. DuckLake's flush_inlined_data procedure, or set " +
                            "data_inlining_row_limit = 0).")
        }

        // Build insert handle for UPDATE support (delete+insert pattern)
        val ducklakeColumns: List<DucklakeColumnHandle> = catalog.getTableColumns(handle.tableId, handle.snapshotId).stream()
                .filter { col -> col.parentColumn == null }
                .map { col -> DucklakeColumnHandle(
                        col.columnId,
                        col.columnName,
                        typeConverter.toTrinoType(col.columnType),
                        col.nullsAllowed) }
                .collect(toImmutableList())

        val allCatalogColumns: List<DucklakeColumn> = catalog.getAllColumnsWithParentage(handle.tableId, handle.snapshotId)

        val partitionSpecs: List<DucklakePartitionSpec> = catalog.getPartitionSpecs(handle.tableId, handle.snapshotId)
        val activePartitionSpec: Optional<DucklakePartitionSpec> = activePartitionSpecOf(partitionSpecs)

        val tableDataPath: String = resolveTableDataPath(handle.schemaName, handle.tableName, handle.snapshotId)

        val insertHandle = DucklakeWritableTableHandle(
                handle.schemaName,
                handle.tableName,
                handle.tableId,
                ducklakeColumns,
                allCatalogColumns,
                tableDataPath,
                activePartitionSpec,
                temporalPartitionEncoding,
                resolveWriteFormat(session, handle.tableId, handle.snapshotId),
                DucklakeSessionProperties.getDuckDbWriterMode(session))

        // Build data file ranges for row ID → data file resolution. getDataFiles LEFT-JOINs
        // ducklake_delete_file, returning one row per (data_file, active delete_file) pair —
        // group by data_file_id to deduplicate and collect resolved delete-file paths. The
        // merge sink uses these paths to read prior-active positions and union them with
        // this commit's new deletes (B3a: writeDeleteFile must preserve prior deletions when
        // it produces a superseding file, otherwise positions resurrect).
        val dataFiles: List<DucklakeDataFile> = catalog.getDataFiles(handle.tableId, handle.snapshotId)
        val primaryByFileId: LinkedHashMap<Long, DucklakeDataFile> = linkedMapOf()
        val deletePathsByFileId: LinkedHashMap<Long, MutableList<String>> = linkedMapOf()
        for (df in dataFiles) {
            primaryByFileId.putIfAbsent(df.dataFileId, df)
            if (df.deleteFilePath != null) {
                val resolved: String = pathResolver!!.resolveFilePath(
                        df.deleteFilePath!!,
                        df.deleteFilePathIsRelative ?: false,
                        tableDataPath)
                deletePathsByFileId
                        .computeIfAbsent(df.dataFileId) { _ -> mutableListOf() }
                        .add(resolved)
            }
        }
        val dataFileRanges: List<DucklakeMergeTableHandle.DataFileRange> = primaryByFileId.values.stream()
                .map { df -> DucklakeMergeTableHandle.DataFileRange(
                        df.dataFileId,
                        df.rowIdStart,
                        df.recordCount,
                        deletePathsByFileId.getOrDefault(df.dataFileId, emptyList()),
                        pathResolver!!.resolveFilePath(df.path, df.pathIsRelative, tableDataPath)) }
                .collect(toImmutableList())

        return DucklakeMergeTableHandle(handle, insertHandle, dataFileRanges)
    }

    override fun finishMerge(
            session: ConnectorSession,
            mergeTableHandle: ConnectorMergeTableHandle,
            sourceTableHandles: List<ConnectorTableHandle>,
            fragments: Collection<Slice>,
            computedStatistics: Collection<ComputedStatistics>)
    {
        val mergeHandle = mergeTableHandle as DucklakeMergeTableHandle
        val tableHandle: DucklakeTableHandle = mergeHandle.tableHandle

        val deleteFragments: MutableList<DucklakeDeleteFragment> = mutableListOf()
        val insertFragments: MutableList<DucklakeWriteFragment> = mutableListOf()

        for (fragment in fragments) {
            val bytes: ByteArray = fragment.bytes
            // Try to parse as delete fragment first, fall back to write fragment
            try {
                val deleteFragment: DucklakeDeleteFragment = deleteFragmentCodec!!.fromJson(bytes)
                // Verify it's actually a delete fragment (has dataFileId > 0 and path starts with "ducklake-delete-")
                if (deleteFragment.path.startsWith("ducklake-delete-")) {
                    deleteFragments.add(deleteFragment)
                    continue
                }
            }
            catch (_: RuntimeException) {
                // Not a delete fragment
            }
            try {
                insertFragments.add(fragmentCodec!!.fromJson(bytes))
            }
            catch (e: RuntimeException) {
                throw RuntimeException("Failed to deserialize merge fragment", e)
            }
        }

        // Commit atomically in a single snapshot — critical for UPDATE (delete+insert must be atomic)
        if (!deleteFragments.isEmpty() && !insertFragments.isEmpty()) {
            translateCatalogExceptions { catalog.commitMerge(tableHandle.tableId, deleteFragments, insertFragments) }
        }
        else if (!deleteFragments.isEmpty()) {
            translateCatalogExceptions { catalog.commitDelete(tableHandle.tableId, deleteFragments) }
        }
        else if (!insertFragments.isEmpty()) {
            translateCatalogExceptions { catalog.commitInsert(tableHandle.tableId, insertFragments) }
        }
    }

    // ==================== View operations ====================

    override fun listViews(session: ConnectorSession, schemaName: Optional<String>): List<SchemaTableName>
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)

        if (schemaName.isPresent) {
            val schema: DucklakeSchema = catalog.getSchema(schemaName.get(), snapshotId)
                ?: return ImmutableList.of()
            return catalog.listViews(schema.schemaId, snapshotId).stream()
                    .filter { view -> isViewAccessible(view) }
                    .map { view -> SchemaTableName(schemaName.get(), view.viewName) }
                    .collect(toImmutableList())
        }

        val views: ImmutableList.Builder<SchemaTableName> = ImmutableList.builder()
        for (schema in catalog.listSchemas(snapshotId)) {
            for (view in catalog.listViews(schema.schemaId, snapshotId)) {
                if (isViewAccessible(view)) {
                    views.add(SchemaTableName(schema.schemaName, view.viewName))
                }
            }
        }
        return views.build()
    }

    override fun getView(session: ConnectorSession, viewName: SchemaTableName): Optional<ConnectorViewDefinition>
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)

        val view: DucklakeView = catalog.getView(viewName.schemaName, viewName.tableName, snapshotId)
            ?: return Optional.empty()
        if (!isViewAccessible(view)) {
            return Optional.empty()
        }

        return decodeTrinoView(view, viewName)
    }

    override fun getViews(session: ConnectorSession, schemaName: Optional<String>): Map<SchemaTableName, ConnectorViewDefinition>
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        val views: ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> = ImmutableMap.builder()

        val schemas: List<DucklakeSchema>
        if (schemaName.isPresent) {
            val schema: DucklakeSchema = catalog.getSchema(schemaName.get(), snapshotId)
                ?: return ImmutableMap.of()
            schemas = ImmutableList.of(schema)
        }
        else {
            schemas = catalog.listSchemas(snapshotId)
        }

        for (schema in schemas) {
            for (view in catalog.listViews(schema.schemaId, snapshotId)) {
                if (isViewAccessible(view)) {
                    val name = SchemaTableName(schema.schemaName, view.viewName)
                    decodeTrinoView(view, name).ifPresent { def -> views.put(name, def) }
                }
            }
        }

        return views.buildOrThrow()
    }

    override fun createView(session: ConnectorSession, viewName: SchemaTableName, definition: ConnectorViewDefinition, viewProperties: Map<String, Any?>, replace: Boolean)
    {
        if (replace) {
            val snapshotId = snapshotResolver.resolveSnapshotId(session)
            val existing: DucklakeView? = catalog.getView(viewName.schemaName, viewName.tableName, snapshotId)
            if (existing != null) {
                translateCatalogExceptions { catalog.dropView(viewName.schemaName, viewName.tableName) }
            }
        }

        // Store the original SQL in the sql field (spec-compliant).
        // Store the full ConnectorViewDefinition as JSON in column_aliases
        // so we can round-trip column names and types.
        val originalSql: String = definition.originalSql
        val viewMetadataJson: String = VIEW_CODEC.toJson(definition)

        translateCatalogExceptions {
            catalog.createView(
                viewName.schemaName,
                viewName.tableName,
                originalSql,
                TRINO_VIEW_DIALECT,
                viewMetadataJson
            )
        }
    }

    override fun renameView(session: ConnectorSession, source: SchemaTableName, target: SchemaTableName)
    {
        if (source == target) {
            return
        }

        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        val sourceView: DucklakeView? = catalog.getView(source.schemaName, source.tableName, snapshotId)
        if (sourceView == null || !isViewAccessible(sourceView)) {
            throw ViewNotFoundException(source)
        }

        if (catalog.getTable(target.schemaName, target.tableName, snapshotId) != null || catalog.getView(target.schemaName, target.tableName, snapshotId) != null) {
            throw TrinoException(ALREADY_EXISTS, "Relation already exists: $target")
        }

        translateCatalogExceptions {
            catalog.renameView(
                source.schemaName,
                source.tableName,
                target.schemaName,
                target.tableName
            )
        }
    }

    override fun setViewComment(session: ConnectorSession, viewName: SchemaTableName, comment: Optional<String>)
    {
        val definition: ConnectorViewDefinition = getRequiredViewDefinition(session, viewName)
        val updated = ConnectorViewDefinition(
                definition.originalSql,
                definition.catalog,
                definition.schema,
                definition.columns,
                comment,
                definition.owner,
                definition.isRunAsInvoker,
                definition.path
        )

        translateCatalogExceptions {
            catalog.replaceViewMetadata(
                viewName.schemaName,
                viewName.tableName,
                updated.originalSql,
                TRINO_VIEW_DIALECT,
                VIEW_CODEC.toJson(updated)
            )
        }
    }

    override fun setViewColumnComment(session: ConnectorSession, viewName: SchemaTableName, columnName: String, comment: Optional<String>)
    {
        val definition: ConnectorViewDefinition = getRequiredViewDefinition(session, viewName)

        var updated = false
        val columnsBuilder: ImmutableList.Builder<ConnectorViewDefinition.ViewColumn> = ImmutableList.builderWithExpectedSize(definition.columns.size)
        for (column in definition.columns) {
            if (column.name == columnName) {
                columnsBuilder.add(ConnectorViewDefinition.ViewColumn(column.name, column.type, comment))
                updated = true
            }
            else {
                columnsBuilder.add(column)
            }
        }
        if (!updated) {
            throw ColumnNotFoundException(viewName, columnName)
        }
        val columns: List<ConnectorViewDefinition.ViewColumn> = columnsBuilder.build()

        val updatedDefinition = ConnectorViewDefinition(
                definition.originalSql,
                definition.catalog,
                definition.schema,
                columns,
                definition.comment,
                definition.owner,
                definition.isRunAsInvoker,
                definition.path
        )

        translateCatalogExceptions {
            catalog.replaceViewMetadata(
                viewName.schemaName,
                viewName.tableName,
                updatedDefinition.originalSql,
                TRINO_VIEW_DIALECT,
                VIEW_CODEC.toJson(updatedDefinition)
            )
        }
    }

    override fun dropView(session: ConnectorSession, viewName: SchemaTableName)
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        val existing: DucklakeView? = catalog.getView(viewName.schemaName, viewName.tableName, snapshotId)
        if (existing == null) {
            throw ViewNotFoundException(viewName)
        }

        translateCatalogExceptions { catalog.dropView(viewName.schemaName, viewName.tableName) }
    }

    private fun getRequiredViewDefinition(session: ConnectorSession, viewName: SchemaTableName): ConnectorViewDefinition
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        val view: DucklakeView = catalog.getView(viewName.schemaName, viewName.tableName, snapshotId)
            ?: throw ViewNotFoundException(viewName)
        if (!isViewAccessible(view)) {
            throw ViewNotFoundException(viewName)
        }

        return decodeTrinoView(view, viewName)
                .orElseThrow { ViewNotFoundException(viewName, "View metadata is unavailable") }
    }

    /**
     * Only Trino-dialect views are currently accessible.
     * Non-Trino dialects (e.g., duckdb) require a SQL transpiler to safely expose.
     */
    private fun isViewAccessible(view: DucklakeView): Boolean
    {
        val dialect: String = view.dialect.lowercase(Locale.ROOT)
        if (TRINO_VIEW_DIALECT == dialect) {
            return true
        }
        // Future: check viewSqlDialects set + transpiler availability
        log.debug("Skipping view %s with non-Trino dialect: %s (transpiler not configured)", view.viewName, dialect)
        return false
    }

    /**
     * Decode a Trino-dialect view from the catalog.
     * Column metadata is stored as JSON in the column_aliases field.
     * Falls back to original SQL if metadata is missing or corrupt.
     */
    private fun decodeTrinoView(view: DucklakeView, viewName: SchemaTableName): Optional<ConnectorViewDefinition>
    {
        // Trino views store the full ConnectorViewDefinition as JSON in column_aliases
        val viewMetadata = view.viewMetadata
        if (viewMetadata != null && !viewMetadata.isBlank()) {
            try {
                return Optional.of(VIEW_CODEC.fromJson(viewMetadata))
            }
            catch (e: RuntimeException) {
                log.warn(e, "Failed to decode Trino view metadata for %s", viewName)
            }
        }

        log.warn("View %s has no Trino metadata in column_aliases, cannot resolve column types", viewName)
        return Optional.empty()
    }

    companion object {
        private val log: Logger = Logger.get(DucklakeMetadata::class.java)
        private const val METADATA_TABLE_SEPARATOR: String = "$"

        // The virtual (hidden) columns exposed via getColumnHandles / getTableMetadata.
        // Constant-per-split ($path, $snapshot_id, $file_size_bytes) plus the row-varying
        // lineage pair ($file_row_number, $row_id), all injected by DucklakePageSourceProvider.
        private val EXPOSED_VIRTUAL_COLUMNS: List<VirtualKind> = VirtualKind.values().toList()

        /**
         * Reject any virtual column handle in a write column list with NOT_SUPPORTED.
         * Routed through DucklakeColumnHandle.isVirtual() (the single sentinel check) so
         * the guard can never drift from VirtualKind. The MERGE row-id handle (-100) is
         * NOT virtual, so this never rejects it. `internal` (not private) so TestDucklakeMetadata
         * can pin the write-path guard directly (DESIGN-virtual-columns.md § 5).
         */
        internal fun rejectVirtualColumnWrites(columns: Iterable<ColumnHandle>)
        {
            for (column in columns) {
                if (column is DucklakeColumnHandle && column.isVirtual()) {
                    throw TrinoException(NOT_SUPPORTED, "Virtual column cannot be written: ${column.columnName}")
                }
            }
        }
        private val VIEW_CODEC: JsonCodec<ConnectorViewDefinition> = JsonCodecFactory().jsonCodec(ConnectorViewDefinition::class.java)
        // `trino/brikk` — base dialect is Trino SQL, `/brikk` marks that this row also carries
        // our plugin-specific JSON sidecar in `ducklake_view.column_aliases` (serialized
        // `ConnectorViewDefinition`). Other writers that understand plain Trino SQL should skip
        // rows with this suffix unless they want to cross-parse our metadata.
        private const val TRINO_VIEW_DIALECT: String = "trino/brikk"

        private fun getSnapshotIdFromVersion(version: ConnectorTableVersion): Long
        {
            val versionType: Type = version.versionType
            if (versionType === SMALLINT || versionType === TINYINT || versionType === INTEGER || versionType === BIGINT) {
                return (version.version as Number).toLong()
            }

            throw TrinoException(NOT_SUPPORTED, "Unsupported type for table version: ${versionType.displayName}")
        }

        // @JvmStatic: TestDucklakeMetadataTemporalVersionConversion reflects this as a real
        // static method (getDeclaredMethod(...).invoke(null, ...)); without it the companion
        // accessor moves off the class and the test's static initializer fails.
        @JvmStatic
        private fun getSnapshotTimestampFromVersion(session: ConnectorSession, version: ConnectorTableVersion): Instant
        {
            val versionType: Type = version.versionType
            if (versionType == DATE) {
                return LocalDate.ofEpochDay(version.version as Long)
                        .atStartOfDay()
                        .atZone(session.timeZoneKey.zoneId)
                        .toInstant()
            }
            if (versionType is TimestampType) {
                val epochMicrosUtc: Long = if (versionType.isShort)
                    version.version as Long
                else
                    (version.version as LongTimestamp).epochMicros
                val epochSecondUtc: Long = floorDiv(epochMicrosUtc, MICROSECONDS_PER_SECOND)
                val nanosOfSecond: Int = floorMod(epochMicrosUtc, MICROSECONDS_PER_SECOND).toInt() * NANOSECONDS_PER_MICROSECOND
                return LocalDateTime.ofEpochSecond(epochSecondUtc, nanosOfSecond, ZoneOffset.UTC)
                        .atZone(session.timeZoneKey.zoneId)
                        .toInstant()
            }
            if (versionType is TimestampWithTimeZoneType) {
                val epochMillis: Long = if (versionType.isShort)
                    unpackMillisUtc(version.version as Long)
                else
                    (version.version as LongTimestampWithTimeZone).epochMillis
                return Instant.ofEpochMilli(epochMillis)
            }

            throw TrinoException(NOT_SUPPORTED, "Unsupported type for temporal table version: ${versionType.displayName}")
        }

        private fun toDoubleRange(type: Type, stats: DucklakeColumnStats): DoubleRange?
        {
            val minStr: String = stats.minValue ?: return null
            val maxStr: String = stats.maxValue ?: return null

            try {

                if (type == BIGINT || type == INTEGER || type == SMALLINT || type == TINYINT) {
                    return DoubleRange(minStr.toDouble(), maxStr.toDouble())
                }
                if (type == DOUBLE || type == REAL) {
                    return DoubleRange(minStr.toDouble(), maxStr.toDouble())
                }
                if (type == DATE) {
                    val minDays: Long = LocalDate.parse(minStr).toEpochDay()
                    val maxDays: Long = LocalDate.parse(maxStr).toEpochDay()
                    return DoubleRange(minDays.toDouble(), maxDays.toDouble())
                }
            }
            catch (_: RuntimeException) {
                // If parsing fails, skip range
            }
            return null
        }

        private fun mergePushedExpressions(existing: List<String>, additions: List<String>): List<String>
        {
            if (additions.isEmpty()) {
                return existing
            }
            val merged: ImmutableList.Builder<String> = ImmutableList.builder<String>().addAll(existing)
            for (clause in additions) {
                if (!existing.contains(clause)) {
                    merged.add(clause)
                }
            }
            return merged.build()
        }

        private fun classifyColumnConstraint(
                specs: List<DucklakePartitionSpec>,
                column: DucklakeColumnHandle): ConstraintEnforcement
        {
            if (specs.isEmpty()) {
                return ConstraintEnforcement.NOT_ENFORCED
            }

            var fullyEnforced = true
            // A predicate can only be enforced (fully or partially) if it is enforceable in EVERY active spec
            // (spec evolution means different files may have different partition schemes)
            for (spec in specs) {
                val field: Optional<DucklakePartitionField> = spec.fields.stream()
                        .filter { partitionField -> partitionField.columnId == column.columnId }
                        .findFirst()
                if (field.isEmpty) {
                    return ConstraintEnforcement.NOT_ENFORCED
                }

                val fieldEnforcement: ConstraintEnforcement = classifyTransformEnforcement(field.get(), column)
                if (fieldEnforcement == ConstraintEnforcement.NOT_ENFORCED) {
                    return ConstraintEnforcement.NOT_ENFORCED
                }
                if (fieldEnforcement == ConstraintEnforcement.PARTIALLY_ENFORCED) {
                    fullyEnforced = false
                }
            }
            return if (fullyEnforced) ConstraintEnforcement.FULLY_ENFORCED else ConstraintEnforcement.PARTIALLY_ENFORCED
        }

        private fun classifyTransformEnforcement(field: DucklakePartitionField, column: DucklakeColumnHandle): ConstraintEnforcement
        {
            if (field.transform.isIdentity()) {
                return ConstraintEnforcement.FULLY_ENFORCED
            }
            if (field.transform.isTemporal()) {
                // Temporal transforms support safe partition pruning but do not fully enforce
                // original predicates (e.g. day equality with month transform).
                val type: Type = column.columnType
                if (type == DATE || type == TIMESTAMP_MILLIS || type == TIMESTAMP_MICROS || type == TIMESTAMP_TZ_MILLIS || type == TIMESTAMP_TZ_MICROS) {
                    return ConstraintEnforcement.PARTIALLY_ENFORCED
                }
            }
            return ConstraintEnforcement.NOT_ENFORCED
        }

        private fun extractDucklakePredicate(summary: TupleDomain<ColumnHandle>): TupleDomain<DucklakeColumnHandle>
        {
            if (summary.isNone) {
                return TupleDomain.none()
            }

            val domains: Optional<Map<ColumnHandle, Domain>> = summary.getDomains()
            if (domains.isEmpty) {
                return TupleDomain.all()
            }

            val ducklakeDomains: ImmutableMap.Builder<DucklakeColumnHandle, Domain> = ImmutableMap.builder()
            for (entry in domains.get().entries) {
                if (entry.key is DucklakeColumnHandle) {
                    val columnHandle = entry.key as DucklakeColumnHandle
                    // Only push down primitive types (arrays/complex types can't be pruned)
                    if (!columnHandle.columnType.typeParameters.isEmpty()) {
                        continue
                    }
                    ducklakeDomains.put(columnHandle, entry.value)
                }
            }

            val result: Map<DucklakeColumnHandle, Domain> = ducklakeDomains.buildOrThrow()
            if (result.isEmpty()) {
                return TupleDomain.all()
            }
            return TupleDomain.withColumnDomains(result)
        }

        private fun toTupleDomain(domains: Map<DucklakeColumnHandle, Domain>): TupleDomain<DucklakeColumnHandle>
        {
            if (domains.isEmpty()) {
                return TupleDomain.all()
            }
            return TupleDomain.withColumnDomains(domains)
        }

        private fun parseMetadataTableName(tableName: SchemaTableName): MetadataTableName?
        {
            val rawTableName: String = tableName.tableName
            val separator: Int = rawTableName.lastIndexOf(METADATA_TABLE_SEPARATOR)
            if (separator <= 0 || separator == rawTableName.length - 1) {
                return null
            }

            val baseName: String = rawTableName.substring(0, separator)
            val suffix: String = rawTableName.substring(separator + 1)
            return DucklakeMetadataTableType.fromSuffix(suffix)
                    ?.let { type -> MetadataTableName(baseName, type) }
        }

        private fun toColumnHandles(metadataColumns: List<ColumnMetadata>): Map<String, ColumnHandle>
        {
            val handles: ImmutableMap.Builder<String, ColumnHandle> = ImmutableMap.builder()
            for (index in metadataColumns.indices) {
                val column: ColumnMetadata = metadataColumns[index]
                handles.put(
                        column.name,
                        DucklakeColumnHandle(
                                -(index + 1L),
                                column.name,
                                column.type,
                                column.isNullable
                        ))
            }
            return handles.buildOrThrow()
        }

        private fun getMetadataColumns(metadataTableType: DucklakeMetadataTableType): List<ColumnMetadata> =
            when (metadataTableType) {
                DucklakeMetadataTableType.FILES -> ImmutableList.of(
                        column("data_file_id", BIGINT, false),
                        column("path", VARCHAR, false),
                        column("file_format", VARCHAR, false),
                        column("record_count", BIGINT, false),
                        column("file_size_bytes", BIGINT, false),
                        column("row_id_start", BIGINT, false),
                        column("partition_id", BIGINT, true),
                        column("delete_file_path", VARCHAR, true))
                DucklakeMetadataTableType.SNAPSHOTS -> snapshotColumns()
                DucklakeMetadataTableType.CURRENT_SNAPSHOT -> snapshotColumns()
                DucklakeMetadataTableType.SNAPSHOT_CHANGES -> ImmutableList.of(
                        column("snapshot_id", BIGINT, false),
                        column("changes_made", VARCHAR, true),
                        column("author", VARCHAR, true),
                        column("commit_message", VARCHAR, true),
                        column("commit_extra_info", VARCHAR, true))
            }

        private fun snapshotColumns(): List<ColumnMetadata> =
                ImmutableList.of(
                        column("snapshot_id", BIGINT, false),
                        column("snapshot_time", TIMESTAMP_TZ_MILLIS, false),
                        column("schema_version", BIGINT, false),
                        column("next_catalog_id", BIGINT, false),
                        column("next_file_id", BIGINT, false))

        private fun column(name: String, type: Type, nullable: Boolean): ColumnMetadata =
                ColumnMetadata.builder()
                        .setName(name)
                        .setType(type)
                        .setNullable(nullable)
                        .build()

        /**
         * Translate catalog-layer TransactionConflictException to Trino's
         * TrinoException(TRANSACTION_CONFLICT) so the engine reports it correctly.
         */
        private fun translateCatalogExceptions(action: Runnable)
        {
            try {
                action.run()
            }
            catch (e: TransactionConflictException) {
                throw TrinoException(TRANSACTION_CONFLICT, e.message, e)
            }
        }
    }
}
