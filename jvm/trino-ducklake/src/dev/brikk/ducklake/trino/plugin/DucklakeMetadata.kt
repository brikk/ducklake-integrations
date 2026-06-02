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
import io.trino.spi.connector.ViewNotFoundException
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.security.TrinoPrincipal
import io.trino.spi.statistics.ColumnStatistics
import io.trino.spi.statistics.ComputedStatistics
import io.trino.spi.statistics.DoubleRange
import io.trino.spi.statistics.Estimate
import io.trino.spi.statistics.TableStatistics
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
import java.util.ArrayList
import java.util.HashSet
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
            : this(catalog, typeConverter, DucklakeSnapshotResolver(catalog, OptionalLong.empty(), Optional.empty()), null, null, null, DucklakeTemporalPartitionEncoding.CALENDAR)

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
        var querySnapshotId: OptionalLong = OptionalLong.empty()
        var querySnapshotTimestamp: Optional<Instant> = Optional.empty()
        if (queryVersion.isPresent) {
            val version = queryVersion.get()
            when (version.getPointerType()) {
                PointerType.TARGET_ID -> querySnapshotId = OptionalLong.of(getSnapshotIdFromVersion(version))
                PointerType.TEMPORAL -> querySnapshotTimestamp = Optional.of(getSnapshotTimestampFromVersion(session, version))
                else -> {}
            }
        }

        val metadataTable: Optional<MetadataTableName> = parseMetadataTableName(tableName)
        val baseTableName: SchemaTableName = metadataTable
                .map { parsed -> SchemaTableName(tableName.getSchemaName(), parsed.baseTableName) }
                .orElse(tableName)

        val snapshotId = snapshotResolver.resolveSnapshotId(session, querySnapshotId, querySnapshotTimestamp)

        val table: Optional<DucklakeTable> = catalog.getTable(baseTableName.getSchemaName(), baseTableName.getTableName(), snapshotId)
        if (table.isEmpty) {
            return null
        }

        if (metadataTable.isPresent) {
            val parsed = metadataTable.get()
            return DucklakeMetadataTableHandle(
                    tableName.getSchemaName(),
                    tableName.getTableName(),
                    parsed.baseTableName,
                    table.get().tableId,
                    snapshotId,
                    parsed.metadataTableType)
        }

        return DucklakeTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.get().tableId,
                snapshotId)
    }

    override fun getTableMetadata(session: ConnectorSession, tableHandle: ConnectorTableHandle): ConnectorTableMetadata
    {
        if (tableHandle is DucklakeMetadataTableHandle) {
            return ConnectorTableMetadata(
                    tableHandle.getSchemaTableName(),
                    getMetadataColumns(tableHandle.metadataTableType))
        }

        val ducklakeTableHandle = tableHandle as DucklakeTableHandle

        val columns: List<DucklakeColumn> = catalog.getTableColumns(
                ducklakeTableHandle.tableId,
                ducklakeTableHandle.snapshotId)

        val columnMetadata: List<ColumnMetadata> = columns.stream()
                .map { column ->
                    ColumnMetadata.builder()
                            .setName(column.columnName)
                            .setType(typeConverter.toTrinoType(column.columnType))
                            .setNullable(column.nullsAllowed)
                            .build()
                }
                .collect(toImmutableList())

        return ConnectorTableMetadata(
                ducklakeTableHandle.getSchemaTableName(),
                columnMetadata)
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
        val columnHandlesByLowercaseName: MutableMap<String, ColumnHandle> = java.util.HashMap()
        for (column in catalog.getTableColumns(handle.tableId, handle.snapshotId)) {
            columnHandlesByLowercaseName.put(
                    column.columnName.lowercase(java.util.Locale.ROOT),
                    DucklakeColumnHandle(
                            column.columnId,
                            column.columnName,
                            typeConverter.toTrinoType(column.columnType),
                            column.nullsAllowed))
        }
        val localProperties: List<LocalProperty<ColumnHandle>> =
                DucklakeSortPropertyMapper.toLocalProperties(sortKeys, columnHandlesByLowercaseName)
        if (localProperties.isEmpty()) {
            return ConnectorTableProperties()
        }
        return ConnectorTableProperties(
                io.trino.spi.predicate.TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                localProperties)
    }

    // TODO(review:after id=spi-listtables-omits-views): listTables omits views, violating the SPI contract
    override fun listTables(session: ConnectorSession, schemaName: Optional<String>): List<SchemaTableName>
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)

        if (schemaName.isPresent) {
            val schema: Optional<DucklakeSchema> = catalog.getSchema(schemaName.get(), snapshotId)
            if (schema.isEmpty) {
                return ImmutableList.of()
            }

            return catalog.listTables(schema.get().schemaId, snapshotId).stream()
                    .map { table -> SchemaTableName(schemaName.get(), table.tableName) }
                    .collect(toImmutableList())
        }

        // List all tables across all schemas
        val tables: ImmutableList.Builder<SchemaTableName> = ImmutableList.builder()
        for (schema in catalog.listSchemas(snapshotId)) {
            for (table in catalog.listTables(schema.schemaId, snapshotId)) {
                tables.add(SchemaTableName(schema.schemaName, table.tableName))
            }
        }
        return tables.build()
    }

    override fun getColumnHandles(session: ConnectorSession, tableHandle: ConnectorTableHandle): Map<String, ColumnHandle>
    {
        if (tableHandle is DucklakeMetadataTableHandle) {
            return toColumnHandles(getMetadataColumns(tableHandle.metadataTableType))
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
                .build()
    }

    override fun getTableStatistics(session: ConnectorSession, tableHandle: ConnectorTableHandle): TableStatistics
    {
        if (tableHandle is DucklakeMetadataTableHandle) {
            return TableStatistics.empty()
        }

        val table = tableHandle as DucklakeTableHandle
        val dataFiles: List<DucklakeDataFile> = catalog.getDataFiles(table.tableId, table.snapshotId)
        val hasDeleteFiles = dataFiles.stream().anyMatch { dataFile -> dataFile.deleteFilePath.isPresent }
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

        val tableStats: Optional<DucklakeTableStats> = catalog.getTableStats(table.tableId)
        val recordCount: Long
        if (tableStats.isPresent) {
            recordCount = tableStats.get().recordCount
        }
        else {
            val fallbackRecordCount = getFallbackRecordCount(table)
            if (fallbackRecordCount.isEmpty) {
                return TableStatistics.empty()
            }
            recordCount = fallbackRecordCount.asLong
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

        val seenDataFileIds: MutableSet<Long> = HashSet()
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
            val colStats: DucklakeColumnStats? = statsById.get(column.columnId)
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

            toDoubleRange(column.columnType, colStats).ifPresent { colBuilder.setRange(it) }

            stats.setColumnStatistics(column, colBuilder.build())
        }

        return stats.build()
    }

    private fun hasLiveInlinedRows(table: DucklakeTableHandle): Boolean
    {
        return catalog.getInlinedDataInfos(table.tableId, table.snapshotId).stream()
                .anyMatch { info -> catalog.hasInlinedRows(info.tableId, info.schemaVersion, table.snapshotId) }
    }

    // TODO(review:after id=eff-fallback-recordcount-materialize): readInlinedData materialises entire inlined dataset just to call .size()
    private fun getFallbackRecordCount(table: DucklakeTableHandle): OptionalLong
    {
        // Align with Iceberg/Delta behavior: if we can prove there is no data at this snapshot,
        // return row count 0 instead of unknown.
        if (!catalog.getDataFiles(table.tableId, table.snapshotId).isEmpty()) {
            // Data files exist but no table stats were found. Keep row count unknown.
            return OptionalLong.empty()
        }

        val inlinedInfos: List<DucklakeInlinedDataInfo> = catalog.getInlinedDataInfos(table.tableId, table.snapshotId)
        if (inlinedInfos.isEmpty()) {
            return OptionalLong.of(0)
        }

        val tableColumns: List<DucklakeColumn> = catalog.getTableColumns(table.tableId, table.snapshotId)
        if (tableColumns.isEmpty()) {
            return OptionalLong.of(0)
        }

        val inlinedRowCount: Long = inlinedInfos.stream()
                .mapToLong { info -> catalog.readInlinedData(
                        info.tableId,
                        info.schemaVersion,
                        table.snapshotId,
                        ImmutableList.of(tableColumns.first())).size.toLong() }
                .sum()
        return OptionalLong.of(inlinedRowCount)
    }

    override fun applyFilter(
            session: ConnectorSession,
            handle: ConnectorTableHandle,
            constraint: Constraint): Optional<ConstraintApplicationResult<ConnectorTableHandle>>
    {
        if (handle is DucklakeMetadataTableHandle) {
            return Optional.empty()
        }

        val table = handle as DucklakeTableHandle

        val summary: TupleDomain<ColumnHandle> = constraint.getSummary()

        val newEnforced: TupleDomain<DucklakeColumnHandle>
        val newUnenforced: TupleDomain<DucklakeColumnHandle>
        if (summary.isAll()) {
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

            if (!newPredicate.isNone()) {
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

            newEnforced = if (newPredicate.isNone())
                TupleDomain.none()
            else
                toTupleDomain(enforced.buildOrThrow())
            newUnenforced = if (newPredicate.isNone())
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
                constraint.getExpression(), constraint.getAssignments(), session)
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
                constraint.getExpression(),
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

        val tables: List<SchemaTableName> = prefix.getTable()
                .map { _ -> java.util.List.of(prefix.toSchemaTableName()) }
                .orElseGet { listTables(session, prefix.getSchema()) }

        for (tableName in tables) {
            val metadataTable: Optional<MetadataTableName> = parseMetadataTableName(tableName)
            if (metadataTable.isPresent) {
                val baseTable = SchemaTableName(tableName.getSchemaName(), metadataTable.get().baseTableName)
                if (catalog.getTable(baseTable.getSchemaName(), baseTable.getTableName(), snapshotId).isPresent) {
                    columns.put(tableName, getMetadataColumns(metadataTable.get().metadataTableType))
                }
                continue
            }

            val table: Optional<DucklakeTable> = catalog.getTable(tableName.getSchemaName(), tableName.getTableName(), snapshotId)
            if (table.isPresent) {
                val tableColumns: List<DucklakeColumn> = catalog.getTableColumns(table.get().tableId, snapshotId)
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
        translateCatalogExceptions(Runnable { catalog.createSchema(schemaName) })
    }

    override fun dropSchema(session: ConnectorSession, schemaName: String, cascade: Boolean)
    {
        translateCatalogExceptions(Runnable { catalog.dropSchema(schemaName) })
    }

    // ==================== Table DDL ====================

    override fun createTable(session: ConnectorSession, tableMetadata: ConnectorTableMetadata, saveMode: SaveMode)
    {
        if (saveMode == SaveMode.REPLACE) {
            throw TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables")
        }

        val tableName: SchemaTableName = tableMetadata.getTable()

        // Convert columns to DuckLake column specs
        val columnSpecs: List<TableColumnSpec> = tableMetadata.getColumns().stream()
                .map { column -> toColumnSpec(column.getName(), column.getType(), column.isNullable()) }
                .collect(toImmutableList())

        // Parse partition spec from table properties
        val partitionFields: List<PartitionFieldSpec> = DucklakeTableProperties.getPartitionFields(tableMetadata.getProperties())
        val partitionSpec: Optional<List<PartitionFieldSpec>> = if (partitionFields.isEmpty())
            Optional.empty()
        else
            Optional.of(partitionFields)

        val location: Optional<TableLocationSpec> = DucklakeTableProperties.getLocation(tableMetadata.getProperties())

        translateCatalogExceptions(Runnable { catalog.createTable(tableName.getSchemaName(), tableName.getTableName(), columnSpecs, partitionSpec, location) })
    }

    private fun toColumnSpec(name: String, trinoType: Type, nullable: Boolean): TableColumnSpec
    {
        val ducklakeType: String = typeConverter.toDucklakeType(trinoType)

        if (trinoType is ArrayType) {
            val children: List<TableColumnSpec> = ImmutableList.of(
                    toColumnSpec("element", trinoType.getElementType(), true))
            return TableColumnSpec(name, ducklakeType, nullable, children)
        }
        if (trinoType is RowType) {
            val children: List<TableColumnSpec> = trinoType.getFields().stream()
                    .map { field -> toColumnSpec(
                            field.getName().orElseThrow { TrinoException(NOT_SUPPORTED, "Anonymous row fields not supported") },
                            field.getType(),
                            true) }
                    .collect(toImmutableList())
            return TableColumnSpec(name, ducklakeType, nullable, children)
        }
        if (trinoType is MapType) {
            val children: List<TableColumnSpec> = ImmutableList.of(
                    toColumnSpec("key", trinoType.getKeyType(), false),
                    toColumnSpec("value", trinoType.getValueType(), true))
            return TableColumnSpec(name, ducklakeType, nullable, children)
        }

        return TableColumnSpec.leaf(name, ducklakeType, nullable)
    }

    override fun dropTable(session: ConnectorSession, tableHandle: ConnectorTableHandle)
    {
        val handle = tableHandle as DucklakeTableHandle
        translateCatalogExceptions(Runnable { catalog.dropTable(handle.schemaName, handle.tableName) })
    }

    // ==================== ALTER TABLE ====================

    override fun addColumn(session: ConnectorSession, tableHandle: ConnectorTableHandle, column: ColumnMetadata, position: ColumnPosition)
    {
        val handle = tableHandle as DucklakeTableHandle
        val columnSpec: TableColumnSpec = toColumnSpec(column.getName(), column.getType(), column.isNullable())
        translateCatalogExceptions(Runnable { catalog.addColumn(handle.tableId, columnSpec) })
    }

    override fun dropColumn(session: ConnectorSession, tableHandle: ConnectorTableHandle, column: ColumnHandle)
    {
        val handle = tableHandle as DucklakeTableHandle
        val ducklakeColumn = column as DucklakeColumnHandle
        translateCatalogExceptions(Runnable { catalog.dropColumn(handle.tableId, ducklakeColumn.columnId) })
    }

    override fun renameColumn(session: ConnectorSession, tableHandle: ConnectorTableHandle, source: ColumnHandle, target: String)
    {
        val handle = tableHandle as DucklakeTableHandle
        val ducklakeColumn = source as DucklakeColumnHandle
        translateCatalogExceptions(Runnable { catalog.renameColumn(handle.tableId, ducklakeColumn.columnId, target) })
    }

    // ==================== INSERT ====================

    override fun beginInsert(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle,
            columns: List<ColumnHandle>,
            retryMode: RetryMode): ConnectorInsertTableHandle
    {
        val handle = tableHandle as DucklakeTableHandle

        val ducklakeColumns: List<DucklakeColumnHandle> = columns.stream()
                .map(DucklakeColumnHandle::class.java::cast)
                .collect(toImmutableList())

        val allCatalogColumns: List<DucklakeColumn> = catalog.getAllColumnsWithParentage(handle.tableId, handle.snapshotId)

        val partitionSpecs: List<DucklakePartitionSpec> = catalog.getPartitionSpecs(handle.tableId, handle.snapshotId)
        val activePartitionSpec: Optional<DucklakePartitionSpec> = if (partitionSpecs.isEmpty())
            Optional.empty()
        else
            Optional.of(partitionSpecs.last())

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
            translateCatalogExceptions(Runnable { catalog.commitInsert(handle.tableId, writeFragments) })
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

        val tableName: SchemaTableName = tableMetadata.getTable()

        // Create the table structure (DDL snapshot)
        val columnSpecs: List<TableColumnSpec> = tableMetadata.getColumns().stream()
                .map { column -> toColumnSpec(column.getName(), column.getType(), column.isNullable()) }
                .collect(toImmutableList())

        val partitionFields: List<PartitionFieldSpec> = DucklakeTableProperties.getPartitionFields(tableMetadata.getProperties())
        val partitionSpec: Optional<List<PartitionFieldSpec>> = if (partitionFields.isEmpty())
            Optional.empty()
        else
            Optional.of(partitionFields)

        val location: Optional<TableLocationSpec> = DucklakeTableProperties.getLocation(tableMetadata.getProperties())

        translateCatalogExceptions(Runnable { catalog.createTable(tableName.getSchemaName(), tableName.getTableName(), columnSpecs, partitionSpec, location) })

        // Resolve the newly created table to get its ID and build a writable handle
        val snapshotId: Long = catalog.currentSnapshotId
        val table: Optional<DucklakeTable> = catalog.getTable(tableName.getSchemaName(), tableName.getTableName(), snapshotId)
        if (table.isEmpty) {
            throw TrinoException(NOT_SUPPORTED, "Table was not created: " + tableName)
        }

        val catalogColumns: List<DucklakeColumn> = catalog.getTableColumns(table.get().tableId, snapshotId)
        val columnHandles: List<DucklakeColumnHandle> = catalogColumns.stream()
                .filter { col -> col.parentColumn.isEmpty }
                .map { col -> DucklakeColumnHandle(
                        col.columnId,
                        col.columnName,
                        typeConverter.toTrinoType(col.columnType),
                        col.nullsAllowed) }
                .collect(toImmutableList())

        val allCatalogColumns: List<DucklakeColumn> = catalog.getAllColumnsWithParentage(table.get().tableId, snapshotId)

        val partitionSpecs: List<DucklakePartitionSpec> = catalog.getPartitionSpecs(table.get().tableId, snapshotId)
        val activePartitionSpec: Optional<DucklakePartitionSpec> = if (partitionSpecs.isEmpty())
            Optional.empty()
        else
            Optional.of(partitionSpecs.last())

        val tableDataPath: String = resolveTableDataPath(tableName.getSchemaName(), tableName.getTableName(), snapshotId)

        // CTAS precedence: WITH clause > session property > connector default. The
        // "match latest existing data file" rule that drives INSERT inheritance can't
        // apply here because the table is fresh — there are no prior data files.
        // (See N1 in TODO-duckdb-lake-format.md.)
        val tablePropertyFormat: String? = DucklakeTableProperties.getDataFileFormat(tableMetadata.getProperties())
        val fileFormat: String = if (tablePropertyFormat != null)
            tablePropertyFormat
        else
            DucklakeSessionProperties.getDataFileFormat(session)
                    .orElse(DucklakeSessionProperties.FORMAT_PARQUET)

        return DucklakeWritableTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.get().tableId,
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
            translateCatalogExceptions(Runnable { catalog.commitInsert(handle.tableId, writeFragments) })
        }

        return Optional.empty()
    }

    private fun deserializeFragments(fragments: Collection<Slice>): List<DucklakeWriteFragment>
    {
        return fragments.stream()
                .map { fragment -> fragmentCodec!!.fromJson(fragment.getBytes()) }
                .collect(toImmutableList())
    }

    /**
     * Resolve the data file format for an INSERT (or the insert leg of a MERGE/UPDATE).
     * <p>Precedence (N1):
     * <ol>
     *   <li>Session property {@code ducklake.data_file_format}, when explicitly set.</li>
     *   <li>Format of the most recent active data file already in the table.</li>
     *   <li>Connector default ({@code parquet}).</li>
     * </ol>
     * The CTAS-time {@code WITH (data_file_format = ...)} clause is not in this chain — it
     * applies only to the materialization that creates the table. There is no per-table
     * persistence of the format yet (no schema-level extensible properties in DuckLake spec),
     * so rule 2 is what makes the natural CTAS-then-INSERT workflow keep a consistent format.
     */
    private fun resolveWriteFormat(session: ConnectorSession, tableId: Long, snapshotId: Long): String
    {
        val sessionFormat: Optional<String> = DucklakeSessionProperties.getDataFileFormat(session)
        if (sessionFormat.isPresent) {
            return sessionFormat.get()
        }
        val latestFormat: Optional<String> = catalog.getLatestDataFileFormat(tableId, snapshotId)
        if (latestFormat.isPresent) {
            return latestFormat.get()
        }
        return DucklakeSessionProperties.FORMAT_PARQUET
    }

    private fun resolveTableDataPath(schemaName: String, tableName: String, snapshotId: Long): String
    {
        val schema: Optional<DucklakeSchema> = catalog.getSchema(schemaName, snapshotId)
        val table: Optional<DucklakeTable> = catalog.getTable(schemaName, tableName, snapshotId)

        if (schema.isEmpty || table.isEmpty) {
            throw TrinoException(NOT_SUPPORTED, "Cannot resolve data path for " + schemaName + "." + tableName)
        }

        return pathResolver!!.resolveTableDataPath(schema.get(), table.get())
    }

    // ==================== DELETE / MERGE ====================

    override fun getRowChangeParadigm(session: ConnectorSession, tableHandle: ConnectorTableHandle): RowChangeParadigm
    {
        return RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW
    }

    override fun getMergeRowIdColumnHandle(session: ConnectorSession, tableHandle: ConnectorTableHandle): ColumnHandle
    {
        return DucklakeColumnHandle.rowIdColumnHandle()
    }

    override fun beginMerge(
            session: ConnectorSession,
            tableHandle: ConnectorTableHandle,
            updateCaseColumns: Map<Int, Collection<ColumnHandle>>,
            retryMode: RetryMode): ConnectorMergeTableHandle
    {
        val handle = tableHandle as DucklakeTableHandle

        // Build insert handle for UPDATE support (delete+insert pattern)
        val ducklakeColumns: List<DucklakeColumnHandle> = catalog.getTableColumns(handle.tableId, handle.snapshotId).stream()
                .filter { col -> col.parentColumn.isEmpty }
                .map { col -> DucklakeColumnHandle(
                        col.columnId,
                        col.columnName,
                        typeConverter.toTrinoType(col.columnType),
                        col.nullsAllowed) }
                .collect(toImmutableList())

        val allCatalogColumns: List<DucklakeColumn> = catalog.getAllColumnsWithParentage(handle.tableId, handle.snapshotId)

        val partitionSpecs: List<DucklakePartitionSpec> = catalog.getPartitionSpecs(handle.tableId, handle.snapshotId)
        val activePartitionSpec: Optional<DucklakePartitionSpec> = if (partitionSpecs.isEmpty())
            Optional.empty()
        else
            Optional.of(partitionSpecs.last())

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
        val primaryByFileId: LinkedHashMap<Long, DucklakeDataFile> = LinkedHashMap()
        val deletePathsByFileId: LinkedHashMap<Long, MutableList<String>> = LinkedHashMap()
        for (df in dataFiles) {
            primaryByFileId.putIfAbsent(df.dataFileId, df)
            if (df.deleteFilePath.isPresent) {
                val resolved: String = pathResolver!!.resolveFilePath(
                        df.deleteFilePath.orElseThrow(),
                        df.deleteFilePathIsRelative.orElse(false),
                        tableDataPath)
                deletePathsByFileId
                        .computeIfAbsent(df.dataFileId) { _ -> ArrayList() }
                        .add(resolved)
            }
        }
        val dataFileRanges: List<DucklakeMergeTableHandle.DataFileRange> = primaryByFileId.values.stream()
                .map { df -> DucklakeMergeTableHandle.DataFileRange(
                        df.dataFileId,
                        df.rowIdStart,
                        df.recordCount,
                        deletePathsByFileId.getOrDefault(df.dataFileId, emptyList())) }
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

        val deleteFragments: MutableList<DucklakeDeleteFragment> = ArrayList()
        val insertFragments: MutableList<DucklakeWriteFragment> = ArrayList()

        for (fragment in fragments) {
            val bytes: ByteArray = fragment.getBytes()
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
            translateCatalogExceptions(Runnable { catalog.commitMerge(tableHandle.tableId, deleteFragments, insertFragments) })
        }
        else if (!deleteFragments.isEmpty()) {
            translateCatalogExceptions(Runnable { catalog.commitDelete(tableHandle.tableId, deleteFragments) })
        }
        else if (!insertFragments.isEmpty()) {
            translateCatalogExceptions(Runnable { catalog.commitInsert(tableHandle.tableId, insertFragments) })
        }
    }

    // ==================== View operations ====================

    override fun listViews(session: ConnectorSession, schemaName: Optional<String>): List<SchemaTableName>
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)

        if (schemaName.isPresent) {
            val schema: Optional<DucklakeSchema> = catalog.getSchema(schemaName.get(), snapshotId)
            if (schema.isEmpty) {
                return ImmutableList.of()
            }
            return catalog.listViews(schema.get().schemaId, snapshotId).stream()
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

        val ducklakeView: Optional<DucklakeView> = catalog.getView(viewName.getSchemaName(), viewName.getTableName(), snapshotId)
        if (ducklakeView.isEmpty) {
            return Optional.empty()
        }

        val view: DucklakeView = ducklakeView.get()
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
            val schema: Optional<DucklakeSchema> = catalog.getSchema(schemaName.get(), snapshotId)
            if (schema.isEmpty) {
                return ImmutableMap.of()
            }
            schemas = ImmutableList.of(schema.get())
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
            val existing: Optional<DucklakeView> = catalog.getView(viewName.getSchemaName(), viewName.getTableName(), snapshotId)
            if (existing.isPresent) {
                translateCatalogExceptions(Runnable { catalog.dropView(viewName.getSchemaName(), viewName.getTableName()) })
            }
        }

        // Store the original SQL in the sql field (spec-compliant).
        // Store the full ConnectorViewDefinition as JSON in column_aliases
        // so we can round-trip column names and types.
        val originalSql: String = definition.getOriginalSql()
        val viewMetadataJson: String = VIEW_CODEC.toJson(definition)

        translateCatalogExceptions(Runnable { catalog.createView(viewName.getSchemaName(), viewName.getTableName(), originalSql, TRINO_VIEW_DIALECT, viewMetadataJson) })
    }

    override fun renameView(session: ConnectorSession, source: SchemaTableName, target: SchemaTableName)
    {
        if (source == target) {
            return
        }

        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        val sourceView: Optional<DucklakeView> = catalog.getView(source.getSchemaName(), source.getTableName(), snapshotId)
        if (sourceView.isEmpty || !isViewAccessible(sourceView.get())) {
            throw ViewNotFoundException(source)
        }

        if (catalog.getTable(target.getSchemaName(), target.getTableName(), snapshotId).isPresent || catalog.getView(target.getSchemaName(), target.getTableName(), snapshotId).isPresent) {
            throw TrinoException(ALREADY_EXISTS, "Relation already exists: " + target)
        }

        translateCatalogExceptions(Runnable { catalog.renameView(source.getSchemaName(), source.getTableName(), target.getSchemaName(), target.getTableName()) })
    }

    override fun setViewComment(session: ConnectorSession, viewName: SchemaTableName, comment: Optional<String>)
    {
        val definition: ConnectorViewDefinition = getRequiredViewDefinition(session, viewName)
        val updated = ConnectorViewDefinition(
                definition.getOriginalSql(),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns(),
                comment,
                definition.getOwner(),
                definition.isRunAsInvoker(),
                definition.getPath())

        translateCatalogExceptions(Runnable { catalog.replaceViewMetadata(
                viewName.getSchemaName(),
                viewName.getTableName(),
                updated.getOriginalSql(),
                TRINO_VIEW_DIALECT,
                VIEW_CODEC.toJson(updated)) })
    }

    override fun setViewColumnComment(session: ConnectorSession, viewName: SchemaTableName, columnName: String, comment: Optional<String>)
    {
        val definition: ConnectorViewDefinition = getRequiredViewDefinition(session, viewName)

        var updated = false
        val columnsBuilder: ImmutableList.Builder<ConnectorViewDefinition.ViewColumn> = ImmutableList.builderWithExpectedSize(definition.getColumns().size)
        for (column in definition.getColumns()) {
            if (column.getName() == columnName) {
                columnsBuilder.add(ConnectorViewDefinition.ViewColumn(column.getName(), column.getType(), comment))
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
                definition.getOriginalSql(),
                definition.getCatalog(),
                definition.getSchema(),
                columns,
                definition.getComment(),
                definition.getOwner(),
                definition.isRunAsInvoker(),
                definition.getPath())

        translateCatalogExceptions(Runnable { catalog.replaceViewMetadata(
                viewName.getSchemaName(),
                viewName.getTableName(),
                updatedDefinition.getOriginalSql(),
                TRINO_VIEW_DIALECT,
                VIEW_CODEC.toJson(updatedDefinition)) })
    }

    override fun dropView(session: ConnectorSession, viewName: SchemaTableName)
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        val existing: Optional<DucklakeView> = catalog.getView(viewName.getSchemaName(), viewName.getTableName(), snapshotId)
        if (existing.isEmpty) {
            throw ViewNotFoundException(viewName)
        }

        translateCatalogExceptions(Runnable { catalog.dropView(viewName.getSchemaName(), viewName.getTableName()) })
    }

    private fun getRequiredViewDefinition(session: ConnectorSession, viewName: SchemaTableName): ConnectorViewDefinition
    {
        val snapshotId = snapshotResolver.resolveSnapshotId(session)
        val view: Optional<DucklakeView> = catalog.getView(viewName.getSchemaName(), viewName.getTableName(), snapshotId)
        if (view.isEmpty || !isViewAccessible(view.get())) {
            throw ViewNotFoundException(viewName)
        }

        return decodeTrinoView(view.get(), viewName)
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
        if (view.viewMetadata.isPresent && !view.viewMetadata.get().isBlank()) {
            try {
                return Optional.of(VIEW_CODEC.fromJson(view.viewMetadata.get()))
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
        private val VIEW_CODEC: JsonCodec<ConnectorViewDefinition> = JsonCodecFactory().jsonCodec(ConnectorViewDefinition::class.java)
        // `trino/brikk` — base dialect is Trino SQL, `/brikk` marks that this row also carries
        // our plugin-specific JSON sidecar in `ducklake_view.column_aliases` (serialized
        // `ConnectorViewDefinition`). Other writers that understand plain Trino SQL should skip
        // rows with this suffix unless they want to cross-parse our metadata.
        private const val TRINO_VIEW_DIALECT: String = "trino/brikk"

        private fun getSnapshotIdFromVersion(version: ConnectorTableVersion): Long
        {
            val versionType: Type = version.getVersionType()
            if (versionType === SMALLINT || versionType === TINYINT || versionType === INTEGER || versionType === BIGINT) {
                return (version.getVersion() as Number).toLong()
            }

            throw TrinoException(NOT_SUPPORTED, "Unsupported type for table version: " + versionType.getDisplayName())
        }

        @JvmStatic
        private fun getSnapshotTimestampFromVersion(session: ConnectorSession, version: ConnectorTableVersion): Instant
        {
            val versionType: Type = version.getVersionType()
            if (versionType.equals(DATE)) {
                return LocalDate.ofEpochDay(version.getVersion() as Long)
                        .atStartOfDay()
                        .atZone(session.getTimeZoneKey().getZoneId())
                        .toInstant()
            }
            if (versionType is TimestampType) {
                val epochMicrosUtc: Long = if (versionType.isShort())
                    version.getVersion() as Long
                else
                    (version.getVersion() as LongTimestamp).getEpochMicros()
                val epochSecondUtc: Long = floorDiv(epochMicrosUtc, MICROSECONDS_PER_SECOND)
                val nanosOfSecond: Int = floorMod(epochMicrosUtc, MICROSECONDS_PER_SECOND).toInt() * NANOSECONDS_PER_MICROSECOND
                return LocalDateTime.ofEpochSecond(epochSecondUtc, nanosOfSecond, ZoneOffset.UTC)
                        .atZone(session.getTimeZoneKey().getZoneId())
                        .toInstant()
            }
            if (versionType is TimestampWithTimeZoneType) {
                val epochMillis: Long = if (versionType.isShort())
                    unpackMillisUtc(version.getVersion() as Long)
                else
                    (version.getVersion() as LongTimestampWithTimeZone).getEpochMillis()
                return Instant.ofEpochMilli(epochMillis)
            }

            throw TrinoException(NOT_SUPPORTED, "Unsupported type for temporal table version: " + versionType.getDisplayName())
        }

        private fun toDoubleRange(type: Type, stats: DucklakeColumnStats): Optional<DoubleRange>
        {
            if (stats.minValue.isEmpty || stats.maxValue.isEmpty) {
                return Optional.empty()
            }

            try {
                val minStr: String = stats.minValue.get()
                val maxStr: String = stats.maxValue.get()

                if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
                    return Optional.of(DoubleRange(minStr.toDouble(), maxStr.toDouble()))
                }
                if (type.equals(DOUBLE) || type.equals(REAL)) {
                    return Optional.of(DoubleRange(minStr.toDouble(), maxStr.toDouble()))
                }
                if (type.equals(DATE)) {
                    val minDays: Long = java.time.LocalDate.parse(minStr).toEpochDay()
                    val maxDays: Long = java.time.LocalDate.parse(maxStr).toEpochDay()
                    return Optional.of(DoubleRange(minDays.toDouble(), maxDays.toDouble()))
                }
            }
            catch (_: RuntimeException) {
                // If parsing fails, skip range
            }
            return Optional.empty()
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
                if (type.equals(DATE) || type.equals(TIMESTAMP_MILLIS) || type.equals(TIMESTAMP_MICROS) || type.equals(TIMESTAMP_TZ_MILLIS) || type.equals(TIMESTAMP_TZ_MICROS)) {
                    return ConstraintEnforcement.PARTIALLY_ENFORCED
                }
            }
            return ConstraintEnforcement.NOT_ENFORCED
        }

        private fun extractDucklakePredicate(summary: TupleDomain<ColumnHandle>): TupleDomain<DucklakeColumnHandle>
        {
            if (summary.isNone()) {
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
                    if (!columnHandle.columnType.getTypeParameters().isEmpty()) {
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

        private fun parseMetadataTableName(tableName: SchemaTableName): Optional<MetadataTableName>
        {
            val rawTableName: String = tableName.getTableName()
            val separator: Int = rawTableName.lastIndexOf(METADATA_TABLE_SEPARATOR)
            if (separator <= 0 || separator == rawTableName.length - 1) {
                return Optional.empty()
            }

            val baseName: String = rawTableName.substring(0, separator)
            val suffix: String = rawTableName.substring(separator + 1)
            return DucklakeMetadataTableType.fromSuffix(suffix)
                    .map { type -> MetadataTableName(baseName, type) }
        }

        private fun toColumnHandles(metadataColumns: List<ColumnMetadata>): Map<String, ColumnHandle>
        {
            val handles: ImmutableMap.Builder<String, ColumnHandle> = ImmutableMap.builder()
            for (index in metadataColumns.indices) {
                val column: ColumnMetadata = metadataColumns.get(index)
                handles.put(
                        column.getName(),
                        DucklakeColumnHandle(
                                -(index + 1L),
                                column.getName(),
                                column.getType(),
                                column.isNullable()))
            }
            return handles.buildOrThrow()
        }

        private fun getMetadataColumns(metadataTableType: DucklakeMetadataTableType): List<ColumnMetadata>
        {
            return when (metadataTableType) {
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
        }

        private fun snapshotColumns(): List<ColumnMetadata>
        {
            return ImmutableList.of(
                    column("snapshot_id", BIGINT, false),
                    column("snapshot_time", TIMESTAMP_TZ_MILLIS, false),
                    column("schema_version", BIGINT, false),
                    column("next_catalog_id", BIGINT, false),
                    column("next_file_id", BIGINT, false))
        }

        private fun column(name: String, type: Type, nullable: Boolean): ColumnMetadata
        {
            return ColumnMetadata.builder()
                    .setName(name)
                    .setType(type)
                    .setNullable(nullable)
                    .build()
        }

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
