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
import com.google.inject.Inject
import com.google.inject.Provider
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeFileColumnStats
import dev.brikk.ducklake.catalog.DucklakePartitionSpec
import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable
import dev.brikk.ducklake.catalog.TransactionConflictException
import io.airlift.slice.Slices.utf8Slice
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.filesystem.TrinoInputFile
import io.trino.memory.context.AggregatedMemoryContext
import io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext
import io.trino.parquet.ParquetDataSource
import io.trino.parquet.ParquetReaderOptions
import io.trino.parquet.metadata.FileMetadata
import io.trino.parquet.metadata.ParquetMetadata
import io.trino.parquet.reader.MetadataReader
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource
import io.trino.plugin.hive.parquet.ParquetReaderConfig
import io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.procedure.Procedure
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.VarcharType.VARCHAR
import java.io.IOException
import java.lang.invoke.MethodHandle
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.sql.SQLException
import java.util.Optional
import java.util.OptionalLong

/**
 * Implements `CALL <catalog>.system.add_files(...)` — registers
 * pre-existing data files (parquet, lance dataset directories, vortex files, duckdb
 * `.db` files) as DuckLake data files of a table without rewriting. Mirrors upstream's
 * `ducklake_add_data_files` table function for parquet; lance/vortex/duckdb are registered
 * opaquely (no footer/stats/name map) and read through the FileScan / ATTACH paths.
 *
 * v1 contract:
 *
 *   - Each entry in `FILES` is a concrete file path (no glob expansion yet).
 *   - Parquet column names must match the table column names case-insensitively;
 *       reordering is permitted. Lance/vortex/duckdb reads project by name, so their column
 *       names must match the table's exactly (as DuckDB renders them). A duckdb `.db` file
 *       must hold the data in a single `main.t` table (the shape the connector's own duckdb
 *       writer emits and the read path ATTACHes).
 *   - Hive partitioning supports the IDENTITY transform only (path segments of the
 *       form `key=value/`). Files for tables with transform-based partition
 *       specs (year / month / etc.) are out of scope for v1.
 *   - Partitioned tables: parquet uses `hive_partitioning => true` to read each partition value
 *       from the `key=value/` path (the column may be omitted from the file body — it is
 *       constant-filled on read). lance/vortex ALSO support `hive_partitioning => true` now, but
 *       because opaque scan-registered files are column-projected by the DuckDB engine (no
 *       constant-fill), the partition column must be PRESENT in the file; the path value is
 *       recorded so the registered file is prunable without a scan. A partitioned lance/vortex
 *       `add_files` WITHOUT `hive_partitioning` is rejected (we can't place the opaque file).
 *
 */
class DucklakeAddFilesProcedure @Inject constructor(
        private val catalog: DucklakeCatalog,
        private val fileSystemFactory: DucklakeFileSystemFactory,
        private val typeConverter: DucklakeTypeConverter,
        private val pathResolver: DucklakePathResolver,
        private val fileFormatDataSourceStats: FileFormatDataSourceStats,
        private val executorFactory: DucklakeDuckDbExecutorFactory,
        private val duckDbS3Config: DuckDbS3Config,
        parquetReaderConfig: ParquetReaderConfig,
) : Provider<Procedure> {
    private val parquetReaderOptions: ParquetReaderOptions = parquetReaderConfig.toParquetReaderOptions()

    override fun get(): Procedure =
        Procedure(
                "system",
                "add_files",
                ImmutableList.of(
                        Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        Procedure.Argument("TABLE_NAME", VARCHAR),
                        Procedure.Argument("FILES", ArrayType(VARCHAR)),
                        Procedure.Argument("ALLOW_MISSING", BOOLEAN, false, false),
                        Procedure.Argument("IGNORE_EXTRA_COLUMNS", BOOLEAN, false, false),
                        Procedure.Argument("HIVE_PARTITIONING", BOOLEAN, false, false),
                        // Optional format selector. Default 'parquet' keeps every existing call
                        // unchanged. 'lance' registers an externally-written Lance dataset
                        // *directory* (read via the FileScan __lance_scan path); 'vortex' a
                        // single externally-written .vortex file (FileScan read_vortex path);
                        // 'duckdb' a single externally-written .db file (read via the ATTACH
                        // path, reading its main.t table) — all three without a parquet footer,
                        // see buildLanceFragment / buildVortexFragment / buildDuckDbFragment.
                        Procedure.Argument("FILE_FORMAT", VARCHAR, false, utf8Slice("parquet"))),
                ADD_FILES.bindTo(this),
                true)

    @Suppress("unused") // invoked via MethodHandle
    fun addFiles(
            session: ConnectorSession,
            schemaName: String?,
            tableName: String?,
            fileList: List<*>?,
            allowMissing: Boolean,
            ignoreExtraColumns: Boolean,
            hivePartitioning: Boolean,
            fileFormat: String?,
    ) {
        if (schemaName.isNullOrEmpty()) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "schema_name is required")
        }
        if (tableName.isNullOrEmpty()) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "table_name is required")
        }
        if (fileList.isNullOrEmpty()) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "files must be a non-empty array")
        }
        val format: String = (fileFormat ?: FORMAT_PARQUET).lowercase()
        // add_files supports every format the connector can write (parquet + the three opaque
        // scan/attach formats), which is exactly SUPPORTED_DATA_FILE_FORMATS.
        if (format !in DucklakeSessionProperties.SUPPORTED_DATA_FILE_FORMATS) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT,
                    "add_files file_format must be 'parquet', 'lance', 'vortex', or 'duckdb', got '$format'")
        }
        // lance + vortex + duckdb register opaquely (no footer, no stats, no name map). Partitioned
        // registration is supported via hive_partitioning => true (the partition values are read
        // from the key=value/ path layout, identity transforms only — see the gate below).
        val isScanRegistered: Boolean = format != FORMAT_PARQUET

        val filePaths = extractStringArray(fileList)
        val snapshotId = catalog.currentSnapshotId
        if (catalog.getSchema(schemaName, snapshotId) == null) {
            throw TrinoException(NOT_SUPPORTED, "Schema not found: $schemaName")
        }
        val tableInfo: DucklakeTable = catalog.getTable(schemaName, tableName, snapshotId)
            ?: throw TrinoException(NOT_SUPPORTED, "Table not found: $schemaName.$tableName")
        val tableId = tableInfo.tableId

        // getTableColumns returns top-level columns with type strings already resolved
        // ("struct(j integer, i integer)" rather than the catalog row's literal "struct"),
        // which the DucklakeTypeConverter expects. getAllColumnsWithParentage is the flat
        // tree the name mapper needs for descending into children of nested types.
        val allColumns: List<DucklakeColumn> = catalog.getAllColumnsWithParentage(tableId, snapshotId)
        val topLevelColumns: List<DucklakeColumn> = catalog.getTableColumns(tableId, snapshotId)

        val partitionSpecs: List<DucklakePartitionSpec> = catalog.getPartitionSpecs(tableId, snapshotId)
        val activePartitionSpec: Optional<DucklakePartitionSpec> = activePartitionSpecOf(partitionSpecs)

        // A lance/vortex file is registered opaquely (no column read), so the only way to know
        // which partition it belongs to is the hive-style key=value/ path layout. Require
        // hive_partitioning => true for a partitioned table; without it we can't place the file.
        if (isScanRegistered && activePartitionSpec.isPresent && !hivePartitioning) {
            throw TrinoException(NOT_SUPPORTED,
                    "add_files file_format => '$format' into a partitioned table requires hive_partitioning => true "
                            + "(the partition value is read from the key=value/ path); table \"$schemaName.$tableName\" is partitioned")
        }

        if (activePartitionSpec.isPresent && hivePartitioning) {
            for (field in activePartitionSpec.get().fields) {
                if (field.transform != DucklakePartitionTransform.IDENTITY) {
                    throw TrinoException(NOT_SUPPORTED, String.format(
                            "add_files with hive_partitioning => true currently supports identity partition transforms only; "
                                    + "table \"%s.%s\" has transform %s",
                            schemaName, tableName, field.transform))
                }
            }
        }

        val fileSystem: TrinoFileSystem = fileSystemFactory.create(session)

        // Opaque-format (lance/vortex/duckdb) registration is validated by PROJECTING the table's
        // data columns out of the source and converting one batch — this fails loudly here on a
        // missing/renamed/incompatibly-typed column instead of committing an unreadable catalog
        // entry that only errors at first SELECT. Partition columns live in the hive path, not the
        // file, so they are excluded from the projection.
        val partitionColumnIds: Set<Long> = activePartitionSpec
                .map { spec -> spec.fields.filter { it.transform == DucklakePartitionTransform.IDENTITY }
                        .map { it.columnId }.toSet() }
                .orElse(emptySet())
        val dataColumnHandles: List<DucklakeColumnHandle> = topLevelColumns
                .filter { it.columnId !in partitionColumnIds }
                .map { DucklakeColumnHandle(it.columnId, it.columnName, typeConverter.toTrinoType(it.columnType), it.nullsAllowed) }

        val processed: MutableSet<String> = mutableSetOf()
        val fragments: MutableList<dev.brikk.ducklake.catalog.DucklakeWriteFragment> = mutableListOf()

        for (filePath in filePaths) {
            if (filePath.isEmpty()) {
                throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "files contains a null/empty path")
            }
            val normalized = filePath.replace('\\', '/')
            if (!processed.add(normalized)) {
                continue
            }
            fragments.add(when (format) {
                DucklakeSessionProperties.FORMAT_LANCE -> {
                    val (pv, pid) = scanPartitionValues(filePath, activePartitionSpec, topLevelColumns, hivePartitioning)
                    buildLanceFragment(fileSystem, filePath, pv, pid, dataColumnHandles)
                }
                DucklakeSessionProperties.FORMAT_VORTEX -> {
                    val (pv, pid) = scanPartitionValues(filePath, activePartitionSpec, topLevelColumns, hivePartitioning)
                    buildVortexFragment(fileSystem, filePath, pv, pid, dataColumnHandles)
                }
                DucklakeSessionProperties.FORMAT_DUCKDB -> {
                    val (pv, pid) = scanPartitionValues(filePath, activePartitionSpec, topLevelColumns, hivePartitioning)
                    buildDuckDbFragment(fileSystem, filePath, pv, pid, dataColumnHandles)
                }
                else -> buildFragment(
                        fileSystem,
                        filePath,
                        schemaName,
                        tableName,
                        allColumns,
                        topLevelColumns,
                        activePartitionSpec,
                        allowMissing,
                        ignoreExtraColumns,
                        hivePartitioning)
            })
        }

        try {
            catalog.commitAddFiles(tableId, fragments)
        }
        catch (e: TransactionConflictException) {
            throw TrinoException(TRANSACTION_CONFLICT, e.message, e)
        }
    }

    private fun buildFragment(
            fileSystem: TrinoFileSystem,
            filePath: String,
            schemaName: String,
            tableName: String,
            allColumns: List<DucklakeColumn>,
            topLevelColumns: List<DucklakeColumn>,
            activePartitionSpec: Optional<DucklakePartitionSpec>,
            allowMissing: Boolean,
            ignoreExtraColumns: Boolean,
            hivePartitioning: Boolean,
    ): dev.brikk.ducklake.catalog.DucklakeWriteFragment {
        val inputFile: TrinoInputFile
        val fileSize: Long
        try {
            inputFile = fileSystem.newInputFile(Location.of(filePath))
            if (!inputFile.exists()) {
                throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "File does not exist: $filePath")
            }
            fileSize = inputFile.length()
        }
        catch (e: IOException) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "Failed to open file: $filePath", e)
        }

        val hivePartitionValues: Map<String, String> = if (hivePartitioning)
            parseHivePartitions(filePath)
        else
            mapOf()

        val memoryContext: AggregatedMemoryContext = newSimpleAggregatedMemoryContext()
        var dataSource: ParquetDataSource? = null
        try {
            dataSource = createDataSource(
                    inputFile,
                    OptionalLong.of(fileSize),
                    parquetReaderOptions,
                    memoryContext,
                    fileFormatDataSourceStats)
            // Read the file tail exactly once and reuse it for both the footer_size probe and
            // MetadataReader.readFooter. readFooter reads min(fileSize, footerReadSize) from the
            // end and derives the metadata length from the same post-script we need; previously
            // readFooterLengthFromPostScript then issued a *second*, uncached readTail(8) — an
            // extra object-store round trip per file in the batch. We pre-read the tail at the
            // size readFooter would use, parse footer_size from its post-script, and hand the
            // bytes to readFooter via the single-shot prefetch wrapper (which falls through to
            // the delegate if the footer turns out larger than this read, exactly as before).
            val tailReadSize = minOf(dataSource.estimatedSize, parquetReaderOptions.footerReadSize.toBytes())
            var footerSize = 0L
            var footerSource: ParquetDataSource = dataSource
            if (tailReadSize >= POST_SCRIPT_SIZE) {
                val tail: io.airlift.slice.Slice = dataSource.readTail(tailReadSize.toInt())
                footerSize = footerLengthFromPostScript(tail)
                footerSource = FooterPrefetchingParquetDataSource.wrapWithPrefetchedTail(dataSource, tail)
            }

            val parquetMetadata: ParquetMetadata = MetadataReader.readFooter(
                    footerSource,
                    parquetReaderOptions,
                    Optional.empty(),
                    Optional.empty())
            val fileMetadata: FileMetadata = parquetMetadata.fileMetaData

            val mapper = DucklakeAddFilesNameMapper(
                    typeConverter,
                    allowMissing,
                    ignoreExtraColumns,
                    hivePartitionValues,
                    filePath,
                    "$schemaName.$tableName")
            val result: DucklakeAddFilesNameMapper.Result
            try {
                result = mapper.map(fileMetadata.schema, allColumns, topLevelColumns)
            }
            catch (e: DucklakeAddFilesException) {
                throw TrinoException(INVALID_PROCEDURE_ARGUMENT, e.message)
            }

            // Convert the trimmed Trino-side ParquetMetadata to the legacy
            // org.apache.parquet.format.FileMetaData thrift shape that our extractor
            // consumes. (The extractor walks RowGroup.columns positionally and decodes
            // min/max bytes against the leaf's Trino target type.)
            val thriftMetadata: org.apache.parquet.format.FileMetaData = toThriftFileMetaData(parquetMetadata)

            // result.leafStatsTargets() lists one entry per matched parquet leaf in file
            // order, with parquetColumnIndex tracking through ignored-extra-columns and
            // hive-partition-overrides so the index stays aligned with RowGroup.columns.
            val stats: List<DucklakeFileColumnStats> = DucklakeStatsExtractor.extractStats(
                    thriftMetadata, result.leafStatsTargets)

            val recordCount = aggregateRecordCount(thriftMetadata)

            val partitionId: OptionalLong = activePartitionSpec.map { spec -> OptionalLong.of(spec.partitionId) }
                    .orElse(OptionalLong.empty())

            val partitionValues: Map<Int, String> = remapPartitionValuesToPartitionKeyIndex(
                    result.partitionValues,
                    activePartitionSpec)

            val nameMap = result.nameMap
            return dev.brikk.ducklake.catalog.DucklakeWriteFragment(
                    filePath,
                    /* pathIsRelative */ false,
                    "parquet",
                    fileSize,
                    footerSize,
                    recordCount,
                    stats,
                    partitionValues,
                    if (partitionId.isPresent) partitionId.asLong else null,
                    nameMap)
        }
        catch (e: IOException) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "Failed to read parquet footer: $filePath", e)
        }
        finally {
            if (dataSource != null) {
                try {
                    dataSource.close()
                }
                catch (ignored: IOException) {
                    // best-effort
                }
            }
        }
    }

    /**
     * Builds a fragment for an externally-written Lance dataset *directory*. Unlike parquet there
     * is no footer to read: the path is registered opaquely (one catalog row per dataset, read via
     * the FileScan `__lance_scan` path), record_count is sourced by counting rows through the same
     * DuckDB executor the read path uses, and file_size is a best-effort sum of the directory's
     * files. No column stats and no name map — Lance reads project columns by name, so the
     * dataset's column names must match the table's (as DuckDB renders them).
     */
    /**
     * Partition values + partition_id for a scan-registered (lance/vortex) file, read from the
     * hive-style `key=value/` path layout. Empty/null when the table is unpartitioned or
     * `hive_partitioning` is off. Identity transforms only — the caller already gated non-identity.
     */
    private fun scanPartitionValues(
            filePath: String,
            activePartitionSpec: Optional<DucklakePartitionSpec>,
            topLevelColumns: List<DucklakeColumn>,
            hivePartitioning: Boolean,
    ): Pair<Map<Int, String?>, Long?> {
        if (!hivePartitioning || activePartitionSpec.isEmpty) {
            return emptyMap<Int, String?>() to null
        }
        val spec = activePartitionSpec.get()
        val hiveValues: Map<String, String> = parseHivePartitions(filePath)
        val columnNameById: Map<Long, String> = topLevelColumns.associate { it.columnId to it.columnName }
        val out: MutableMap<Int, String?> = linkedMapOf()
        for (field in spec.fields) {
            if (field.transform != DucklakePartitionTransform.IDENTITY) {
                continue
            }
            val value: String? = columnNameById[field.columnId]?.let { hiveValues[it] }
            if (value != null) {
                out[field.partitionKeyIndex] = value
            }
        }
        return out to (if (out.isEmpty()) null else spec.partitionId)
    }

    private fun buildLanceFragment(
            fileSystem: TrinoFileSystem,
            filePath: String,
            partitionValues: Map<Int, String?>,
            partitionId: Long?,
            dataColumnHandles: List<DucklakeColumnHandle>,
    ): dev.brikk.ducklake.catalog.DucklakeWriteFragment {
        val recordCount: Long = countRowsViaFileScan(filePath, "__lance_scan", DucklakeSessionProperties.FORMAT_LANCE, dataColumnHandles)
        val fileSize: Long = bestEffortDirectorySize(fileSystem, filePath)
        return dev.brikk.ducklake.catalog.DucklakeWriteFragment(
                filePath,
                /* pathIsRelative */ false,
                DucklakeSessionProperties.FORMAT_LANCE,
                fileSize,
                /* footerSize */ 0L,
                recordCount,
                /* columnStats */ emptyList(),
                partitionValues,
                partitionId,
                /* nameMap */ null)
    }

    /**
     * Builds a fragment for an externally-written single `.vortex` file. Like lance, the
     * registration is opaque: no footer, no column stats, no name map — `read_vortex`
     * projects columns by name, so the file's column names must match the table's.
     * record_count is sourced by scanning the file through the same DuckDB executor the
     * read path uses; file_size comes from the filesystem (vortex is a single file, unlike
     * the lance dataset directory).
     */
    private fun buildVortexFragment(
            fileSystem: TrinoFileSystem,
            filePath: String,
            partitionValues: Map<Int, String?>,
            partitionId: Long?,
            dataColumnHandles: List<DucklakeColumnHandle>,
    ): dev.brikk.ducklake.catalog.DucklakeWriteFragment {
        val fileSize: Long
        try {
            val inputFile: TrinoInputFile = fileSystem.newInputFile(Location.of(filePath))
            if (!inputFile.exists()) {
                throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "File does not exist: $filePath")
            }
            fileSize = inputFile.length()
        }
        catch (e: IOException) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "Failed to open file: $filePath", e)
        }
        val recordCount: Long = countRowsViaFileScan(filePath, "read_vortex", DucklakeSessionProperties.FORMAT_VORTEX, dataColumnHandles)
        return dev.brikk.ducklake.catalog.DucklakeWriteFragment(
                filePath,
                /* pathIsRelative */ false,
                DucklakeSessionProperties.FORMAT_VORTEX,
                fileSize,
                /* footerSize */ 0L,
                recordCount,
                /* columnStats */ emptyList(),
                partitionValues,
                partitionId,
                /* nameMap */ null)
    }

    /**
     * Builds a fragment for an externally-written single DuckDB `.db` file. Like lance/vortex the
     * registration is opaque: no footer, no column stats, no name map — the read path ATTACHes the
     * file READ_ONLY and projects its `main.t` table by column name, so the file's column names must
     * match the table's (as DuckDB renders them). record_count is sourced by scanning the file's
     * `main.t` through the same DuckDB executor the read path uses (ATTACH, not a FileScan table
     * function); file_size comes from the filesystem (duckdb is a single file, like vortex).
     */
    private fun buildDuckDbFragment(
            fileSystem: TrinoFileSystem,
            filePath: String,
            partitionValues: Map<Int, String?>,
            partitionId: Long?,
            dataColumnHandles: List<DucklakeColumnHandle>,
    ): dev.brikk.ducklake.catalog.DucklakeWriteFragment {
        val fileSize: Long
        try {
            val inputFile: TrinoInputFile = fileSystem.newInputFile(Location.of(filePath))
            if (!inputFile.exists()) {
                throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "File does not exist: $filePath")
            }
            fileSize = inputFile.length()
        }
        catch (e: IOException) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "Failed to open file: $filePath", e)
        }
        val recordCount: Long = countRowsViaAttach(filePath, dataColumnHandles)
        return dev.brikk.ducklake.catalog.DucklakeWriteFragment(
                filePath,
                /* pathIsRelative */ false,
                DucklakeSessionProperties.FORMAT_DUCKDB,
                fileSize,
                /* footerSize */ 0L,
                recordCount,
                /* columnStats */ emptyList(),
                partitionValues,
                partitionId,
                /* nameMap */ null)
    }

    /**
     * Counts the rows of a scan-registered dataset (lance directory / vortex file) by scanning
     * it through the DuckDB executor (the same engine the read path uses), reusing the FileScan
     * source. Doubles as a readability/existence check — a missing or unreadable dataset
     * surfaces as an error here.
     */
    private fun countRowsViaFileScan(
            url: String,
            scanFunction: String,
            extension: String,
            projection: List<DucklakeColumnHandle>,
    ): Long {
        // No DuckDbS3Config even for s3:// — both extensions bind object_store credentials from
        // process AWS_* env, never DuckDB secrets (HANDOFF O1), so the httpfs + secret setup the
        // config triggers is pure overhead (and a concurrent-CREATE race on the Quack engine).
        val target = DuckDbAttachTarget.FileScan(url, scanFunction, extension, null)
        return countRows(target, extension, url, projection)
    }

    /**
     * Counts the rows of a scan-registered DuckDB `.db` file by ATTACHing it READ_ONLY through the
     * DuckDB executor (the same engine + attach target the read path uses) and scanning its
     * `main.t` table. Local paths attach directly; `s3://` URLs attach through httpfs with the
     * catalog's `ducklake_s3` secret (unlike lance/vortex, a `.db` ATTACH reads through DuckDB's own
     * filesystem layer, which DOES honor the secret — see [DuckDbAttachTarget]). Doubles as a
     * readability/existence check.
     */
    private fun countRowsViaAttach(url: String, projection: List<DucklakeColumnHandle>): Long {
        val target: DuckDbAttachTarget = if (isS3(url)) {
            DuckDbAttachTarget.HttpfsS3(url, duckDbS3Config)
        }
        else {
            DuckDbAttachTarget.LocalPath(java.nio.file.Path.of(stripFileScheme(url)))
        }
        return countRows(target, DucklakeSessionProperties.FORMAT_DUCKDB, url, projection)
    }

    /**
     * Scans [target] through the DuckDB executor and returns the total row count — AND validates
     * the source schema against the table by [projection]. Projecting the table's data columns by
     * name makes a missing/renamed column fail (DuckDB "column not found"); converting the first
     * batch through the read path's Arrow→page converter makes an incompatibly-typed column fail.
     * Both surface here as a clear `INVALID_PROCEDURE_ARGUMENT` BEFORE `commitAddFiles`, instead of
     * committing a catalog entry that only errors at first SELECT. An empty [projection] (a table
     * with only partition columns) falls back to a count-only `SELECT 1` scan.
     */
    private fun countRows(
            target: DuckDbAttachTarget,
            label: String,
            url: String,
            projection: List<DucklakeColumnHandle>,
    ): Long {
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                target, projection, TupleDomain.all<DucklakeColumnHandle>())
        // Reuse the read path's converter to type-check the projected columns (positional, by the
        // requested types). Only the first batch is converted — the Arrow schema is fixed per scan.
        val converter: DucklakeArrowToPageConverter? =
                if (projection.isEmpty()) null else DucklakeArrowToPageConverter(projection.map { it.columnType })
        var count = 0L
        try {
            executorFactory.create().execute(request).use { ctx ->
                val reader = ctx.arrowReader()
                var validated = false
                while (reader.loadNextBatch()) {
                    val root = reader.vectorSchemaRoot
                    count += root.rowCount.toLong()
                    if (!validated) {
                        converter?.convert(root)
                        validated = true
                    }
                }
            }
        }
        catch (e: SQLException) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT,
                    "Failed to read $label dataset (does its schema match the table?): $url", e)
        }
        catch (e: IOException) {
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "Failed to read $label dataset: $url", e)
        }
        catch (e: RuntimeException) {
            // Arrow→page conversion of a present-but-incompatibly-typed column throws here.
            throw TrinoException(INVALID_PROCEDURE_ARGUMENT,
                    "$label dataset schema is incompatible with the table (column type mismatch): $url", e)
        }
        return count
    }

    /** Best-effort sum of a dataset directory's file sizes; 0 if it cannot be listed. */
    private fun bestEffortDirectorySize(fileSystem: TrinoFileSystem, dirPath: String): Long {
        return try {
            var total = 0L
            val iterator = fileSystem.listFiles(Location.of(dirPath))
            while (iterator.hasNext()) {
                total += iterator.next().length()
            }
            total
        }
        catch (_: IOException) {
            0L
        }
    }

    private fun remapPartitionValuesToPartitionKeyIndex(
            byFieldId: Map<Int, String>,
            activePartitionSpec: Optional<DucklakePartitionSpec>,
    ): Map<Int, String> {
        if (byFieldId.isEmpty()) {
            return mapOf()
        }
        if (activePartitionSpec.isEmpty) {
            // Path looks partitioned but table isn't — upstream silently ignores;
            // mirror that to avoid breaking already-deployed warehouses where folks
            // happen to lay parquet under key=value/ without a partition spec.
            return mapOf()
        }
        val out: MutableMap<Int, String> = linkedMapOf()
        for (field in activePartitionSpec.get().fields) {
            val value = byFieldId[field.columnId.toInt()]
            if (value != null) {
                out[field.partitionKeyIndex] = value
            }
        }
        return out
    }

    companion object {
        private const val FORMAT_PARQUET: String = "parquet"

        private val ADD_FILES: MethodHandle

        init {
            try {
                ADD_FILES = MethodHandles.lookup().findVirtual(
                        DucklakeAddFilesProcedure::class.java,
                        "addFiles",
                        MethodType.methodType(
                                Void.TYPE,
                                ConnectorSession::class.java,
                                String::class.java,
                                String::class.java,
                                List::class.java,
                                java.lang.Boolean.TYPE,
                                java.lang.Boolean.TYPE,
                                java.lang.Boolean.TYPE,
                                String::class.java))
            }
            catch (e: ReflectiveOperationException) {
                throw AssertionError(e)
            }
        }

        private fun aggregateRecordCount(thriftMetadata: org.apache.parquet.format.FileMetaData): Long =
            thriftMetadata.getNum_rows()

        /** Mirrors the read path's s3-scheme detection (DucklakePageSourceProvider). */
        private fun isS3(url: String): Boolean =
            url.startsWith("s3://") || url.startsWith("s3a://") || url.startsWith("s3n://")

        /** Strip a `file://` scheme so DuckDB's ATTACH gets a bare local path. */
        private fun stripFileScheme(url: String): String =
            if (url.startsWith("file://")) url.removePrefix("file://") else url

        // Post-script size: 4-byte LE FileMetaData length + 4-byte magic. Mirrors Trino's
        // MetadataReader.POST_SCRIPT_SIZE.
        private const val POST_SCRIPT_SIZE: Int = 8

        /**
         * Read the 4-byte little-endian Thrift FileMetaData length from the parquet
         * post-script at the end of an already-read file tail. The trailer layout per the
         * parquet spec:
         *
         * <pre>
         *   [ Thrift FileMetaData ][ 4 bytes LE footer length ][ 4 bytes "PAR1" magic ]
         * </pre>
         *
         * This value is what DuckLake stores in `ducklake_data_file.footer_size`
         * (matches the existing [FooterPrefetchingParquetDataSource] contract:
         * footer_size is the Thrift FileMetaData length only, excluding the 8-byte
         * post-script). The `tail` must be a genuine file-end slice — the post-script
         * is its last 8 bytes.
         *
         * Best-effort: any short slice, non-magic trailer, or parse error returns 0, in
         * which case the read path falls back to its default blind tail read — i.e.
         * the previous behavior. Correctness is unaffected.
         */
        private fun footerLengthFromPostScript(tail: io.airlift.slice.Slice): Long {
            try {
                if (tail.length() < POST_SCRIPT_SIZE) {
                    return 0
                }
                val base = tail.length() - POST_SCRIPT_SIZE
                // Magic bytes at the last 4 bytes of the tail: encrypted parquet uses "PARE"
                // and the encrypted footer length sits in the same 4-byte LE slot, so we
                // accept either marker. Anything else means this isn't a valid trailer.
                val b4 = tail.getByte(base + 4)
                val b5 = tail.getByte(base + 5)
                val b6 = tail.getByte(base + 6)
                val b7 = tail.getByte(base + 7)
                val isParquetMagic = b4 == 'P'.code.toByte() && b5 == 'A'.code.toByte() && b6 == 'R'.code.toByte() && (b7 == '1'.code.toByte() || b7 == 'E'.code.toByte())
                if (!isParquetMagic) {
                    return 0
                }
                // Airlift Slice is little-endian by contract, so getInt is already LE.
                val footerLength = tail.getInt(base)
                return if (footerLength >= 0) footerLength.toLong() else 0
            }
            catch (_: RuntimeException) {
                return 0
            }
        }

        /**
         * Parse `key=value` segments out of a file path (hive-style layout).
         * URL-decoded values are returned as their raw text — upstream casts to the
         * partition column's type at read time; we forward strings, matching today's
         * connector convention.
         */
        private fun parseHivePartitions(path: String): Map<String, String> {
            val out: MutableMap<String, String> = linkedMapOf()
            val normalized = path.replace('\\', '/')
            // Strip filename so we don't accidentally split on a "=" in the basename.
            val lastSlash = normalized.lastIndexOf('/')
            val dirs = if (lastSlash < 0) "" else normalized.substring(0, lastSlash)
            for (segment in dirs.split("/")) {
                val eq = segment.indexOf('=')
                if (eq > 0 && eq < segment.length - 1) {
                    val key = segment.substring(0, eq)
                    val value = segment.substring(eq + 1)
                    if (!key.isEmpty()) {
                        out[key] = value
                    }
                }
            }
            return out
        }

        private fun extractStringArray(values: List<*>): List<String> {
            val out: MutableList<String> = ArrayList(values.size)
            for (value in values) {
                if (value == null) {
                    throw TrinoException(INVALID_PROCEDURE_ARGUMENT, "files must not contain NULL entries")
                }
                // Procedure runtime delivers VARCHAR array entries as Slice in some
                // Trino versions and String in others; tolerate both.
                if (value is io.airlift.slice.Slice) {
                    out.add(value.toStringUtf8())
                }
                else if (value is String) {
                    out.add(value)
                }
                else {
                    throw TrinoException(INVALID_PROCEDURE_ARGUMENT,
                            "files entry must be VARCHAR, got ${value.javaClass.name}")
                }
            }
            return out
        }

        /**
         * Adapter from Trino's [ParquetMetadata] to the legacy thrift
         * [org.apache.parquet.format.FileMetaData] consumed by
         * [DucklakeStatsExtractor]. Trino's view exposes the per-row-group block
         * metadata but the existing extractor needs the thrift form; this rebuilds the
         * pieces the extractor actually reads (row_groups[].columns[].meta_data with
         * statistics) and leaves unused fields default-initialized.
         */
        @Throws(IOException::class)
        private fun toThriftFileMetaData(metadata: ParquetMetadata): org.apache.parquet.format.FileMetaData {
            val thrift = org.apache.parquet.format.FileMetaData()
            var numRows: Long = 0
            val rowGroups: MutableList<org.apache.parquet.format.RowGroup> = mutableListOf()
            for (block in metadata.blocks) {
                val rg = org.apache.parquet.format.RowGroup()
                rg.setNum_rows(block.rowCount())
                val chunks: MutableList<org.apache.parquet.format.ColumnChunk> = mutableListOf()
                for (column in block.columns()) {
                    val chunk = org.apache.parquet.format.ColumnChunk()
                    val meta = org.apache.parquet.format.ColumnMetaData()
                    meta.setNum_values(column.valueCount)
                    meta.setTotal_compressed_size(column.totalSize)
                    val nativeStats: org.apache.parquet.column.statistics.Statistics<*>? = column.statistics
                    if (nativeStats != null) {
                        val statistics = org.apache.parquet.format.Statistics()
                        if (!nativeStats.isEmpty) {
                            val minBytes = nativeStats.minBytes
                            val maxBytes = nativeStats.maxBytes
                            if (minBytes != null) {
                                statistics.setMin_value(minBytes)
                            }
                            if (maxBytes != null) {
                                statistics.setMax_value(maxBytes)
                            }
                        }
                        if (nativeStats.numNulls >= 0) {
                            statistics.setNull_count(nativeStats.numNulls)
                        }
                        meta.setStatistics(statistics)
                    }
                    chunk.setMeta_data(meta)
                    chunks.add(chunk)
                }
                rg.setColumns(chunks)
                rowGroups.add(rg)
                numRows += block.rowCount()
            }
            thrift.setRow_groups(rowGroups)
            thrift.setNum_rows(numRows)
            return thrift
        }
    }
}
