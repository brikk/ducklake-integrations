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

import com.fasterxml.uuid.Generators
import com.fasterxml.uuid.impl.TimeBasedEpochGenerator
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import dev.brikk.ducklake.catalog.SnapshotRange.activeAt
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_COLUMN
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_COLUMN_MAPPING
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DATA_FILE
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DELETE_FILE
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_FILE_COLUMN_STATS
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_FILE_PARTITION_VALUE
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_INLINED_DATA_TABLES
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_METADATA
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_NAME_MAPPING
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_PARTITION_COLUMN
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_PARTITION_INFO
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SCHEMA
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SCHEMA_VERSIONS
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SNAPSHOT
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SNAPSHOT_CHANGES
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SORT_EXPRESSION
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SORT_INFO
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE_COLUMN_STATS
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE_STATS
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_VIEW
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeColumnMappingRecord
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeColumnRecord
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeDataFileRecord
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeDeleteFileRecord
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeFileColumnStatsRecord
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeFilePartitionValueRecord
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeNameMappingRecord
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeSchemaRecord
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeSnapshotChangesRecord
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeSnapshotRecord
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeTableColumnStatsRecord
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeTableRecord
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeTableStatsRecord
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeViewRecord
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.Field
import org.jooq.Param
import org.jooq.Record
import org.jooq.SQLDialect
import org.jooq.Table
import org.jooq.conf.RenderQuotedNames
import org.jooq.conf.Settings
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.jooq.tools.jdbc.JDBCUtils
import java.sql.Connection
import java.sql.SQLException
import java.time.Instant
import java.util.HashMap
import java.util.HashSet
import java.util.LinkedHashMap
import java.util.Locale
import java.util.Optional
import java.util.OptionalInt
import java.util.OptionalLong
import java.util.TreeSet
import java.util.UUID
import javax.sql.DataSource

/**
 * JDBC implementation of DucklakeCatalog.
 * Queries the Ducklake metadata tables via JDBC.
 */
class JdbcDucklakeCatalog(config: DucklakeCatalogConfig) : DucklakeCatalog {

    private val dataSource: DataSource
    private val hikariDataSource: HikariDataSource
    private val dialect: SQLDialect
    private val jooqSettings: Settings
    private val dsl: DSLContext
    private val metadata: MetadataQuery

    init {
        @Suppress("SENSELESS_COMPARISON")
        if (config == null) throw NullPointerException("config is null")

        val hikariConfig = HikariConfig()
        val configuredUrl: String = config.getCatalogDatabaseUrl()!!
        val dialectInferenceUrl: String
        val metadataQuery: MetadataQuery
        if (QuackBackedDuckDbCatalogUrl.matches(configuredUrl)) {
            // Synthetic URL — `jdbc:duckdb:quack://host:port[?metadata_catalog=name]`.
            // Open a plain in-memory DuckDB JDBC connection and let HikariCP run a
            // per-connection init script that loads quack + ducklake, creates the
            // Quack auth secret, ATTACHes the remote DuckLake catalog with a
            // METADATA_CATALOG name, and USEs that catalog's main schema. After init,
            // bare references to `ducklake_*` tables resolve directly to the remote
            // metadata storage — JdbcDucklakeCatalog's jOOQ DSL stays unchanged.
            val quack = QuackBackedDuckDbCatalogUrl.parse(
                configuredUrl, config.getCatalogDatabasePassword(), config.getDataPath())
            hikariConfig.jdbcUrl = QuackBackedDuckDbCatalogUrl.UNDERLYING_JDBC_URL
            hikariConfig.connectionInitSql = quack.connectionInitSql()
            // The user/password slots aren't used at the JDBC layer for this backend;
            // the token is interpolated into the CREATE SECRET statement inside the
            // init script.
            dialectInferenceUrl = QuackBackedDuckDbCatalogUrl.UNDERLYING_JDBC_URL
            // Quack RPC's optimizer rejects SQL shapes that the local DuckDB binder
            // happily plans against the Quack-attached metadata catalog (same-table
            // multi-scan, multi-table JOINs). Route the call sites that hit those
            // shapes through quack_query_by_name so the inner SQL is executed
            // server-side as a single LogicalGet from the local plan's POV.
            metadataQuery = QuackWrappedMetadataQuery(quack.metadataCatalog())
        }
        else {
            hikariConfig.jdbcUrl = configuredUrl
            if (config.getCatalogDatabaseUser() != null) {
                hikariConfig.username = config.getCatalogDatabaseUser()
            }
            if (config.getCatalogDatabasePassword() != null) {
                hikariConfig.password = config.getCatalogDatabasePassword()
            }
            dialectInferenceUrl = configuredUrl
            metadataQuery = DirectMetadataQuery()
        }
        hikariConfig.maximumPoolSize = config.getMaxCatalogConnections()
        hikariConfig.minimumIdle = 1
        hikariConfig.connectionTimeout = 30000

        this.hikariDataSource = HikariDataSource(hikariConfig)
        this.dataSource = hikariDataSource

        // Infer dialect from the JDBC URL. JDBCUtils.dialect() returns SQLDialect.DEFAULT for
        // backends jOOQ OSS doesn't recognize, which keeps query rendering portable.
        this.dialect = JDBCUtils.dialect(dialectInferenceUrl)
        this.jooqSettings = Settings()
            // The generated DuckLake tables use lowercase unquoted identifiers. Quoting is
            // only needed on identifiers that collide with reserved words (none in the
            // ducklake_* schema today) — leaving it off keeps queries readable in logs.
            .withRenderQuotedNames(RenderQuotedNames.EXPLICIT_DEFAULT_UNQUOTED)
            // jOOQ's codegen runs against Postgres, so generated Table<?> references
            // hardcode the `public` schema prefix. PG's default `search_path` would pick
            // unqualified references up anyway; on the Quack-backed DuckDB path the
            // metadata lives at <metadata_catalog>.main and the per-connection
            // `USE <metadata_catalog>.main` makes unqualified resolution work. Stripping
            // the rendered schema works for both — but is required for the latter.
            .withRenderSchema(false)
        this.dsl = DSL.using(dataSource, dialect, jooqSettings)
        this.metadata = metadataQuery

        log.log(
            System.Logger.Level.INFO,
            "Initialized Ducklake JDBC catalog: {0} (jOOQ dialect: {1})",
            config.getCatalogDatabaseUrl(), dialect)
    }

    /**
     * Returns a [DSLContext] scoped to a caller-supplied [Connection].
     * Writes routed through this context run on the caller's transaction rather
     * than checking out a fresh pool connection, so commits and rollbacks on the
     * caller's connection apply to every jOOQ-issued statement.
     *
     * Used by [DucklakeWriteTransaction] to keep all mutations on a
     * single transactional connection.
     */
    // Visibility note: package-private in Java; widened to public (rather than
    // `internal`) to preserve JVM call-compatibility with same-package callers.
    fun forConnection(connection: Connection): DSLContext {
        return DSL.using(connection, dialect, jooqSettings)
    }

    override val currentSnapshotId: Long
        get() {
            val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
            val maxId: Long? = dsl.select(DSL.max(snap.SNAPSHOT_ID))
                .from(snap)
                .fetchOne(0, Long::class.java)
            return maxId ?: throw IllegalStateException("No snapshots found in ducklake_snapshot table")
        }

    override fun getSnapshot(snapshotId: Long): Optional<DucklakeSnapshot> {
        val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
        return dsl.selectFrom(snap)
            .where(snap.SNAPSHOT_ID.eq(snapshotId))
            .fetchOptional()
            .map { toDucklakeSnapshot(it) }
    }

    // TODO(review:after id=eff-snapshot-at-or-before-full-scan): materializes the entire snapshot table to find one row
    override fun getSnapshotAtOrBefore(timestamp: Instant): Optional<DucklakeSnapshot> {
        val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
        return dsl.selectFrom(snap)
            .orderBy(snap.SNAPSHOT_ID.desc())
            .fetch { toDucklakeSnapshot(it) }
            .stream()
            .filter { snapshot -> !snapshot.snapshotTime.isAfter(timestamp) }
            .findFirst()
    }

    override fun listSnapshots(): List<DucklakeSnapshot> {
        val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
        return dsl.selectFrom(snap)
            .orderBy(snap.SNAPSHOT_ID.desc())
            .fetch { toDucklakeSnapshot(it) }
    }

    override fun listSnapshotChanges(): List<DucklakeSnapshotChange> {
        val snapchg = DUCKLAKE_SNAPSHOT_CHANGES.`as`("snapchg")
        return dsl.selectFrom(snapchg)
            .orderBy(snapchg.SNAPSHOT_ID.desc())
            .fetch { toDucklakeSnapshotChange(it) }
    }

    override fun listSchemas(snapshotId: Long): List<DucklakeSchema> {
        val sch = DUCKLAKE_SCHEMA.`as`("sch")
        return dsl.selectFrom(sch)
            .where(activeAt(sch, snapshotId))
            .fetch { toDucklakeSchema(it) }
    }

    override fun getSchema(schemaName: String, snapshotId: Long): Optional<DucklakeSchema> {
        val sch = DUCKLAKE_SCHEMA.`as`("sch")
        return dsl.selectFrom(sch)
            .where(sch.SCHEMA_NAME.eq(schemaName))
            .and(activeAt(sch, snapshotId))
            .fetchOptional()
            .map { toDucklakeSchema(it) }
    }

    override fun listTables(schemaId: Long, snapshotId: Long): List<DucklakeTable> {
        val tab = DUCKLAKE_TABLE.`as`("tab")
        return dsl.selectFrom(tab)
            .where(tab.SCHEMA_ID.eq(schemaId))
            .and(activeAt(tab, snapshotId))
            .fetch { toDucklakeTable(it) }
    }

    override fun getTable(schemaName: String, tableName: String, snapshotId: Long): Optional<DucklakeTable> {
        val schema = getSchema(schemaName, snapshotId)
        if (schema.isEmpty) {
            return Optional.empty()
        }

        val tab = DUCKLAKE_TABLE.`as`("tab")
        return dsl.selectFrom(tab)
            .where(tab.SCHEMA_ID.eq(schema.get().schemaId))
            .and(tab.TABLE_NAME.eq(tableName))
            .and(activeAt(tab, snapshotId))
            .fetchOptional()
            .map { toDucklakeTable(it) }
    }

    override fun getTableById(tableId: Long, snapshotId: Long): Optional<DucklakeTable> {
        val tab = DUCKLAKE_TABLE.`as`("tab")
        return dsl.selectFrom(tab)
            .where(tab.TABLE_ID.eq(tableId))
            .and(activeAt(tab, snapshotId))
            .fetchOptional()
            .map { toDucklakeTable(it) }
    }

    override fun getTableColumns(tableId: Long, snapshotId: Long): List<DucklakeColumn> {
        val allColumns = fetchTableColumns(tableId, snapshotId)

        val childrenByParent: MutableMap<Long, MutableList<DucklakeColumn>> = HashMap()
        for (column in allColumns) {
            column.parentColumn.ifPresent { parent ->
                childrenByParent.getOrPut(parent) { ArrayList() }.add(column)
            }
        }

        val topLevelColumns: MutableList<DucklakeColumn> = ArrayList()
        for (column in allColumns) {
            if (column.parentColumn.isEmpty) {
                topLevelColumns.add(
                    DucklakeColumn(
                        column.columnId,
                        column.beginSnapshot,
                        column.endSnapshot,
                        column.tableId,
                        column.columnOrder,
                        column.columnName,
                        resolveColumnType(column, childrenByParent),
                        column.nullsAllowed,
                        Optional.empty(),
                    ),
                )
            }
        }

        return topLevelColumns
    }

    override fun getAllColumnsWithParentage(tableId: Long, snapshotId: Long): List<DucklakeColumn> {
        return fetchTableColumns(tableId, snapshotId)
    }

    private fun fetchTableColumns(tableId: Long, snapshotId: Long): List<DucklakeColumn> {
        val col = DUCKLAKE_COLUMN.`as`("col")
        return dsl.selectFrom(col)
            .where(col.TABLE_ID.eq(tableId))
            .and(activeAt(col, snapshotId))
            .orderBy(col.COLUMN_ORDER, col.COLUMN_ID)
            .fetch { toDucklakeColumn(it) }
    }

    override fun getDataFiles(tableId: Long, snapshotId: Long): List<DucklakeDataFile> {
        val file = DUCKLAKE_DATA_FILE.`as`("file")
        val delfile = DUCKLAKE_DELETE_FILE.`as`("delfile")
        // PATH / PATH_IS_RELATIVE / FOOTER_SIZE exist on BOTH sides of the LEFT
        // JOIN. jOOQ's Record field lookup is name-based, so without explicit
        // aliases the second occurrence of each name resolves to the first
        // column with that name — silently returning the data-file's PATH for
        // r.get(delfile.PATH) etc. This was invisible on PG (which renders
        // qualifier-aware result-set metadata) but surfaced under the Quack
        // wrapper's coerce step, where the resulting Records carry only the
        // coerced Fields and name collisions become unambiguous wrong-result
        // bugs. Alias the duplicates so each projected Field has a unique
        // name; the mapper accesses through these aliased Field locals.
        val dataFilePath = file.PATH.`as`("data_file_path")
        val dataFilePathIsRelative = file.PATH_IS_RELATIVE.`as`("data_file_path_is_relative")
        val dataFileFooterSize = file.FOOTER_SIZE.`as`("data_file_footer_size")
        val deleteFilePath = delfile.PATH.`as`("delete_file_path")
        val deleteFilePathIsRelative = delfile.PATH_IS_RELATIVE.`as`("delete_file_path_is_relative")
        val deleteFileFooterSize = delfile.FOOTER_SIZE.`as`("delete_file_footer_size")
        // Multi-table JOIN — routed through `metadata` for Quack compatibility.
        return metadata.fetch(
            dsl,
            dsl.select(
                file.DATA_FILE_ID,
                file.TABLE_ID,
                file.BEGIN_SNAPSHOT,
                file.END_SNAPSHOT,
                file.FILE_ORDER,
                dataFilePath,
                dataFilePathIsRelative,
                file.FILE_FORMAT,
                file.RECORD_COUNT,
                file.FILE_SIZE_BYTES,
                dataFileFooterSize,
                file.ROW_ID_START,
                file.PARTITION_ID,
                file.MAPPING_ID,
                deleteFilePath,
                deleteFilePathIsRelative,
                deleteFileFooterSize,
                delfile.FORMAT,
            )
                .from(file)
                .leftJoin(delfile)
                .on(file.DATA_FILE_ID.eq(delfile.DATA_FILE_ID))
                .and(activeAt(delfile, snapshotId))
                .where(file.TABLE_ID.eq(tableId))
                .and(activeAt(file, snapshotId))
                .orderBy(file.FILE_ORDER),
        ) { r ->
            DucklakeDataFile(
                orZero(r.get(file.DATA_FILE_ID)),
                orZero(r.get(file.TABLE_ID)),
                orZero(r.get(file.BEGIN_SNAPSHOT)),
                Optional.ofNullable(r.get(file.END_SNAPSHOT)),
                orZero(r.get(file.FILE_ORDER)),
                r.get(dataFilePath),
                java.lang.Boolean.TRUE == r.get(dataFilePathIsRelative),
                r.get(file.FILE_FORMAT),
                orZero(r.get(file.RECORD_COUNT)),
                orZero(r.get(file.FILE_SIZE_BYTES)),
                orZero(r.get(dataFileFooterSize)),
                orZero(r.get(file.ROW_ID_START)),
                Optional.ofNullable(r.get(file.PARTITION_ID)),
                Optional.ofNullable(r.get(deleteFilePath)),
                Optional.ofNullable(r.get(deleteFilePathIsRelative)),
                Optional.ofNullable(r.get(deleteFileFooterSize)),
                Optional.ofNullable(r.get(delfile.FORMAT)),
                Optional.ofNullable(r.get(file.MAPPING_ID)),
            )
        }
    }

    override fun getLatestDataFileFormat(tableId: Long, snapshotId: Long): Optional<String> {
        // "Latest" = highest data_file_id among rows still active at the requested snapshot.
        // data_file_id is allocated from a monotonic catalog sequence at insert time, so DESC
        // order on it picks the most recently committed file (cross-snapshot, cross-partition).
        val file = DUCKLAKE_DATA_FILE.`as`("file")
        return Optional.ofNullable(
            dsl.select(file.FILE_FORMAT)
                .from(file)
                .where(file.TABLE_ID.eq(tableId))
                .and(activeAt(file, snapshotId))
                .orderBy(file.DATA_FILE_ID.desc())
                .limit(1)
                .fetchOne(file.FILE_FORMAT),
        )
    }

    override fun findDataFileIdsInRange(tableId: Long, snapshotId: Long, predicate: ColumnRangePredicate): List<Long> {
        val columnId = predicate.columnId

        val col = DUCKLAKE_COLUMN.`as`("col")
        val columnType: String? = dsl.select(col.COLUMN_TYPE)
            .from(col)
            .where(col.TABLE_ID.eq(tableId))
            .and(col.COLUMN_ID.eq(columnId))
            .and(activeAt(col, snapshotId))
            .fetchOne(col.COLUMN_TYPE)
        if (columnType == null) {
            return emptyList()
        }

        val lowerBound: Comparable<*>? = parseStatValue(columnType, predicate.minValue)
        val upperBound: Comparable<*>? = parseStatValue(columnType, predicate.maxValue)

        val colstats = DUCKLAKE_FILE_COLUMN_STATS.`as`("colstats")
        val file = DUCKLAKE_DATA_FILE.`as`("file")
        // Multi-table JOIN — routed through `metadata` for Quack compatibility.
        return metadata.fetch(
            dsl,
            dsl.select(
                colstats.DATA_FILE_ID,
                colstats.MIN_VALUE,
                colstats.MAX_VALUE,
            )
                .from(colstats)
                .innerJoin(file)
                .on(colstats.DATA_FILE_ID.eq(file.DATA_FILE_ID))
                .where(colstats.TABLE_ID.eq(tableId))
                .and(colstats.COLUMN_ID.eq(columnId))
                .and(file.TABLE_ID.eq(tableId))
                .and(activeAt(file, snapshotId)),
        ) { r -> r }
            .asSequence()
            .filter { r ->
                isWithinBounds(
                    lowerBound,
                    upperBound,
                    parseStatValue(columnType, r.get(colstats.MIN_VALUE)),
                    parseStatValue(columnType, r.get(colstats.MAX_VALUE)),
                )
            }
            .map { r -> r.get(colstats.DATA_FILE_ID) }
            .toList()
    }

    override fun getTableStats(tableId: Long): Optional<DucklakeTableStats> {
        val tabstats = DUCKLAKE_TABLE_STATS.`as`("tabstats")
        return dsl.selectFrom(tabstats)
            .where(tabstats.TABLE_ID.eq(tableId))
            .fetchOptional()
            .map { toDucklakeTableStats(it) }
    }

    override fun getColumnStats(tableId: Long, snapshotId: Long): List<DucklakeColumnStats> {
        val columnTypes: Map<Long, String> = getTableColumns(tableId, snapshotId)
            .associate { it.columnId to it.columnType }

        val countAccumulators: MutableMap<Long, LongArray> = HashMap() // [valueCount, nullCount, sizeBytes]
        val minAccumulators: MutableMap<Long, String> = HashMap()
        val maxAccumulators: MutableMap<Long, String> = HashMap()

        val colstats = DUCKLAKE_FILE_COLUMN_STATS.`as`("colstats")
        val file = DUCKLAKE_DATA_FILE.`as`("file")
        // Multi-table JOIN — routed through `metadata` for Quack compatibility.
        metadata.fetch(
            dsl,
            dsl.select(
                colstats.COLUMN_ID,
                colstats.VALUE_COUNT,
                colstats.NULL_COUNT,
                colstats.COLUMN_SIZE_BYTES,
                colstats.MIN_VALUE,
                colstats.MAX_VALUE,
            )
                .from(colstats)
                .innerJoin(file)
                .on(colstats.DATA_FILE_ID.eq(file.DATA_FILE_ID))
                .where(colstats.TABLE_ID.eq(tableId))
                .and(file.TABLE_ID.eq(tableId))
                .and(activeAt(file, snapshotId)),
        ) { r ->
            val columnId = orZero(r.get(colstats.COLUMN_ID))
            val counts = countAccumulators.getOrPut(columnId) { LongArray(3) }
            counts[0] += orZero(r.get(colstats.VALUE_COUNT))
            counts[1] += orZero(r.get(colstats.NULL_COUNT))
            counts[2] += orZero(r.get(colstats.COLUMN_SIZE_BYTES))

            val columnType = columnTypes.getOrDefault(columnId, "")
            val minValue = r.get(colstats.MIN_VALUE)
            val maxValue = r.get(colstats.MAX_VALUE)
            if (minValue != null) {
                minAccumulators.merge(columnId, minValue) { a, b -> typedMin(a, b, columnType) }
            }
            if (maxValue != null) {
                maxAccumulators.merge(columnId, maxValue) { a, b -> typedMax(a, b, columnType) }
            }
            null
        }

        val result: MutableList<DucklakeColumnStats> = ArrayList()
        for ((columnId, counts) in countAccumulators) {
            result.add(
                DucklakeColumnStats(
                    columnId,
                    counts[0],
                    counts[1],
                    counts[2],
                    Optional.ofNullable(minAccumulators[columnId]),
                    Optional.ofNullable(maxAccumulators[columnId]),
                ),
            )
        }

        return result
    }

    override fun getPartitionSpecs(tableId: Long, snapshotId: Long): List<DucklakePartitionSpec> {
        val fieldsByPartition: MutableMap<Long, MutableList<DucklakePartitionField>> = LinkedHashMap()
        val tableIdByPartition: MutableMap<Long, Long> = HashMap()

        val partinfo = DUCKLAKE_PARTITION_INFO.`as`("partinfo")
        val partcol = DUCKLAKE_PARTITION_COLUMN.`as`("partcol")
        // Multi-table JOIN — routed through `metadata` for Quack compatibility.
        metadata.fetch(
            dsl,
            dsl.select(
                partinfo.PARTITION_ID,
                partinfo.TABLE_ID,
                partcol.PARTITION_KEY_INDEX,
                partcol.COLUMN_ID,
                partcol.TRANSFORM,
            )
                .from(partinfo)
                .innerJoin(partcol)
                .on(partinfo.PARTITION_ID.eq(partcol.PARTITION_ID))
                .and(partinfo.TABLE_ID.eq(partcol.TABLE_ID))
                .where(partinfo.TABLE_ID.eq(tableId))
                .and(activeAt(partinfo, snapshotId))
                .orderBy(partinfo.PARTITION_ID, partcol.PARTITION_KEY_INDEX),
        ) { r ->
            val partitionId = orZero(r.get(partinfo.PARTITION_ID))
            tableIdByPartition[partitionId] = orZero(r.get(partinfo.TABLE_ID))
            val parsed = DucklakePartitionTransform.parseCatalogTransform(r.get(partcol.TRANSFORM))
            fieldsByPartition.getOrPut(partitionId) { ArrayList() }
                .add(
                    DucklakePartitionField(
                        orZero(r.get(partcol.PARTITION_KEY_INDEX)).toInt(),
                        orZero(r.get(partcol.COLUMN_ID)),
                        parsed.transform,
                        parsed.arity,
                    ),
                )
            null // mapper return discarded — using fold-into-maps idiom
        }

        val specs: MutableList<DucklakePartitionSpec> = ArrayList()
        for ((partitionId, fields) in fieldsByPartition) {
            specs.add(DucklakePartitionSpec(partitionId, tableIdByPartition[partitionId]!!, fields))
        }
        return specs
    }

    override fun getSortKeys(tableId: Long, snapshotId: Long): List<DucklakeSortKey> {
        val sortinfo = DUCKLAKE_SORT_INFO.`as`("sortinfo")
        val sortexpr = DUCKLAKE_SORT_EXPRESSION.`as`("sortexpr")
        // Multi-table JOIN — routed through `metadata` so the Quack RPC
        // optimizer's multi-streaming-scan check doesn't fire. Pass-through
        // on PG / local DuckDB.
        return metadata.fetch(
            dsl,
            dsl.select(
                sortexpr.SORT_KEY_INDEX,
                sortexpr.EXPRESSION,
                sortexpr.DIALECT,
                sortexpr.SORT_DIRECTION,
                sortexpr.NULL_ORDER,
            )
                .from(sortinfo)
                .innerJoin(sortexpr)
                .on(sortinfo.SORT_ID.eq(sortexpr.SORT_ID))
                .and(sortinfo.TABLE_ID.eq(sortexpr.TABLE_ID))
                .where(sortinfo.TABLE_ID.eq(tableId))
                .and(activeAt(sortinfo, snapshotId))
                .orderBy(sortexpr.SORT_KEY_INDEX),
        ) { r ->
            DucklakeSortKey(
                orZero(r.get(sortexpr.SORT_KEY_INDEX)).toInt(),
                r.get(sortexpr.EXPRESSION),
                r.get(sortexpr.DIALECT),
                DucklakeSortDirection.fromCatalog(r.get(sortexpr.SORT_DIRECTION)),
                DucklakeNullOrder.fromCatalog(r.get(sortexpr.NULL_ORDER)),
            )
        }
    }

    override fun getFilePartitionValues(tableId: Long, snapshotId: Long): Map<Long, List<DucklakeFilePartitionValue>> {
        val result: MutableMap<Long, MutableList<DucklakeFilePartitionValue>> = HashMap()

        val partval = DUCKLAKE_FILE_PARTITION_VALUE.`as`("partval")
        val file = DUCKLAKE_DATA_FILE.`as`("file")
        // Multi-table JOIN — routed through `metadata` for Quack compatibility.
        metadata.fetch(
            dsl,
            dsl.select(
                partval.DATA_FILE_ID,
                partval.PARTITION_KEY_INDEX,
                partval.PARTITION_VALUE,
            )
                .from(partval)
                .innerJoin(file)
                .on(partval.DATA_FILE_ID.eq(file.DATA_FILE_ID))
                .and(partval.TABLE_ID.eq(file.TABLE_ID))
                .where(partval.TABLE_ID.eq(tableId))
                .and(activeAt(file, snapshotId)),
        ) { r ->
            val dataFileId = orZero(r.get(partval.DATA_FILE_ID))
            result.getOrPut(dataFileId) { ArrayList() }
                .add(
                    DucklakeFilePartitionValue(
                        dataFileId,
                        orZero(r.get(partval.PARTITION_KEY_INDEX)).toInt(),
                        r.get(partval.PARTITION_VALUE),
                    ),
                )
            null
        }

        return result
    }

    override fun getNameMaps(mappingIds: java.util.Set<Long>): Map<Long, Map<Long, String>> {
        if (mappingIds.isEmpty()) {
            return emptyMap()
        }
        val nm = DUCKLAKE_NAME_MAPPING.`as`("nm")
        val result: MutableMap<Long, MutableMap<Long, String>> = HashMap()
        dsl.select(nm.MAPPING_ID, nm.TARGET_FIELD_ID, nm.SOURCE_NAME)
            .from(nm)
            .where(nm.MAPPING_ID.`in`(mappingIds))
            // Top-level entries only — nested struct/list/map source-name resolution
            // is handled by Trino's reader once we descend into a matched group field.
            .and(nm.PARENT_COLUMN.isNull)
            // Exclude hive partition entries — those have no parquet column to find.
            .and(nm.IS_PARTITION.isFalse.or(nm.IS_PARTITION.isNull))
            .forEach { r ->
                val mappingId = r.get(nm.MAPPING_ID)
                val fieldId = r.get(nm.TARGET_FIELD_ID)
                val sourceName = r.get(nm.SOURCE_NAME)
                if (mappingId != null && fieldId != null && sourceName != null) {
                    result.getOrPut(mappingId) { HashMap() }[fieldId] = sourceName
                }
            }
        return result
    }

    override fun getInlinedDataInfos(tableId: Long, snapshotId: Long): List<DucklakeInlinedDataInfo> {
        // A table can have multiple inlined data tables (one per schema version).
        val inlined = DUCKLAKE_INLINED_DATA_TABLES.`as`("inlined")
        val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
        try {
            return dsl.select(
                inlined.TABLE_ID,
                inlined.TABLE_NAME,
                inlined.SCHEMA_VERSION,
            )
                .from(inlined)
                .where(inlined.TABLE_ID.eq(tableId))
                .and(
                    inlined.SCHEMA_VERSION.le(
                        DSL.select(snap.SCHEMA_VERSION)
                            .from(snap)
                            .where(snap.SNAPSHOT_ID.eq(snapshotId)),
                    ),
                )
                .orderBy(inlined.SCHEMA_VERSION)
                .fetch()
                .asSequence()
                .filter { r ->
                    val inlinedTable = InlinedDataTable.of(
                        orZero(r.get(inlined.TABLE_ID)),
                        orZero(r.get(inlined.SCHEMA_VERSION)),
                    )
                    if (!inlinedTable.existsAsTable(dsl)) {
                        // Catalog metadata can point to a dropped/non-materialized inlined table.
                        // Treat this as "no inlined data" so scan planning does not emit a dead split.
                        log.log(
                            System.Logger.Level.DEBUG,
                            "Inlined data table {0} not available for table {1}",
                            inlinedTable.name, tableId,
                        )
                        false
                    }
                    else {
                        true
                    }
                }
                .map { r ->
                    DucklakeInlinedDataInfo(
                        orZero(r.get(inlined.TABLE_ID)),
                        r.get(inlined.TABLE_NAME),
                        orZero(r.get(inlined.SCHEMA_VERSION)),
                    )
                }
                .toList()
        }
        catch (e: DataAccessException) {
            // ducklake_inlined_data_tables may not exist in catalogs that never used inlining
            log.log(
                System.Logger.Level.DEBUG,
                "Could not query inlined data tables (table may not exist): {0}",
                e.message,
            )
            return emptyList()
        }
    }

    override fun hasInlinedRows(tableId: Long, schemaVersion: Long, snapshotId: Long): Boolean {
        val inlined = InlinedDataTable.of(tableId, schemaVersion)
        return try {
            dsl.fetchExists(
                DSL.selectOne()
                    .from(inlined.table)
                    .where(inlined.activeAt(snapshotId)),
            )
        }
        catch (e: DataAccessException) {
            log.log(
                System.Logger.Level.DEBUG,
                "Could not probe inlined data rows from {0} (table may not exist): {1}",
                inlined.name, e.message,
            )
            false
        }
    }

    override fun hasInlinedDeletes(tableId: Long, snapshotId: Long): Boolean {
        // ducklake_inlined_delete_<tableId> is created lazily by DuckDB the first
        // time DATA_INLINING_ROW_LIMIT causes a deletion to be inlined; before
        // that it doesn't exist at all. The probe catches the
        // table-doesn't-exist case and returns false.
        // Schema (per upstream data_inlining.md): file_id, row_id, begin_snapshot.
        // No end_snapshot — once an inlined delete row exists for a snapshot, it's
        // permanent until compaction rewrites the data file.
        val inlinedDeleteName = "ducklake_inlined_delete_$tableId"
        val tab: Table<*> = DSL.table(DSL.name(inlinedDeleteName))
        val beginSnapshot: Field<Long> = DSL.field(DSL.name("begin_snapshot"), Long::class.java)
        return try {
            dsl.fetchExists(
                DSL.selectOne()
                    .from(tab)
                    .where(beginSnapshot.le(snapshotId)),
            )
        }
        catch (e: DataAccessException) {
            log.log(
                System.Logger.Level.DEBUG,
                "Could not probe inlined deletions from {0} (table may not exist): {1}",
                inlinedDeleteName, e.message,
            )
            false
        }
    }

    override fun getInlinedDeletes(tableId: Long, snapshotId: Long): Map<Long, java.util.Set<Long>> {
        // Schema (per upstream data_inlining.md):
        //   ducklake_inlined_delete_<tableId>(file_id BIGINT, row_id BIGINT, begin_snapshot BIGINT)
        // file_id = ducklake_data_file.data_file_id
        // row_id  = deleted row's file-local position
        // No end_snapshot — rows accumulate until compaction rewrites the data file.
        // Table is created lazily by DuckDB the first time DATA_INLINING_ROW_LIMIT
        // causes a deletion to be inlined; absence is the common case.
        val inlinedDeleteName = "ducklake_inlined_delete_$tableId"
        val tab: Table<*> = DSL.table(DSL.name(inlinedDeleteName))
        val fileId: Field<Long> = DSL.field(DSL.name("file_id"), Long::class.java)
        val rowId: Field<Long> = DSL.field(DSL.name("row_id"), Long::class.java)
        val beginSnapshot: Field<Long> = DSL.field(DSL.name("begin_snapshot"), Long::class.java)
        return try {
            val result = dsl.select(fileId, rowId)
                .from(tab)
                .where(beginSnapshot.le(snapshotId))
                .fetch()
            val grouped: MutableMap<Long, java.util.HashSet<Long>> = HashMap()
            for (rec in result) {
                val fid = rec.get(fileId)
                val rid = rec.get(rowId)
                if (fid == null || rid == null) {
                    continue
                }
                grouped.getOrPut(fid) { HashSet() }.add(rid)
            }
            @Suppress("UNCHECKED_CAST")
            grouped as Map<Long, java.util.Set<Long>>
        }
        catch (e: DataAccessException) {
            log.log(
                System.Logger.Level.DEBUG,
                "Could not read inlined deletions from {0} (table may not exist): {1}",
                inlinedDeleteName, e.message,
            )
            emptyMap()
        }
    }

    override fun readInlinedData(
        tableId: Long,
        schemaVersion: Long,
        snapshotId: Long,
        columns: List<DucklakeColumn>,
    ): List<List<Any?>> {
        if (columns.isEmpty()) {
            return emptyList()
        }

        val inlined = InlinedDataTable.of(tableId, schemaVersion)

        val sourceSchemaSnapshot = getSnapshotIdForSchemaVersion(tableId, schemaVersion, snapshotId)
        if (sourceSchemaSnapshot.isEmpty) {
            return emptyList()
        }

        val sourceColumnsById: Map<Long, DucklakeColumn> = getTableColumns(tableId, sourceSchemaSnapshot.asLong)
            .associateBy { it.columnId }

        val projected: MutableList<Field<*>> = ArrayList(columns.size)
        for (index in columns.indices) {
            val alias = "c$index"
            val sourceColumn = sourceColumnsById[columns[index].columnId]
            projected.add(
                if (sourceColumn == null) {
                    DSL.inline(null as Any?).`as`(alias)
                }
                else {
                    DSL.field(DSL.name(sourceColumn.columnName)).`as`(alias)
                },
            )
        }

        return try {
            val result = dsl.select(projected)
                .from(inlined.table)
                .where(inlined.activeAt(snapshotId))
                .orderBy(DSL.field(DSL.name("row_id")))
                .fetch()

            val columnCount = columns.size
            val rows: MutableList<List<Any?>> = ArrayList(result.size)
            for (rec in result) {
                val row: MutableList<Any?> = ArrayList(columnCount)
                for (i in 0 until columnCount) {
                    row.add(rec.get(i))
                }
                rows.add(row)
            }
            rows
        }
        catch (e: DataAccessException) {
            // The inlined data table may not exist if the table was created but never had data inserted,
            // or if the inlined data was flushed to Parquet files. Return empty in these cases.
            log.log(
                System.Logger.Level.DEBUG,
                "Could not read inlined data from {0} (table may not exist): {1}",
                inlined.name, e.message,
            )
            emptyList()
        }
    }

    private fun getSnapshotIdForSchemaVersion(tableId: Long, schemaVersion: Long, snapshotId: Long): OptionalLong {
        // Prefer table-scoped schema version rows when available.
        // Some catalogs include ducklake_schema_versions.table_id (DuckDB behavior),
        // which gives an unambiguous snapshot where this table's schema version was introduced.
        val schver = DUCKLAKE_SCHEMA_VERSIONS.`as`("schver")
        try {
            val tableScoped: Long? = dsl.select(schver.BEGIN_SNAPSHOT)
                .from(schver)
                .where(schver.TABLE_ID.eq(tableId))
                .and(schver.SCHEMA_VERSION.eq(schemaVersion))
                .and(schver.BEGIN_SNAPSHOT.le(snapshotId))
                .orderBy(schver.BEGIN_SNAPSHOT.desc())
                .limit(1)
                .fetchOne(schver.BEGIN_SNAPSHOT)
            if (tableScoped != null) {
                return OptionalLong.of(tableScoped)
            }
        }
        catch (e: DataAccessException) {
            // Fallback for catalogs without table_id in ducklake_schema_versions or older metadata.
            log.log(
                System.Logger.Level.DEBUG,
                "Could not resolve schema version via ducklake_schema_versions for table {0}: {1}",
                tableId, e.message,
            )
        }

        // Backward-compatible fallback: resolve by snapshot.schema_version only.
        val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
        val fallback: Long? = dsl.select(snap.SNAPSHOT_ID)
            .from(snap)
            .where(snap.SCHEMA_VERSION.eq(schemaVersion))
            .and(snap.SNAPSHOT_ID.le(snapshotId))
            .orderBy(snap.SNAPSHOT_ID.desc())
            .limit(1)
            .fetchOne(snap.SNAPSHOT_ID)
        return if (fallback == null) OptionalLong.empty() else OptionalLong.of(fallback)
    }

    /**
     * Handle to a dynamic `ducklake_inlined_data_{tableId}_{schemaVersion}` table that
     * codegen doesn't know about. Bundles the table reference with its `begin_snapshot` /
     * `end_snapshot` fields so the per-method setup dance reduces to one line.
     */
    private data class InlinedDataTable(
        val name: String,
        val table: Table<*>,
        val beginSnapshot: Field<Long>,
        val endSnapshot: Field<Long>,
    ) {
        fun activeAt(snapshotId: Long): Condition {
            return SnapshotRange.activeAt(beginSnapshot, endSnapshot, snapshotId)
        }

        fun existsAsTable(dsl: DSLContext): Boolean {
            return try {
                dsl.selectOne().from(table).where(DSL.falseCondition()).fetch()
                true
            }
            catch (e: DataAccessException) {
                false
            }
        }

        companion object {
            @JvmStatic
            fun of(tableId: Long, schemaVersion: Long): InlinedDataTable {
                val name = String.format("ducklake_inlined_data_%d_%d", tableId, schemaVersion)
                return InlinedDataTable(
                    name,
                    DSL.table(DSL.name(name)),
                    DSL.field(DSL.name("begin_snapshot"), Long::class.java),
                    DSL.field(DSL.name("end_snapshot"), Long::class.java),
                )
            }
        }
    }

    // Snapshot-change recording lives on the typed `WriteChange` hierarchy.
    // Quoting and joining used to be local helpers here; they now live on
    // `WriteChange` so that the conflict-checking machinery and the
    // `ducklake_snapshot_changes.changes_made` serializer share one source
    // of truth.

    override fun getDataPath(): Optional<String> {
        val meta = DUCKLAKE_METADATA.`as`("meta")
        return dsl.select(meta.VALUE)
            .from(meta)
            .where(meta.KEY.eq("data_path"))
            .fetchOptional(meta.VALUE)
    }

    // ==================== View operations ====================

    override fun listViews(schemaId: Long, snapshotId: Long): List<DucklakeView> {
        val view = DUCKLAKE_VIEW.`as`("view")
        return dsl.selectFrom(view)
            .where(view.SCHEMA_ID.eq(schemaId))
            .and(activeAt(view, snapshotId))
            .fetch { toDucklakeView(it) }
    }

    override fun getView(schemaName: String, viewName: String, snapshotId: Long): Optional<DucklakeView> {
        val schema = getSchema(schemaName, snapshotId)
        if (schema.isEmpty) {
            return Optional.empty()
        }

        val view = DUCKLAKE_VIEW.`as`("view")
        return dsl.selectFrom(view)
            .where(view.SCHEMA_ID.eq(schema.get().schemaId))
            .and(view.VIEW_NAME.eq(viewName))
            .and(activeAt(view, snapshotId))
            .fetchOptional()
            .map { toDucklakeView(it) }
    }

    // Write transaction infrastructure

    fun interface WriteTransactionAction {
        @Throws(SQLException::class)
        fun execute(transaction: DucklakeWriteTransaction)
    }

    /**
     * Test seam: invoked once per attempt, after this attempt has read the
     * current snapshot but before any of its mutations run. Tests assign a
     * barrier-aware Runnable here to deterministically park one writer while
     * a competing writer commits, so the parked writer's lineage check fails
     * on resume and triggers retry. Positioned pre-mutation so the parked
     * transaction holds no row locks and can't deadlock with the competitor.
     * No-op in production.
     */
    @Volatile
    @JvmField
    var beforeWriteTransactionAction: Runnable = Runnable {}

    /**
     * Executes a write operation within an atomic snapshot transaction.
     * Handles connection management, snapshot creation, change tracking,
     * and commit/rollback. The caller provides a callback that performs
     * its mutations using the transaction context.
     *
     * On a [TransactionConflictException] (PK collision on
     * `ducklake_snapshot` or detected lineage advance), the operation
     * is retried with exponential backoff up to [MAX_RETRY_COUNT]
     * times. After exhaustion, the most recent conflict is rethrown wrapped
     * with an "exceeded retry count" message. This matches upstream DuckLake's
     * retry semantics so that low-rate contention is absorbed transparently.
     */
    private fun executeWriteTransaction(operationDescription: String, action: WriteTransactionAction) {
        // Captured once on attempt 1, propagated across retries so the
        // change-vs-change conflict matrix on retry can ask "what committed
        // since this transaction started?" — mirrors upstream's
        // `transaction_snapshot` captured outside the retry loop in
        // ducklake_transaction.cpp:2455.
        val transactionStartSnapshotId = longArrayOf(-1L)
        WriteTransactionRetry.retryOnConflict(
            MAX_RETRY_COUNT,
            INITIAL_RETRY_WAIT_MS,
            RETRY_BACKOFF_MULTIPLIER,
            { millis -> Thread.sleep(millis) },
            operationDescription,
        ) {
            attemptWriteTransaction(operationDescription, action, transactionStartSnapshotId)
        }
    }

    private fun attemptWriteTransaction(
        operationDescription: String,
        action: WriteTransactionAction,
        transactionStartSnapshotId: LongArray,
    ) {
        val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
        val schver = DUCKLAKE_SCHEMA_VERSIONS.`as`("schver")
        val snapchg = DUCKLAKE_SNAPSHOT_CHANGES.`as`("snapchg")
        try {
            dataSource.connection.use { conn ->
                conn.autoCommit = false
                val txDsl = forConnection(conn)
                var baseSnapshotId: Long = -1
                try {
                    // 1. Read current snapshot state. Routed through `metadata` because
                    // this is a same-table multi-scan (`WHERE id = (SELECT max(id) FROM
                    // same_table)`), which the Quack RPC optimizer rejects on attached
                    // metadata catalogs. On PG / local DuckDB the helper is a pass-through.
                    val snapshotRow: DucklakeSnapshotRecord? = metadata.fetchOne(
                        txDsl,
                        txDsl.selectFrom(snap)
                            .where(snap.SNAPSHOT_ID.eq(DSL.select(DSL.max(snap.SNAPSHOT_ID)).from(snap))),
                    )
                    if (snapshotRow == null) {
                        throw IllegalStateException("No snapshots found")
                    }
                    val currentSnapshotId: Long = snapshotRow.snapshotId
                    baseSnapshotId = currentSnapshotId
                    if (transactionStartSnapshotId[0] == -1L) {
                        transactionStartSnapshotId[0] = currentSnapshotId
                    }
                    val schemaVersion = orZero(snapshotRow.schemaVersion)
                    val nextCatalogId = orZero(snapshotRow.nextCatalogId)
                    val nextFileId = orZero(snapshotRow.nextFileId)

                    // 2. Execute the caller's mutations
                    val tx = DucklakeWriteTransaction(
                        conn, txDsl, currentSnapshotId, schemaVersion, nextCatalogId, nextFileId,
                    )

                    // Test seam: lets concurrency tests park this attempt before it
                    // does any writes, so a competing committer can advance the
                    // snapshot without deadlocking on row locks this tx would hold.
                    beforeWriteTransactionAction.run()

                    action.execute(tx)

                    // 3. Strict optimistic conflict check: if snapshot lineage advanced, abort.
                    ensureSnapshotLineageUnchanged(txDsl, tx.getCurrentSnapshotId(), operationDescription)

                    // 3b. Logical conflict check: validate the action's payload still
                    // references entities that are active at the current snapshot. This
                    // catches the case the strict lineage check + retry alone misses:
                    // the retry's action re-runs with stale per-call args (table IDs,
                    // fragment column / data-file IDs) captured before any prior
                    // attempt. Throws non-retryable LogicalConflictException on a
                    // mismatch so the retry loop bails out instead of burning the
                    // retry budget on a guaranteed-fail.
                    LogicalConflictCheck.run(tx, operationDescription)

                    // 3c. Change-vs-change conflict matrix (port of upstream
                    // CheckForConflicts at ducklake_transaction.cpp:1184-1314). Only
                    // runs when an earlier attempt of THIS transaction's retry loop
                    // saw an older snapshot — i.e. when other transactions committed
                    // between transactionStartSnapshotId (captured on attempt 1) and
                    // currentSnapshotId. Catches dueling-name commits the state-based
                    // check above can't see (no UNIQUE on (schema_id, name) — see
                    // ducklake_metadata_manager.cpp:198-200).
                    if (currentSnapshotId > transactionStartSnapshotId[0]) {
                        runConflictMatrix(
                            txDsl, tx.getChanges(),
                            transactionStartSnapshotId[0], currentSnapshotId,
                        )
                    }

                    // 4. Create new snapshot row (with final allocated IDs)
                    insertSnapshotRow(txDsl, tx, operationDescription)

                    // 5. Insert schema_versions row if schema version changed
                    if (tx.getSchemaVersion() != schemaVersion) {
                        val schemaVersionTableId: Long? =
                            if (tx.getSchemaVersionTableId() >= 0) tx.getSchemaVersionTableId() else null
                        txDsl.insertInto(schver)
                            .set(schver.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                            .set(schver.SCHEMA_VERSION, tx.getSchemaVersion())
                            .set(schver.TABLE_ID, schemaVersionTableId)
                            .execute()
                    }

                    // 6. Insert snapshot changes (comma-separated per spec, one row per snapshot)
                    if (!tx.getChanges().isEmpty()) {
                        txDsl.insertInto(snapchg)
                            .set(snapchg.SNAPSHOT_ID, tx.getNewSnapshotId())
                            .set(snapchg.CHANGES_MADE, WriteChange.formatChangesMade(tx.getChanges()))
                            .execute()
                    }

                    conn.commit()
                }
                catch (e: Exception) {
                    conn.rollback()
                    if (hasTransactionConflict(e)) {
                        throw e as RuntimeException
                    }
                    if (isMetadataPrimaryKeyConflict(e)) {
                        val currentSnapshot = readLatestSnapshotId(txDsl)
                        throw transactionConflictException(txDsl, baseSnapshotId, currentSnapshot, operationDescription, e)
                    }
                    throw RuntimeException("Failed to $operationDescription", e)
                }
            }
        }
        catch (e: SQLException) {
            throw RuntimeException("Failed to $operationDescription", e)
        }
    }

    private fun ensureSnapshotLineageUnchanged(ctx: DSLContext, expectedSnapshotId: Long, operationDescription: String) {
        val currentSnapshotId = readLatestSnapshotId(ctx)
        if (currentSnapshotId != expectedSnapshotId) {
            throw transactionConflictException(ctx, expectedSnapshotId, currentSnapshotId, operationDescription, null)
        }
    }

    private fun insertSnapshotRow(ctx: DSLContext, tx: DucklakeWriteTransaction, operationDescription: String) {
        val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
        try {
            ctx.insertInto(snap)
                .set(snap.SNAPSHOT_ID, tx.getNewSnapshotId())
                .set(snap.SNAPSHOT_TIME, DSL.currentOffsetDateTime())
                .set(snap.SCHEMA_VERSION, tx.getSchemaVersion())
                .set(snap.NEXT_CATALOG_ID, tx.getFinalNextCatalogId())
                .set(snap.NEXT_FILE_ID, tx.getFinalNextFileId())
                .execute()
        }
        catch (e: DataAccessException) {
            val cause = findSqlException(e)
            if (cause != null && isDuplicateKeyViolation(cause)) {
                val currentSnapshotId = readLatestSnapshotId(ctx)
                throw transactionConflictException(ctx, tx.getCurrentSnapshotId(), currentSnapshotId, operationDescription, e)
            }
            throw e
        }
    }

    private fun transactionConflictException(
        ctx: DSLContext,
        expectedSnapshotId: Long,
        currentSnapshotId: Long,
        operationDescription: String,
        cause: Throwable?,
    ): TransactionConflictException {
        val interveningChanges = getInterveningChangesSummary(ctx, expectedSnapshotId, currentSnapshotId)
        val message = "Concurrent DuckLake commit while attempting to " + operationDescription +
            ": expected base snapshot " + expectedSnapshotId +
            ", but current snapshot is " + currentSnapshotId +
            ". Intervening changes: " + interveningChanges
        return TransactionConflictException(message, cause)
    }

    private fun getInterveningChangesSummary(ctx: DSLContext, fromSnapshotExclusive: Long, toSnapshotInclusive: Long): String {
        if (toSnapshotInclusive <= fromSnapshotExclusive) {
            return "none"
        }

        val snapchg = DUCKLAKE_SNAPSHOT_CHANGES.`as`("snapchg")
        val changes: List<String> = ctx.select(
            snapchg.SNAPSHOT_ID,
            snapchg.CHANGES_MADE,
        )
            .from(snapchg)
            .where(snapchg.SNAPSHOT_ID.gt(fromSnapshotExclusive))
            .and(snapchg.SNAPSHOT_ID.le(toSnapshotInclusive))
            .orderBy(snapchg.SNAPSHOT_ID)
            .limit(CONFLICT_CHANGE_SUMMARY_LIMIT)
            .fetch()
            .map { r -> r.get(snapchg.SNAPSHOT_ID).toString() + ":" + r.get(snapchg.CHANGES_MADE) }

        if (changes.isEmpty()) {
            return "snapshot advanced without snapshot_changes rows"
        }
        return changes.joinToString("; ")
    }

    override fun createView(schemaName: String, viewName: String, sql: String, dialect: String, viewMetadata: String?) {
        executeWriteTransaction("create view $schemaName.$viewName") { tx ->
            val schemaId = tx.resolveSchemaId(schemaName)
            val viewId = tx.allocateCatalogId()
            tx.recordChange(WriteChange.CreatedView(schemaId, schemaName, viewName))

            insertViewRow(tx, viewId, UUID.fromString(newCatalogUuid()), schemaId, viewName, dialect, sql, viewMetadata)
            tx.incrementSchemaVersion()
        }
    }

    override fun dropView(schemaName: String, viewName: String) {
        executeWriteTransaction("drop view $schemaName.$viewName") { tx ->
            val schemaId = tx.resolveSchemaId(schemaName)
            val view = resolveActiveViewRow(tx, schemaId, viewName)
            endSnapshotActiveView(tx, view.viewId)
            tx.recordChange(WriteChange.DroppedView(view.viewId))
            tx.incrementSchemaVersion()
        }
    }

    override fun renameView(
        sourceSchemaName: String,
        sourceViewName: String,
        targetSchemaName: String,
        targetViewName: String,
    ) {
        executeWriteTransaction(
            "rename view $sourceSchemaName.$sourceViewName to $targetSchemaName.$targetViewName",
        ) { tx ->
            val sourceSchemaId = tx.resolveSchemaId(sourceSchemaName)
            val sourceView = resolveActiveViewRow(tx, sourceSchemaId, sourceViewName)
            val targetSchemaId = tx.resolveSchemaId(targetSchemaName)

            if (hasActiveTable(tx, targetSchemaId, targetViewName) || hasActiveView(tx, targetSchemaId, targetViewName)) {
                throw RuntimeException("Relation already exists: $targetSchemaName.$targetViewName")
            }

            endSnapshotActiveView(tx, sourceView.viewId)
            insertViewRow(
                tx,
                sourceView.viewId,
                sourceView.viewUuid,
                targetSchemaId,
                targetViewName,
                sourceView.dialect,
                sourceView.sql,
                sourceView.viewMetadata.orElse(null),
            )
            // Upstream's ParseChangeType does not recognize `renamed_view`; a rename is
            // semantically a schema/name change, so emit `altered_view` to stay
            // spec-conformant with DuckDB's ducklake_snapshots() parser.
            tx.recordChange(WriteChange.AlteredView(sourceView.viewId))
            tx.incrementSchemaVersion()
        }
    }

    override fun replaceViewMetadata(schemaName: String, viewName: String, sql: String, dialect: String, viewMetadata: String?) {
        executeWriteTransaction("alter view $schemaName.$viewName") { tx ->
            val schemaId = tx.resolveSchemaId(schemaName)
            val view = resolveActiveViewRow(tx, schemaId, viewName)

            endSnapshotActiveView(tx, view.viewId)
            insertViewRow(tx, view.viewId, view.viewUuid, schemaId, viewName, dialect, sql, viewMetadata)
            tx.recordChange(WriteChange.AlteredView(view.viewId))
            tx.incrementSchemaVersion()
        }
    }

    private fun resolveActiveViewRow(tx: DucklakeWriteTransaction, schemaId: Long, viewName: String): ActiveViewRow {
        val view = DUCKLAKE_VIEW.`as`("view")
        val row: Record? = tx.dsl().select(
            view.VIEW_ID,
            view.VIEW_UUID,
            view.SCHEMA_ID,
            view.VIEW_NAME,
            view.DIALECT,
            view.SQL,
            view.COLUMN_ALIASES,
        )
            .from(view)
            .where(view.SCHEMA_ID.eq(schemaId))
            .and(view.VIEW_NAME.eq(viewName))
            .and(activeAt(view, tx.getCurrentSnapshotId()))
            .fetchOne()
        if (row == null) {
            throw RuntimeException("View not found: schema_id=$schemaId, view_name=$viewName")
        }
        return ActiveViewRow(
            orZero(row.get(view.VIEW_ID)),
            row.get(view.VIEW_UUID),
            orZero(row.get(view.SCHEMA_ID)),
            row.get(view.VIEW_NAME),
            row.get(view.DIALECT),
            row.get(view.SQL),
            Optional.ofNullable(row.get(view.COLUMN_ALIASES)),
        )
    }

    private fun insertViewRow(
        tx: DucklakeWriteTransaction,
        viewId: Long,
        viewUuid: UUID,
        schemaId: Long,
        viewName: String,
        dialect: String,
        viewSql: String,
        viewMetadata: String?,
    ) {
        val view = DUCKLAKE_VIEW.`as`("view")
        tx.dsl().insertInto(view)
            .set(view.VIEW_ID, viewId)
            .set(view.VIEW_UUID, viewUuid)
            .set(view.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
            .set(view.SCHEMA_ID, schemaId)
            .set(view.VIEW_NAME, viewName)
            .set(view.DIALECT, dialect)
            .set(view.SQL, viewSql)
            .set(view.COLUMN_ALIASES, viewMetadata)
            .execute()
    }

    private fun endSnapshotActiveView(tx: DucklakeWriteTransaction, viewId: Long) {
        val view = DUCKLAKE_VIEW.`as`("view")
        val updatedRows = tx.dsl().update(view)
            .set(view.END_SNAPSHOT, tx.getNewSnapshotId())
            .where(view.VIEW_ID.eq(viewId))
            .and(view.END_SNAPSHOT.isNull)
            .execute()
        if (updatedRows == 0) {
            throw RuntimeException("View not found: $viewId")
        }
    }

    private data class ActiveViewRow(
        val viewId: Long,
        val viewUuid: UUID,
        val schemaId: Long,
        val viewName: String,
        val dialect: String,
        val sql: String,
        val viewMetadata: Optional<String>,
    )

    // ==================== Schema DDL ====================

    override fun createSchema(schemaName: String) {
        val sch = DUCKLAKE_SCHEMA.`as`("sch")
        executeWriteTransaction("create schema $schemaName") { tx ->
            val schemaId = tx.allocateCatalogId()
            tx.recordChange(WriteChange.CreatedSchema(schemaName))

            tx.dsl().insertInto(sch)
                .set(sch.SCHEMA_ID, schemaId)
                .set(sch.SCHEMA_UUID, UUID.fromString(newCatalogUuid()))
                .set(sch.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                .set(sch.SCHEMA_NAME, schemaName)
                .set(sch.PATH, "$schemaName/")
                .set(sch.PATH_IS_RELATIVE, true)
                .execute()

            tx.incrementSchemaVersion()
        }
    }

    override fun dropSchema(schemaName: String) {
        val sch = DUCKLAKE_SCHEMA.`as`("sch")
        executeWriteTransaction("drop schema $schemaName") { tx ->
            val schemaId = tx.resolveSchemaId(schemaName)

            if (tx.hasTablesInSchema(schemaId)) {
                throw RuntimeException("Cannot drop schema $schemaName: schema is not empty")
            }

            tx.recordChange(WriteChange.DroppedSchema(schemaId, schemaName))

            tx.dsl().update(sch)
                .set(sch.END_SNAPSHOT, tx.getNewSnapshotId())
                .where(sch.SCHEMA_ID.eq(schemaId))
                .and(sch.END_SNAPSHOT.isNull)
                .execute()

            tx.incrementSchemaVersion()
        }
    }

    // ==================== Table DDL ====================

    override fun createTable(
        schemaName: String,
        tableName: String,
        columns: List<TableColumnSpec>,
        partitionSpec: Optional<List<PartitionFieldSpec>>,
        location: Optional<TableLocationSpec>,
    ) {
        val tab = DUCKLAKE_TABLE.`as`("tab")
        val partinfo = DUCKLAKE_PARTITION_INFO.`as`("partinfo")
        val partcol = DUCKLAKE_PARTITION_COLUMN.`as`("partcol")
        val tablePath: String = location.map { it.path }.orElse("$tableName/")
        val pathIsRelative: Boolean = location.map { it.isRelative }.orElse(true)
        executeWriteTransaction("create table $schemaName.$tableName") { tx ->
            val schemaId = tx.resolveSchemaId(schemaName)
            val tableId = tx.allocateCatalogId()
            val ctx = tx.dsl()

            // 1. Insert table row
            ctx.insertInto(tab)
                .set(tab.TABLE_ID, tableId)
                .set(tab.TABLE_UUID, UUID.fromString(newCatalogUuid()))
                .set(tab.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                .set(tab.SCHEMA_ID, schemaId)
                .set(tab.TABLE_NAME, tableName)
                .set(tab.PATH, tablePath)
                .set(tab.PATH_IS_RELATIVE, pathIsRelative)
                .execute()

            // 2. Insert column rows (flattening nested types with parent links)
            // column_order is 1-based per DuckDB convention
            val topLevelColumnIds: MutableMap<String, Long> = LinkedHashMap()
            var columnOrder: Long = 1
            for (column in columns) {
                val columnId = insertColumnTree(tx, tableId, column, columnOrder++, OptionalLong.empty())
                topLevelColumnIds[column.name] = columnId
            }

            // 3. Table stats are NOT created at CREATE TABLE time — DuckDB creates them
            // only when data is first inserted. Creating them here with zeros causes
            // DuckDB's GetGlobalTableStats to crash (GetValueInternal on NULL).

            // 4. Insert partition spec if provided
            if (partitionSpec.isPresent && partitionSpec.get().isNotEmpty()) {
                val partitionId = tx.allocateCatalogId()

                ctx.insertInto(partinfo)
                    .set(partinfo.PARTITION_ID, partitionId)
                    .set(partinfo.TABLE_ID, tableId)
                    .set(partinfo.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                    .execute()

                var keyIndex: Long = 0
                for (field in partitionSpec.get()) {
                    val columnId = topLevelColumnIds[field.columnName]
                        ?: throw RuntimeException("Partition column not found: ${field.columnName}")
                    ctx.insertInto(partcol)
                        .set(partcol.PARTITION_ID, partitionId)
                        .set(partcol.TABLE_ID, tableId)
                        .set(partcol.PARTITION_KEY_INDEX, keyIndex++)
                        .set(partcol.COLUMN_ID, columnId)
                        .set(partcol.TRANSFORM, field.transform.toCatalogString(field.arity))
                        .execute()
                }
            }

            tx.incrementSchemaVersion(tableId)
            tx.recordChange(WriteChange.CreatedTable(schemaId, schemaName, tableName))
        }
    }

    /**
     * Recursively inserts a column and its children into ducklake_column.
     * Returns the column_id of the inserted column.
     */
    private fun insertColumnTree(
        tx: DucklakeWriteTransaction,
        tableId: Long,
        column: TableColumnSpec,
        columnOrder: Long,
        parentColumnId: OptionalLong,
    ): Long {
        val columnId = tx.allocateCatalogId()

        // `default_value = 'NULL'` (the four-char string, not SQL NULL) is upstream's "no
        // default" sentinel; `default_value_type = 'literal'` is mandatory per upstream's
        // migration (ducklake_metadata_manager.cpp backfills NULL → 'literal'). `initial_default`
        // and `default_value_dialect` are left unset (SQL NULL): the dialect is informational
        // and only meaningful when there's a real expression to interpret. If we ever wire up
        // user-written Trino DEFAULT expressions, set 'trino' here — the dialect names the SQL
        // syntax of the expression, which would be plain Trino SQL.
        val col = DUCKLAKE_COLUMN.`as`("col")
        tx.dsl().insertInto(col)
            .set(col.COLUMN_ID, columnId)
            .set(col.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
            .set(col.TABLE_ID, tableId)
            .set(col.COLUMN_ORDER, columnOrder)
            .set(col.COLUMN_NAME, column.name)
            .set(col.COLUMN_TYPE, column.ducklakeType)
            .set(col.DEFAULT_VALUE, "NULL")
            .set(col.NULLS_ALLOWED, column.nullable)
            .set(col.PARENT_COLUMN, if (parentColumnId.isPresent) parentColumnId.asLong else null)
            .set(col.DEFAULT_VALUE_TYPE, "literal")
            .execute()

        // Insert children with their own column_order (0-based within parent)
        var childOrder: Long = 0
        for (child in column.children) {
            insertColumnTree(tx, tableId, child, childOrder++, OptionalLong.of(columnId))
        }

        return columnId
    }

    override fun dropTable(schemaName: String, tableName: String) {
        val tab = DUCKLAKE_TABLE.`as`("tab")
        val col = DUCKLAKE_COLUMN.`as`("col")
        val file = DUCKLAKE_DATA_FILE.`as`("file")
        val delfile = DUCKLAKE_DELETE_FILE.`as`("delfile")
        val partinfo = DUCKLAKE_PARTITION_INFO.`as`("partinfo")
        executeWriteTransaction("drop table $schemaName.$tableName") { tx ->
            val schemaId = tx.resolveSchemaId(schemaName)
            val tableId = tx.resolveTableId(schemaId, tableName)
            val ctx = tx.dsl()
            val newSnapshotId = tx.getNewSnapshotId()

            // End-snapshot the table row. Routed through `metadata` because the
            // Quack RPC binder rejects UPDATE on attached-metadata tables with
            // "Can only update base table". Pass-through on PG / local DuckDB.
            metadata.execute(
                ctx,
                ctx.update(tab)
                    .set(tab.END_SNAPSHOT, newSnapshotId)
                    .where(tab.TABLE_ID.eq(tableId))
                    .and(tab.END_SNAPSHOT.isNull),
            )

            // End-snapshot all active columns
            metadata.execute(
                ctx,
                ctx.update(col)
                    .set(col.END_SNAPSHOT, newSnapshotId)
                    .where(col.TABLE_ID.eq(tableId))
                    .and(col.END_SNAPSHOT.isNull),
            )

            // End-snapshot all active data files
            metadata.execute(
                ctx,
                ctx.update(file)
                    .set(file.END_SNAPSHOT, newSnapshotId)
                    .where(file.TABLE_ID.eq(tableId))
                    .and(file.END_SNAPSHOT.isNull),
            )

            // End-snapshot all active delete files (matched via data_file subquery)
            metadata.execute(
                ctx,
                ctx.update(delfile)
                    .set(delfile.END_SNAPSHOT, newSnapshotId)
                    .where(
                        delfile.DATA_FILE_ID.`in`(
                            DSL.select(file.DATA_FILE_ID)
                                .from(file)
                                .where(file.TABLE_ID.eq(tableId)),
                        ),
                    )
                    .and(delfile.END_SNAPSHOT.isNull),
            )

            // End-snapshot partition info
            metadata.execute(
                ctx,
                ctx.update(partinfo)
                    .set(partinfo.END_SNAPSHOT, newSnapshotId)
                    .where(partinfo.TABLE_ID.eq(tableId))
                    .and(partinfo.END_SNAPSHOT.isNull),
            )

            tx.incrementSchemaVersion(tableId)
            tx.recordChange(WriteChange.DroppedTable(tableId))
        }
    }

    override fun addColumn(tableId: Long, column: TableColumnSpec) {
        val col = DUCKLAKE_COLUMN.`as`("col")
        executeWriteTransaction("add column to table $tableId") { tx ->
            // Find the current max column_order for top-level columns
            val maxOrder: Long? = tx.dsl().select(DSL.max(col.COLUMN_ORDER))
                .from(col)
                .where(col.TABLE_ID.eq(tableId))
                .and(col.PARENT_COLUMN.isNull)
                .and(activeAt(col, tx.getCurrentSnapshotId()))
                .fetchOne(0, Long::class.java)

            insertColumnTree(tx, tableId, column, orZero(maxOrder) + 1, OptionalLong.empty())
            tx.incrementSchemaVersion(tableId)
            tx.recordChange(WriteChange.AlteredTable(tableId))
        }
    }

    override fun dropColumn(tableId: Long, columnId: Long) {
        val col = DUCKLAKE_COLUMN.`as`("col")
        executeWriteTransaction("drop column from table $tableId") { tx ->
            // End-snapshot the column and all its children (for nested types)
            tx.dsl().update(col)
                .set(col.END_SNAPSHOT, tx.getNewSnapshotId())
                .where(col.TABLE_ID.eq(tableId))
                .and(
                    col.COLUMN_ID.eq(columnId)
                        .or(col.PARENT_COLUMN.eq(columnId)),
                )
                .and(col.END_SNAPSHOT.isNull)
                .execute()

            tx.incrementSchemaVersion(tableId)
            tx.recordChange(WriteChange.AlteredTable(tableId))
        }
    }

    override fun renameColumn(tableId: Long, columnId: Long, newName: String) {
        val col = DUCKLAKE_COLUMN.`as`("col")
        executeWriteTransaction("rename column in table $tableId") { tx ->
            val ctx = tx.dsl()

            // Read the current column metadata
            val existing: Record? = ctx.select(col.COLUMN_ORDER, col.COLUMN_TYPE, col.NULLS_ALLOWED)
                .from(col)
                .where(col.TABLE_ID.eq(tableId))
                .and(col.COLUMN_ID.eq(columnId))
                .and(activeAt(col, tx.getCurrentSnapshotId()))
                .fetchOne()
            if (existing == null) {
                throw RuntimeException("Column not found: $columnId")
            }
            val columnOrder = orZero(existing.get(col.COLUMN_ORDER))
            val columnType = existing.get(col.COLUMN_TYPE)
            val nullsAllowed = java.lang.Boolean.TRUE == existing.get(col.NULLS_ALLOWED)

            // End-snapshot the current version
            ctx.update(col)
                .set(col.END_SNAPSHOT, tx.getNewSnapshotId())
                .where(col.TABLE_ID.eq(tableId))
                .and(col.COLUMN_ID.eq(columnId))
                .and(col.END_SNAPSHOT.isNull)
                .and(col.PARENT_COLUMN.isNull)
                .execute()

            // Insert new version with same column_id but new name. Default-value columns
            // preserve the "no default" sentinel (`'NULL'` string literal) and leave
            // `default_value_dialect` SQL NULL; same policy as insertColumnTree.
            ctx.insertInto(col)
                .set(col.COLUMN_ID, columnId)
                .set(col.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                .set(col.TABLE_ID, tableId)
                .set(col.COLUMN_ORDER, columnOrder)
                .set(col.COLUMN_NAME, newName)
                .set(col.COLUMN_TYPE, columnType)
                .set(col.DEFAULT_VALUE, "NULL")
                .set(col.NULLS_ALLOWED, nullsAllowed)
                .set(col.DEFAULT_VALUE_TYPE, "literal")
                .execute()

            tx.incrementSchemaVersion(tableId)
            tx.recordChange(WriteChange.AlteredTable(tableId))
        }
    }

    override fun commitInsert(tableId: Long, fragments: List<DucklakeWriteFragment>) {
        if (fragments.isEmpty()) {
            return
        }

        executeWriteTransaction("insert into table $tableId") { tx ->
            applyInsertFragments(tx, tableId, fragments)
            tx.recordChange(WriteChange.InsertedIntoTable(tableId, referencedColumnIds(fragments)))
        }
    }

    override fun commitAddFiles(tableId: Long, fragments: List<DucklakeWriteFragment>) {
        if (fragments.isEmpty()) {
            return
        }

        executeWriteTransaction("add files to table $tableId") { tx ->
            applyInsertFragments(tx, tableId, fragments)
            tx.recordChange(WriteChange.InsertedIntoTable(tableId, referencedColumnIds(fragments)))
        }
    }

    private fun applyInsertFragments(tx: DucklakeWriteTransaction, tableId: Long, fragments: List<DucklakeWriteFragment>) {
        val ctx = tx.dsl()
        val tabstats = DUCKLAKE_TABLE_STATS.`as`("tabstats")
        val file = DUCKLAKE_DATA_FILE.`as`("file")
        val partval = DUCKLAKE_FILE_PARTITION_VALUE.`as`("partval")
        val colstats = DUCKLAKE_FILE_COLUMN_STATS.`as`("colstats")
        val tabcolst = DUCKLAKE_TABLE_COLUMN_STATS.`as`("tabcolst")

        // Read current table stats (may not exist yet — DuckDB creates them on first insert).
        // Note: ducklake_table_stats has no PK/UNIQUE on table_id, so Postgres `ON CONFLICT`
        // isn't usable here; we do an explicit existence probe + INSERT-or-UPDATE.
        val existingStats: DucklakeTableStatsRecord? = ctx.selectFrom(tabstats)
            .where(tabstats.TABLE_ID.eq(tableId))
            .fetchOne()

        var runningRowId: Long = if (existingStats == null) 0L else orZero(existingStats.nextRowId)
        var totalRecords: Long = 0
        var totalFileSize: Long = 0
        val partitionValueRecords: MutableList<DucklakeFilePartitionValueRecord> = ArrayList()
        val fileColumnStatsRecords: MutableList<DucklakeFileColumnStatsRecord> = ArrayList()
        val dataFileRecords: MutableList<DucklakeDataFileRecord> = ArrayList()
        // Dedupe identical NameMap structures within this call. Upstream's
        // ducklake_add_data_files writes one ducklake_column_mapping row per
        // unique parquet schema seen in a batch; matching that here lets a
        // glob over homogeneous parquet files share one mapping_id.
        val nameMapToId: MutableMap<DucklakeNameMap, Long> = HashMap()
        val columnMappingRecords: MutableList<DucklakeColumnMappingRecord> = ArrayList()
        val nameMappingRecords: MutableList<DucklakeNameMappingRecord> = ArrayList()

        for (fragment in fragments) {
            val dataFileId = tx.allocateFileId()

            val dataFile = ctx.newRecord(file)
            dataFile.setDataFileId(dataFileId)
            dataFile.setTableId(tableId)
            dataFile.setBeginSnapshot(tx.getNewSnapshotId())
            // file_order: NULL matches DuckDB convention
            dataFile.setPath(fragment.path)
            dataFile.setPathIsRelative(fragment.pathIsRelative)
            dataFile.setFileFormat(fragment.fileFormat)
            dataFile.setRecordCount(fragment.recordCount)
            dataFile.setFileSizeBytes(fragment.fileSizeBytes)
            // footer_size is a hint column: SQL NULL means "no hint" and the reader
            // falls back to a blind tail read. A literal 0 is wrong and crashes DuckDB's
            // reader ("Invalid footer length"). Callers that don't compute the size
            // (today: add_files) pass 0; map it to NULL here.
            if (fragment.footerSize > 0) {
                dataFile.setFooterSize(fragment.footerSize)
            }
            dataFile.setRowIdStart(runningRowId)
            if (fragment.partitionId.isPresent) {
                dataFile.setPartitionId(fragment.partitionId.asLong)
            }
            if (fragment.nameMap.isPresent) {
                val nameMap = fragment.nameMap.get()
                var mappingId = nameMapToId[nameMap]
                if (mappingId == null) {
                    mappingId = tx.allocateCatalogId()
                    nameMapToId[nameMap] = mappingId
                    val cm = DucklakeColumnMappingRecord()
                    cm.setMappingId(mappingId)
                    cm.setTableId(tableId)
                    cm.setType("map_by_name")
                    columnMappingRecords.add(cm)
                    addNameMappingRows(nameMappingRecords, mappingId, nameMap.entries, null)
                }
                dataFile.setMappingId(mappingId)
            }
            dataFileRecords.add(dataFile)

            for ((key, value) in fragment.partitionValues) {
                val r = ctx.newRecord(partval)
                r.setTableId(tableId)
                r.setDataFileId(dataFileId)
                r.setPartitionKeyIndex(key.toLong())
                r.setPartitionValue(value)
                partitionValueRecords.add(r)
            }

            for (columnStats in fragment.columnStats) {
                val r = ctx.newRecord(colstats)
                r.setDataFileId(dataFileId)
                r.setTableId(tableId)
                r.setColumnId(columnStats.columnId)
                r.setColumnSizeBytes(columnStats.columnSizeBytes)
                r.setValueCount(columnStats.valueCount)
                r.setNullCount(columnStats.nullCount)
                r.setMinValue(columnStats.minValue.orElse(null))
                r.setMaxValue(columnStats.maxValue.orElse(null))
                // contains_nan: TRUE when set, SQL NULL otherwise (upstream convention).
                r.setContainsNan(if (columnStats.containsNan) java.lang.Boolean.TRUE else null)
                fileColumnStatsRecords.add(r)
            }

            runningRowId += fragment.recordCount
            totalRecords += fragment.recordCount
            totalFileSize += fragment.fileSizeBytes
        }

        // Name-map rows must land before the data_file rows that reference them via
        // mapping_id (no FK in upstream's schema, but kept in order for readability
        // and to make crash recovery deterministic).
        if (columnMappingRecords.isNotEmpty()) {
            ctx.batchInsert(columnMappingRecords).execute()
        }
        if (nameMappingRecords.isNotEmpty()) {
            ctx.batchInsert(nameMappingRecords).execute()
        }
        if (dataFileRecords.isNotEmpty()) {
            ctx.batchInsert(dataFileRecords).execute()
        }
        if (partitionValueRecords.isNotEmpty()) {
            ctx.batchInsert(partitionValueRecords).execute()
        }
        if (fileColumnStatsRecords.isNotEmpty()) {
            ctx.batchInsert(fileColumnStatsRecords).execute()
        }

        // Insert or update ducklake_table_stats (no PK/UNIQUE → can't use ON CONFLICT)
        if (existingStats != null) {
            ctx.update(tabstats)
                .set(tabstats.RECORD_COUNT, orZero(existingStats.recordCount) + totalRecords)
                .set(tabstats.NEXT_ROW_ID, orZero(existingStats.nextRowId) + totalRecords)
                .set(tabstats.FILE_SIZE_BYTES, orZero(existingStats.fileSizeBytes) + totalFileSize)
                .where(tabstats.TABLE_ID.eq(tableId))
                .execute()
        }
        else {
            ctx.insertInto(tabstats)
                .set(tabstats.TABLE_ID, tableId)
                .set(tabstats.RECORD_COUNT, totalRecords)
                .set(tabstats.NEXT_ROW_ID, totalRecords)
                .set(tabstats.FILE_SIZE_BYTES, totalFileSize)
                .execute()
        }

        // Upsert ducklake_table_column_stats: aggregate min/max/null across all fragments per column
        val columnAggregates: MutableMap<Long, AggregatedColumnStats> = LinkedHashMap()
        for (fragment in fragments) {
            for (colStats in fragment.columnStats) {
                columnAggregates.getOrPut(colStats.columnId) { AggregatedColumnStats() }.merge(colStats)
            }
        }

        val existingColumnStats: Set<Long> = loadExistingColumnStatsColumnIds(tx, tableId, columnAggregates.keys)
        val insertRecords: MutableList<DucklakeTableColumnStatsRecord> = ArrayList()
        for ((columnId, agg) in columnAggregates) {
            if (existingColumnStats.contains(columnId)) {
                // CASE-based typed min/max merge. Mirrors the original
                //   min_value = CASE WHEN min_value IS NULL THEN ? WHEN ? IS NULL THEN min_value
                //                    WHEN ? < min_value THEN ? ELSE min_value END
                // shape. Value parsing stays in Java (AggregatedColumnStats.merge); SQL only
                // picks between the existing column value and the aggregated candidate.
                //
                // contains_null / contains_nan: the original `col = (col OR ?)` is a no-op
                // when the aggregated flag is false (FALSE→FALSE, TRUE→TRUE, NULL→NULL in
                // Postgres), so we only emit the SET when the flag is true — in which case
                // the new value is unconditionally true. This sidesteps the missing
                // `Field<Boolean>.or(Field<Boolean>)` overload in jOOQ.
                val minParam: Param<String> = DSL.`val`(agg.minValue, tabcolst.MIN_VALUE.dataType)
                val maxParam: Param<String> = DSL.`val`(agg.maxValue, tabcolst.MAX_VALUE.dataType)
                var upd = ctx.update(tabcolst)
                    .set(
                        tabcolst.MIN_VALUE,
                        DSL.`when`(tabcolst.MIN_VALUE.isNull, minParam)
                            .`when`(minParam.isNull, tabcolst.MIN_VALUE)
                            .`when`(minParam.lt(tabcolst.MIN_VALUE), minParam)
                            .otherwise(tabcolst.MIN_VALUE),
                    )
                    .set(
                        tabcolst.MAX_VALUE,
                        DSL.`when`(tabcolst.MAX_VALUE.isNull, maxParam)
                            .`when`(maxParam.isNull, tabcolst.MAX_VALUE)
                            .`when`(maxParam.gt(tabcolst.MAX_VALUE), maxParam)
                            .otherwise(tabcolst.MAX_VALUE),
                    )
                if (agg.containsNull) {
                    upd = upd.set(tabcolst.CONTAINS_NULL, true)
                }
                if (agg.containsNan) {
                    upd = upd.set(tabcolst.CONTAINS_NAN, true)
                }
                upd.where(tabcolst.TABLE_ID.eq(tableId))
                    .and(tabcolst.COLUMN_ID.eq(columnId))
                    .execute()
            }
            else {
                val r = ctx.newRecord(tabcolst)
                r.setTableId(tableId)
                r.setColumnId(columnId)
                r.setContainsNull(agg.containsNull)
                // Asymmetric with contains_null: mirror the original INSERT which wrote
                // SQL NULL for contains_nan when false (and TRUE when true).
                r.setContainsNan(if (agg.containsNan) java.lang.Boolean.TRUE else null)
                r.setMinValue(agg.minValue)
                r.setMaxValue(agg.maxValue)
                insertRecords.add(r)
            }
        }
        if (insertRecords.isNotEmpty()) {
            ctx.batchInsert(insertRecords).execute()
        }
    }

    /**
     * Walks a [DucklakeNameMap] entry tree and emits one
     * `ducklake_name_mapping` row per node. Children point at their parent's
     * column_id (allocated here) so a reader can reconstruct the tree from the flat
     * rows. Mirrors upstream's `DuckLakeNameMapEntry` → row flattening in
     * `DuckLakeTransaction::AppendFiles` (via `WriteNameMap`).
     *
     * `column_id` is per-`mapping_id` (not global), so each top-level
     * invocation starts a fresh `long[1]` counter.
     */
    private fun addNameMappingRows(
        sink: MutableList<DucklakeNameMappingRecord>,
        mappingId: Long,
        entries: List<DucklakeNameMapEntry>,
        parentColumnId: Long?,
    ) {
        appendNameMappingRows(sink, mappingId, entries, parentColumnId, longArrayOf(1L))
    }

    private fun appendNameMappingRows(
        sink: MutableList<DucklakeNameMappingRecord>,
        mappingId: Long,
        entries: List<DucklakeNameMapEntry>,
        parentColumnId: Long?,
        nextColumnId: LongArray,
    ) {
        for (entry in entries) {
            val columnId = nextColumnId[0]++
            val r = DucklakeNameMappingRecord()
            r.setMappingId(mappingId)
            r.setColumnId(columnId)
            r.setSourceName(entry.sourceName)
            r.setTargetFieldId(entry.targetFieldId)
            r.setParentColumn(parentColumnId)
            // DuckDB's GetColumnMappings reader crashes on SQL NULL for is_partition —
            // the column's DDL has DEFAULT false, and upstream always writes a literal bool.
            // Mirror that contract: TRUE for hive-partition entries, FALSE for regular ones.
            r.setIsPartition(entry.isPartition)
            sink.add(r)
            if (entry.children.isNotEmpty()) {
                appendNameMappingRows(sink, mappingId, entry.children, columnId, nextColumnId)
            }
        }
    }

    override fun commitDelete(tableId: Long, deleteFragments: List<DucklakeDeleteFragment>) {
        if (deleteFragments.isEmpty()) {
            return
        }

        executeWriteTransaction("delete from table $tableId") { tx ->
            applyDeleteFragments(tx, tableId, deleteFragments)
            tx.recordChange(WriteChange.DeletedFromTable(tableId, referencedDataFileIds(deleteFragments)))
        }
    }

    override fun commitMerge(
        tableId: Long,
        deleteFragments: List<DucklakeDeleteFragment>,
        insertFragments: List<DucklakeWriteFragment>,
    ) {
        if (deleteFragments.isEmpty() && insertFragments.isEmpty()) {
            return
        }

        executeWriteTransaction("merge into table $tableId") { tx ->
            if (deleteFragments.isNotEmpty()) {
                applyDeleteFragments(tx, tableId, deleteFragments)
                tx.recordChange(WriteChange.DeletedFromTable(tableId, referencedDataFileIds(deleteFragments)))
            }
            if (insertFragments.isNotEmpty()) {
                applyInsertFragments(tx, tableId, insertFragments)
                tx.recordChange(WriteChange.InsertedIntoTable(tableId, referencedColumnIds(insertFragments)))
            }
        }
    }

    private fun applyDeleteFragments(tx: DucklakeWriteTransaction, tableId: Long, deleteFragments: List<DucklakeDeleteFragment>) {
        val ctx = tx.dsl()
        val delfile = DUCKLAKE_DELETE_FILE.`as`("delfile")
        val tabstats = DUCKLAKE_TABLE_STATS.`as`("tabstats")
        var totalNewDeleteCount: Long = 0
        val deleteFileRecords: MutableList<DucklakeDeleteFileRecord> = ArrayList()

        // End-snapshot any prior active delete files for the data_file_ids we're about to
        // write new delete files for. DuckLake's spec invariant is ≤1 active delete file per
        // data_file_id per snapshot (README:223, checkDeleteFileOverlap:1311-1312); the sink
        // unions prior-active positions with this commit's new positions into the new file,
        // so superseding the prior is correct (no rows resurrect — the new file carries the
        // union). Record-count math below uses the DELTA (newDeleteCount), not the union
        // total, because the prior's positions were already deducted at first commit.
        val touchedDataFileIds: MutableSet<Long> = HashSet()
        for (fragment in deleteFragments) {
            touchedDataFileIds.add(fragment.dataFileId)
        }
        if (touchedDataFileIds.isNotEmpty()) {
            ctx.update(delfile)
                .set(delfile.END_SNAPSHOT, tx.getNewSnapshotId())
                .where(delfile.DATA_FILE_ID.`in`(touchedDataFileIds))
                .and(delfile.END_SNAPSHOT.isNull)
                .execute()
        }

        for (fragment in deleteFragments) {
            val deleteFileId = tx.allocateFileId()

            val r = ctx.newRecord(delfile)
            r.setDeleteFileId(deleteFileId)
            r.setTableId(tableId)
            r.setBeginSnapshot(tx.getNewSnapshotId())
            r.setDataFileId(fragment.dataFileId)
            r.setPath(fragment.path)
            r.setPathIsRelative(true)
            r.setFormat("parquet")
            r.setDeleteCount(fragment.deleteCount)
            r.setFileSizeBytes(fragment.fileSizeBytes)
            // footer_size is a hint column: SQL NULL means "no hint" and the reader falls back
            // to a blind tail read; a literal 0 crashes DuckDB ("Invalid footer length"). The
            // merge sink sets footerSize=0 when footer serialization throws (writeDeleteFile),
            // so guard exactly as the insert path above does and map 0 to NULL.
            if (fragment.footerSize > 0) {
                r.setFooterSize(fragment.footerSize)
            }
            deleteFileRecords.add(r)

            totalNewDeleteCount += fragment.newDeleteCount
        }

        if (deleteFileRecords.isNotEmpty()) {
            ctx.batchInsert(deleteFileRecords).execute()
        }

        // Update table stats: decrement record count by the DELTA (positions added by this
        // commit only). GREATEST(0, …) defends against arithmetic underflow if stats were
        // ever inconsistent with the delete-file ledger.
        ctx.update(tabstats)
            .set(
                tabstats.RECORD_COUNT,
                DSL.greatest(DSL.inline(0L), tabstats.RECORD_COUNT.minus(totalNewDeleteCount)),
            )
            .where(tabstats.TABLE_ID.eq(tableId))
            .execute()
    }

    private class AggregatedColumnStats {
        @JvmField var containsNull: Boolean = false
        @JvmField var containsNan: Boolean = false
        @JvmField var minValue: String? = null
        @JvmField var maxValue: String? = null

        fun merge(stats: DucklakeFileColumnStats) {
            if (stats.nullCount > 0) {
                containsNull = true
            }
            if (stats.containsNan) {
                containsNan = true
            }
            if (stats.minValue.isPresent) {
                val v = stats.minValue.get()
                val current = minValue
                if (current == null || v.compareTo(current) < 0) {
                    minValue = v
                }
            }
            if (stats.maxValue.isPresent) {
                val v = stats.maxValue.get()
                val current = maxValue
                if (current == null || v.compareTo(current) > 0) {
                    maxValue = v
                }
            }
        }
    }

    override fun close() {
        hikariDataSource.close()
    }

    private fun resolveColumnType(column: DucklakeColumn, childrenByParent: Map<Long, List<DucklakeColumn>>): String {
        val columnType = column.columnType
        return when (columnType.lowercase(Locale.ROOT)) {
            "list" -> {
                val children = childrenByParent.getOrDefault(column.columnId, emptyList())
                if (children.size != 1) {
                    throw IllegalStateException("List column must have exactly one child column: ${column.columnName}")
                }
                "list<" + resolveColumnType(children[0], childrenByParent) + ">"
            }
            "struct" -> {
                val children = childrenByParent.getOrDefault(column.columnId, emptyList())
                val fields = children.joinToString(",") { child ->
                    child.columnName + ":" + resolveColumnType(child, childrenByParent)
                }
                "struct<$fields>"
            }
            "map" -> {
                val children = childrenByParent.getOrDefault(column.columnId, emptyList())
                if (children.size != 2) {
                    throw IllegalStateException("Map column must have exactly two child columns: ${column.columnName}")
                }
                "map<" + resolveColumnType(children[0], childrenByParent) + "," + resolveColumnType(children[1], childrenByParent) + ">"
            }
            else -> columnType
        }
    }

    private fun parseStatValue(columnType: String, value: String?): Comparable<*>? {
        return DucklakeStatTypes.parseStat(columnType, value)
    }

    private fun isWithinBounds(
        lowerBound: Comparable<*>?,
        upperBound: Comparable<*>?,
        minStat: Comparable<*>?,
        maxStat: Comparable<*>?,
    ): Boolean {
        val lowerVsMax = compareValues(lowerBound, maxStat)
        if (lowerVsMax.isPresent && lowerVsMax.asInt > 0) {
            return false
        }

        val upperVsMin = compareValues(upperBound, minStat)
        return upperVsMin.isEmpty || upperVsMin.asInt >= 0
    }

    @Suppress("UNCHECKED_CAST")
    private fun compareValues(left: Comparable<*>?, right: Comparable<*>?): OptionalInt {
        if (left == null || right == null) {
            return OptionalInt.empty()
        }
        return try {
            OptionalInt.of((left as Comparable<Any>).compareTo(right))
        }
        catch (e: RuntimeException) {
            // Type mismatch or non-comparable values: avoid pruning to prevent false negatives.
            OptionalInt.empty()
        }
    }

    companion object {
        private val log: System.Logger = System.getLogger(JdbcDucklakeCatalog::class.java.name)
        private const val CONFLICT_CHANGE_SUMMARY_LIMIT = 10

        // Optimistic-retry tuning. Defaults match the upstream DuckLake C++ extension
        // (`ducklake_max_retry_count` / `retry_wait_ms` / `retry_backoff` in
        // src/storage/ducklake_transaction.cpp), so behavior under contention matches
        // what callers familiar with upstream expect.
        private const val MAX_RETRY_COUNT = 10
        private const val INITIAL_RETRY_WAIT_MS: Long = 100
        private const val RETRY_BACKOFF_MULTIPLIER: Double = 1.5

        private val V7_UUIDS: TimeBasedEpochGenerator = Generators.timeBasedEpochGenerator()

        // Package-private (Java) → public-with-@JvmStatic so tests can pin the version
        // without reflection. Same-package callers (Java + Kotlin) use the unqualified
        // form `JdbcDucklakeCatalog.newCatalogUuid()`.
        @JvmStatic
        fun newCatalogUuid(): String {
            return V7_UUIDS.generate().toString()
        }

        private fun typedMin(a: String, b: String, columnType: String): String {
            return DucklakeStatTypes.min(a, b, columnType)
        }

        private fun typedMax(a: String, b: String, columnType: String): String {
            return DucklakeStatTypes.max(a, b, columnType)
        }

        private fun hasActiveView(tx: DucklakeWriteTransaction, schemaId: Long, viewName: String): Boolean {
            val view = DUCKLAKE_VIEW.`as`("view")
            return tx.dsl().fetchExists(
                DSL.selectOne()
                    .from(view)
                    .where(view.SCHEMA_ID.eq(schemaId))
                    .and(view.VIEW_NAME.eq(viewName))
                    .and(activeAt(view, tx.getCurrentSnapshotId())),
            )
        }

        private fun hasActiveTable(tx: DucklakeWriteTransaction, schemaId: Long, tableName: String): Boolean {
            val tab = DUCKLAKE_TABLE.`as`("tab")
            return tx.dsl().fetchExists(
                DSL.selectOne()
                    .from(tab)
                    .where(tab.SCHEMA_ID.eq(schemaId))
                    .and(tab.TABLE_NAME.eq(tableName))
                    .and(activeAt(tab, tx.getCurrentSnapshotId())),
            )
        }

        private fun referencedColumnIds(fragments: List<DucklakeWriteFragment>): Set<Long> {
            val result = HashSet<Long>()
            for (f in fragments) {
                for (cs in f.columnStats) {
                    result.add(cs.columnId)
                }
            }
            return result.toSet()
        }

        private fun referencedDataFileIds(fragments: List<DucklakeDeleteFragment>): Set<Long> {
            val result = HashSet<Long>()
            for (f in fragments) {
                result.add(f.dataFileId)
            }
            return result.toSet()
        }

        private fun runConflictMatrix(
            ctx: DSLContext,
            myChanges: List<WriteChange>,
            fromSnapshotExclusive: Long,
            toSnapshotInclusive: Long,
        ) {
            val snapchg = DUCKLAKE_SNAPSHOT_CHANGES.`as`("snapchg")
            val rows: List<String> = ctx.select(snapchg.CHANGES_MADE)
                .from(snapchg)
                .where(snapchg.SNAPSHOT_ID.gt(fromSnapshotExclusive))
                .and(snapchg.SNAPSHOT_ID.le(toSnapshotInclusive))
                .orderBy(snapchg.SNAPSHOT_ID)
                .fetch(snapchg.CHANGES_MADE)
            val other = InterveningChanges.parseAll(rows)
            ConflictMatrix.check(myChanges, other)

            // Finer-grained delete-vs-delete file-overlap check. Two transactions
            // that each write a delete file for the SAME data_file_id silently lose
            // one set of deletions (the second transaction's INSERT into
            // ducklake_delete_file end-snapshots the first). Upstream catches this
            // at ducklake_transaction.cpp:1259-1283 by intersecting MY new
            // delete-file data_file_ids with the set returned by
            // GetFilesDeletedOrDroppedAfterSnapshot.
            checkDeleteFileOverlap(ctx, myChanges, other, fromSnapshotExclusive, toSnapshotInclusive)
        }

        private fun checkDeleteFileOverlap(
            ctx: DSLContext,
            myChanges: List<WriteChange>,
            other: InterveningChanges,
            fromSnapshotExclusive: Long,
            toSnapshotInclusive: Long,
        ) {
            // Collect my data_file_ids whose tables also had intervening deletes.
            // Outside that overlap, no contention is possible.
            val myFileIds: MutableSet<Long> = HashSet()
            for (c in myChanges) {
                if (c is WriteChange.DeletedFromTable && other.tablesDeletedFrom.contains(c.tableId)) {
                    myFileIds.addAll(c.referencedDataFileIds)
                }
            }
            if (myFileIds.isEmpty()) {
                return
            }

            // Any delete file inserted in the intervening snapshot range that
            // targets one of my data_file_ids is a conflict. Use begin_snapshot
            // (when the row was inserted) to find intervening deletes.
            val delfile = DUCKLAKE_DELETE_FILE.`as`("delfile")
            val contendedFileIds: Set<Long> = ctx.select(delfile.DATA_FILE_ID)
                .from(delfile)
                .where(delfile.BEGIN_SNAPSHOT.gt(fromSnapshotExclusive))
                .and(delfile.BEGIN_SNAPSHOT.le(toSnapshotInclusive))
                .and(delfile.DATA_FILE_ID.`in`(myFileIds))
                .fetchSet(delfile.DATA_FILE_ID)
            if (contendedFileIds.isNotEmpty()) {
                throw LogicalConflictException(
                    "Transaction conflict - attempting to delete from data_file_id(s) " +
                        TreeSet(contendedFileIds) +
                        " - but another transaction also wrote delete files for the same" +
                        " data files. The second-committing delete would silently end-snapshot" +
                        " the first transaction's deletions, so this conflict is not retried.",
                )
            }
        }

        private fun readLatestSnapshotId(ctx: DSLContext): Long {
            val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
            val maxId: Long? = ctx.select(DSL.max(snap.SNAPSHOT_ID))
                .from(snap)
                .fetchOne(0, Long::class.java)
            return maxId ?: throw IllegalStateException("No snapshots found")
        }

        private fun hasTransactionConflict(throwable: Throwable): Boolean {
            var current: Throwable? = throwable
            while (current != null) {
                if (current is TransactionConflictException) {
                    return true
                }
                current = current.cause
            }
            return false
        }

        private fun isMetadataPrimaryKeyConflict(throwable: Throwable): Boolean {
            val sqlException = findSqlException(throwable)
            if (sqlException == null || !isDuplicateKeyViolation(sqlException)) {
                return false
            }

            val message = sqlException.message ?: return false

            val lowerMessage = message.lowercase(Locale.ENGLISH)
            if (lowerMessage.contains("_pkey")) {
                return true
            }

            return lowerMessage.contains("ducklake_snapshot.snapshot_id") ||
                lowerMessage.contains("ducklake_schema.schema_id") ||
                lowerMessage.contains("ducklake_table.table_id") ||
                lowerMessage.contains("ducklake_view.view_id") ||
                lowerMessage.contains("ducklake_column.column_id") ||
                lowerMessage.contains("ducklake_partition_info.partition_id") ||
                lowerMessage.contains("ducklake_data_file.data_file_id") ||
                lowerMessage.contains("ducklake_delete_file.delete_file_id")
        }

        private fun findSqlException(throwable: Throwable): SQLException? {
            var current: Throwable? = throwable
            while (current != null) {
                if (current is SQLException) {
                    return current
                }
                current = current.cause
            }
            return null
        }

        private fun isDuplicateKeyViolation(exception: SQLException): Boolean {
            val sqlState = exception.sqlState
            if ("23505" == sqlState) {
                return true
            }

            if (exception.errorCode == 19) {
                return true
            }

            val message = exception.message
            return message != null && (
                message.contains("duplicate key value violates unique constraint") ||
                    message.contains("UNIQUE constraint failed")
                )
        }

        // DuckLake's jOOQ codegen marks most BIGINT columns as nullable (no schema-level NOT NULL
        // constraint), so accessor methods return Long. The JDBC implementation historically used
        // ResultSet.getLong() which returns 0 on SQL NULL; preserve that fallback to keep fidelity
        // with records that don't populate optional columns (e.g. file_order).
        private fun orZero(value: Long?): Long {
            return value ?: 0L
        }

        private fun loadExistingColumnStatsColumnIds(
            tx: DucklakeWriteTransaction,
            tableId: Long,
            candidateColumnIds: Set<Long>,
        ): Set<Long> {
            if (candidateColumnIds.isEmpty()) {
                return emptySet()
            }
            val tabcolst = DUCKLAKE_TABLE_COLUMN_STATS.`as`("tabcolst")
            return tx.dsl().select(tabcolst.COLUMN_ID)
                .from(tabcolst)
                .where(tabcolst.TABLE_ID.eq(tableId))
                .and(tabcolst.COLUMN_ID.`in`(candidateColumnIds))
                .fetchSet(tabcolst.COLUMN_ID)
        }

        private fun toDucklakeColumn(r: DucklakeColumnRecord): DucklakeColumn {
            return DucklakeColumn(
                orZero(r.columnId),
                orZero(r.beginSnapshot),
                Optional.ofNullable(r.endSnapshot),
                orZero(r.tableId),
                orZero(r.columnOrder),
                r.columnName!!,
                r.columnType!!,
                java.lang.Boolean.TRUE == r.nullsAllowed,
                Optional.ofNullable(r.parentColumn),
            )
        }

        private fun toDucklakeSnapshot(r: DucklakeSnapshotRecord): DucklakeSnapshot {
            return DucklakeSnapshot(
                r.snapshotId,
                // snapshot_time is @Nullable in jOOQ. A bare deref NPEs with no context; name the
                // offending row so a partially-written / legacy snapshot is identifiable.
                checkNotNull(r.snapshotTime) { "snapshot_time is null for snapshot_id=${r.snapshotId}" }.toInstant(),
                orZero(r.schemaVersion),
                orZero(r.nextCatalogId),
                orZero(r.nextFileId),
            )
        }

        private fun toDucklakeSnapshotChange(r: DucklakeSnapshotChangesRecord): DucklakeSnapshotChange {
            return DucklakeSnapshotChange(
                r.snapshotId,
                Optional.ofNullable(r.changesMade),
                Optional.ofNullable(r.author),
                Optional.ofNullable(r.commitMessage),
                Optional.ofNullable(r.commitExtraInfo),
            )
        }

        private fun toDucklakeSchema(r: DucklakeSchemaRecord): DucklakeSchema {
            return DucklakeSchema(
                r.schemaId,
                r.schemaUuid!!,
                orZero(r.beginSnapshot),
                Optional.ofNullable(r.endSnapshot),
                r.schemaName!!,
                Optional.ofNullable(r.path),
                Optional.ofNullable(r.pathIsRelative),
            )
        }

        private fun toDucklakeTable(r: DucklakeTableRecord): DucklakeTable {
            return DucklakeTable(
                orZero(r.tableId),
                r.tableUuid!!,
                orZero(r.beginSnapshot),
                Optional.ofNullable(r.endSnapshot),
                orZero(r.schemaId),
                r.tableName!!,
                Optional.ofNullable(r.path),
                Optional.ofNullable(r.pathIsRelative),
            )
        }

        private fun toDucklakeTableStats(r: DucklakeTableStatsRecord): DucklakeTableStats {
            return DucklakeTableStats(
                orZero(r.tableId),
                orZero(r.recordCount),
                orZero(r.fileSizeBytes),
            )
        }

        private fun toDucklakeView(r: DucklakeViewRecord): DucklakeView {
            val endRaw: Long? = r.endSnapshot
            val endSnapshot: OptionalLong = if (endRaw == null) OptionalLong.empty() else OptionalLong.of(endRaw)
            val viewUuid: UUID? = r.viewUuid
            // The Kotlin-ported DucklakeView record declares viewUuid as non-null; the Java
            // side passed null straight through without checking. Mirror the Java shape via
            // `!!` so a null viewUuid surfaces as an NPE here, matching the new record
            // contract. (Reviewer note: kit step or DucklakeView record could widen to
            // String? to restore the exact prior pass-through.)
            return DucklakeView(
                orZero(r.viewId),
                viewUuid?.toString()!!,
                orZero(r.schemaId),
                r.viewName!!,
                r.sql!!,
                r.dialect!!,
                Optional.ofNullable(r.columnAliases),
                orZero(r.beginSnapshot),
                endSnapshot,
            )
        }
    }
}
