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
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_COLUMN_TAG
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DATA_FILE
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DELETE_FILE
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_FILES_SCHEDULED_FOR_DELETION
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_FILE_COLUMN_STATS
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_FILE_PARTITION_VALUE
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_FILE_VARIANT_STATS
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_INLINED_DATA_TABLES
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_MACRO
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_MACRO_IMPL
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_MACRO_PARAMETERS
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
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TAG
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
import java.time.OffsetDateTime
import java.time.ZoneOffset
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
        val configuredUrl: String = config.catalogDatabaseUrl!!
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
                configuredUrl, config.catalogDatabasePassword, config.dataPath)
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
            if (config.catalogDatabaseUser != null) {
                hikariConfig.username = config.catalogDatabaseUser
            }
            if (config.catalogDatabasePassword != null) {
                hikariConfig.password = config.catalogDatabasePassword
            }
            dialectInferenceUrl = configuredUrl
            metadataQuery = DirectMetadataQuery()
        }
        hikariConfig.maximumPoolSize = config.maxCatalogConnections
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
            config.catalogDatabaseUrl, dialect)
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
    fun forConnection(connection: Connection): DSLContext =
        DSL.using(connection, dialect, jooqSettings)

    override val currentSnapshotId: Long
        get() {
            val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
            val maxId: Long? = dsl.select(DSL.max(snap.SNAPSHOT_ID))
                .from(snap)
                .fetchOne(0, Long::class.java)
            return maxId ?: throw IllegalStateException("No snapshots found in ducklake_snapshot table")
        }

    override fun getSnapshot(snapshotId: Long): DucklakeSnapshot? {
        val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
        return dsl.selectFrom(snap)
            .where(snap.SNAPSHOT_ID.eq(snapshotId))
            .fetchOne()
            ?.let { toDucklakeSnapshot(it) }
    }

    override fun getSnapshotAtOrBefore(timestamp: Instant): DucklakeSnapshot? {
        val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
        // Push the predicate + ordering + limit into SQL so the database returns the single
        // matching row instead of materializing the whole snapshot table and filtering in Java.
        // !snapshotTime.isAfter(timestamp) is exactly snapshot_time <= timestamp; ordering stays
        // by snapshot_id DESC (independent of snapshot_time monotonicity), so this is identical
        // to the prior scan-and-findFirst. SNAPSHOT_TIME is an OffsetDateTime column; compare at
        // UTC, which preserves the instant the Java filter used (snapshotTime() == toInstant()).
        return dsl.selectFrom(snap)
            .where(snap.SNAPSHOT_TIME.le(timestamp.atOffset(ZoneOffset.UTC)))
            .orderBy(snap.SNAPSHOT_ID.desc())
            .limit(1)
            .fetchOne()
            ?.let { toDucklakeSnapshot(it) }
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

    override fun getSchema(schemaName: String, snapshotId: Long): DucklakeSchema? {
        val sch = DUCKLAKE_SCHEMA.`as`("sch")
        return dsl.selectFrom(sch)
            .where(sch.SCHEMA_NAME.eq(schemaName))
            .and(activeAt(sch, snapshotId))
            .fetchOne()
            ?.let { toDucklakeSchema(it) }
    }

    override fun listTables(schemaId: Long, snapshotId: Long): List<DucklakeTable> {
        val tab = DUCKLAKE_TABLE.`as`("tab")
        return dsl.selectFrom(tab)
            .where(tab.SCHEMA_ID.eq(schemaId))
            .and(activeAt(tab, snapshotId))
            .fetch { toDucklakeTable(it) }
    }

    override fun getTable(schemaName: String, tableName: String, snapshotId: Long): DucklakeTable? {
        val schema = getSchema(schemaName, snapshotId) ?: return null

        val tab = DUCKLAKE_TABLE.`as`("tab")
        return dsl.selectFrom(tab)
            .where(tab.SCHEMA_ID.eq(schema.schemaId))
            .and(tab.TABLE_NAME.eq(tableName))
            .and(activeAt(tab, snapshotId))
            .fetchOne()
            ?.let { toDucklakeTable(it) }
    }

    override fun getTableById(tableId: Long, snapshotId: Long): DucklakeTable? {
        val tab = DUCKLAKE_TABLE.`as`("tab")
        return dsl.selectFrom(tab)
            .where(tab.TABLE_ID.eq(tableId))
            .and(activeAt(tab, snapshotId))
            .fetchOne()
            ?.let { toDucklakeTable(it) }
    }

    override fun getTableColumns(tableId: Long, snapshotId: Long): List<DucklakeColumn> {
        val allColumns = fetchTableColumns(tableId, snapshotId)

        val childrenByParent: MutableMap<Long, MutableList<DucklakeColumn>> = mutableMapOf()
        for (column in allColumns) {
            column.parentColumn?.let { parent ->
                childrenByParent.getOrPut(parent) { mutableListOf() }.add(column)
            }
        }

        val topLevelColumns: MutableList<DucklakeColumn> = mutableListOf()
        for (column in allColumns) {
            if (column.parentColumn == null) {
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
                        null,
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
        val deleteFilePartialMax = delfile.PARTIAL_MAX.`as`("delete_file_partial_max")
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
                file.PARTIAL_MAX,
                deleteFilePath,
                deleteFilePathIsRelative,
                deleteFileFooterSize,
                delfile.FORMAT,
                deleteFilePartialMax,
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
                r.get(file.END_SNAPSHOT),
                orZero(r.get(file.FILE_ORDER)),
                r.get(dataFilePath),
                r.get(dataFilePathIsRelative) == true,
                r.get(file.FILE_FORMAT),
                orZero(r.get(file.RECORD_COUNT)),
                orZero(r.get(file.FILE_SIZE_BYTES)),
                orZero(r.get(dataFileFooterSize)),
                orZero(r.get(file.ROW_ID_START)),
                r.get(file.PARTITION_ID),
                r.get(deleteFilePath),
                r.get(deleteFilePathIsRelative),
                r.get(deleteFileFooterSize),
                r.get(delfile.FORMAT),
                r.get(file.MAPPING_ID),
                r.get(file.PARTIAL_MAX),
                r.get(deleteFilePartialMax),
            )
        }
    }

    override fun hasPartialDeleteFilesRequiringSnapshotFilter(tableId: Long, snapshotId: Long): Boolean {
        val delf = DUCKLAKE_DELETE_FILE.`as`("delf")
        return dsl.selectOne().from(delf)
            .where(delf.TABLE_ID.eq(tableId))
            .and(activeAt(delf, snapshotId))
            .and(delf.PARTIAL_MAX.isNotNull)
            .and(delf.PARTIAL_MAX.gt(snapshotId))
            // Both PARQUET (via _ducklake_internal_snapshot_id) and PUFFIN (via each blob's embedded
            // ducklake-snapshot-id) partial delete files are now snapshot-filtered on read
            // (DucklakeSplit.deleteFileSnapshotFilters). Anything else is an unknown format that
            // validateDeleteFileFormats already rejects — this stays only as a defensive double-gate.
            .and(delf.FORMAT.isNotNull)
            .and(DSL.lower(delf.FORMAT).ne("parquet"))
            .and(DSL.lower(delf.FORMAT).ne("puffin"))
            .limit(1)
            .fetch().isNotEmpty
    }

    override fun listReferencedFilePaths(tableId: Long): List<DucklakeFilePathRef> {
        val refs = mutableListOf<DucklakeFilePathRef>()
        val collect: (String?, Boolean?) -> Unit = { path, isRelative ->
            if (path != null) {
                refs.add(DucklakeFilePathRef(path, isRelative ?: false))
            }
        }
        // Data + delete files for this table at ANY snapshot (end-snapshotted rows included — the
        // physical files are still catalog-owned until cleanup, so they are NOT orphans).
        val df = DUCKLAKE_DATA_FILE.`as`("df")
        dsl.select(df.PATH, df.PATH_IS_RELATIVE).from(df).where(df.TABLE_ID.eq(tableId))
            .fetch { collect(it.value1(), it.value2()) }
        val delf = DUCKLAKE_DELETE_FILE.`as`("delf")
        dsl.select(delf.PATH, delf.PATH_IS_RELATIVE).from(delf).where(delf.TABLE_ID.eq(tableId))
            .fetch { collect(it.value1(), it.value2()) }
        // Files already scheduled for deletion (the two-phase pipeline owns these). The schedule
        // table has no table_id, so we include all of them; cross-table paths can't collide with a
        // single table's data-path listing anyway.
        val sched = DUCKLAKE_FILES_SCHEDULED_FOR_DELETION.`as`("sched")
        dsl.select(sched.PATH, sched.PATH_IS_RELATIVE).from(sched)
            .fetch { collect(it.value1(), it.value2()) }
        return refs
    }

    override fun listExpirableSnapshots(olderThan: Instant?, versions: Set<Long>?): List<Long> {
        val snap = DUCKLAKE_SNAPSHOT.`as`("snap")
        val maxId: Long = dsl.select(DSL.max(snap.SNAPSHOT_ID)).from(snap)
            .fetchOne(0, Long::class.java) ?: return emptyList()
        var cond: Condition = snap.SNAPSHOT_ID.ne(maxId) // the latest is never expirable
        when {
            versions != null -> cond = cond.and(snap.SNAPSHOT_ID.`in`(versions))
            olderThan != null -> cond = cond.and(
                snap.SNAPSHOT_TIME.lt(OffsetDateTime.ofInstant(olderThan, ZoneOffset.UTC)))
        }
        return dsl.select(snap.SNAPSHOT_ID).from(snap).where(cond)
            .orderBy(snap.SNAPSHOT_ID.asc())
            .fetch(snap.SNAPSHOT_ID)
    }

    override fun expireSnapshots(snapshotIds: Set<Long>): ExpireSnapshotsResult {
        if (snapshotIds.isEmpty()) {
            return ExpireSnapshotsResult(0, 0)
        }
        // Plain catalog transaction — destructive GC, no new snapshot (mirrors analyzeTable).
        dataSource.connection.use { conn ->
            conn.autoCommit = false
            try {
                val ctx = forConnection(conn)
                val maxId = readLatestSnapshotId(ctx)
                require(maxId !in snapshotIds) { "cannot expire the latest snapshot ($maxId)" }

                // Delete the snapshot rows FIRST so the half-open survivor tests below see only the
                // snapshots that REMAIN.
                metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_SNAPSHOT)
                    .where(DUCKLAKE_SNAPSHOT.SNAPSHOT_ID.`in`(snapshotIds)))
                metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_SNAPSHOT_CHANGES)
                    .where(DUCKLAKE_SNAPSHOT_CHANGES.SNAPSHOT_ID.`in`(snapshotIds)))

                val deadTableIds: List<Long> = findDeadTableIds(ctx)
                val tableDataPathCache = HashMap<Long, String?>()
                var scheduledCount = 0

                // Dead data files: end-snapshotted with no surviving snapshot in [begin,end), OR
                // belonging to a fully-expired dropped table (its files may have end_snapshot=NULL).
                val deadData = findDeadDataFiles(ctx, deadTableIds)
                for (f in deadData) {
                    if (scheduleFile(ctx, f, tableDataPathCache)) {
                        scheduledCount++
                    }
                }
                val deadDataIds = deadData.map { it.fileId }
                if (deadDataIds.isNotEmpty()) {
                    metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_FILE_COLUMN_STATS)
                        .where(DUCKLAKE_FILE_COLUMN_STATS.DATA_FILE_ID.`in`(deadDataIds)))
                    metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_FILE_VARIANT_STATS)
                        .where(DUCKLAKE_FILE_VARIANT_STATS.DATA_FILE_ID.`in`(deadDataIds)))
                    metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_FILE_PARTITION_VALUE)
                        .where(DUCKLAKE_FILE_PARTITION_VALUE.DATA_FILE_ID.`in`(deadDataIds)))
                    metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_DATA_FILE)
                        .where(DUCKLAKE_DATA_FILE.DATA_FILE_ID.`in`(deadDataIds)))
                }

                // Dead delete files: same survivor test, or orphaned by a just-removed data file,
                // or belonging to a dead table.
                val deadDelete = findDeadDeleteFiles(ctx, deadTableIds, deadDataIds)
                for (f in deadDelete) {
                    if (scheduleFile(ctx, f, tableDataPathCache)) {
                        scheduledCount++
                    }
                }
                val deadDeleteIds = deadDelete.map { it.fileId }
                if (deadDeleteIds.isNotEmpty()) {
                    metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_DELETE_FILE)
                        .where(DUCKLAKE_DELETE_FILE.DELETE_FILE_ID.`in`(deadDeleteIds)))
                }

                // GC the metadata rows of fully-expired DROPPED tables (every row dead — see
                // findDeadTableIds). Reuses the already-validated deadTableIds, so it can't touch a
                // live table.
                if (deadTableIds.isNotEmpty()) {
                    deleteDeadTableMetadata(ctx, deadTableIds)
                }
                // GC fully-expired schema/view/macro rows + name-mapping rows orphaned by the
                // table GC above. Pure tidiness (no file leak either way) but completes the catalog
                // sweep so a long-lived warehouse doesn't accumulate dead metadata.
                deleteDeadSchemaViewMacroMetadata(ctx)

                conn.commit()
                return ExpireSnapshotsResult(snapshotIds.size, scheduledCount)
            }
            catch (e: Exception) {
                conn.rollback()
                throw e
            }
        }
    }

    /** A dead data/delete file row carrying just what scheduling needs. */
    private data class DeadFile(val fileId: Long, val tableId: Long, val path: String, val pathIsRelative: Boolean)

    /** Deletes every `table_id`-keyed metadata row for fully-expired dropped tables. */
    private fun deleteDeadTableMetadata(ctx: DSLContext, deadTableIds: List<Long>) {
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_TABLE_STATS).where(DUCKLAKE_TABLE_STATS.TABLE_ID.`in`(deadTableIds)))
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_TABLE_COLUMN_STATS).where(DUCKLAKE_TABLE_COLUMN_STATS.TABLE_ID.`in`(deadTableIds)))
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_PARTITION_INFO).where(DUCKLAKE_PARTITION_INFO.TABLE_ID.`in`(deadTableIds)))
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_PARTITION_COLUMN).where(DUCKLAKE_PARTITION_COLUMN.TABLE_ID.`in`(deadTableIds)))
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_COLUMN_TAG).where(DUCKLAKE_COLUMN_TAG.TABLE_ID.`in`(deadTableIds)))
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_COLUMN).where(DUCKLAKE_COLUMN.TABLE_ID.`in`(deadTableIds)))
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_SORT_EXPRESSION).where(DUCKLAKE_SORT_EXPRESSION.TABLE_ID.`in`(deadTableIds)))
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_SORT_INFO).where(DUCKLAKE_SORT_INFO.TABLE_ID.`in`(deadTableIds)))
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_SCHEMA_VERSIONS).where(DUCKLAKE_SCHEMA_VERSIONS.TABLE_ID.`in`(deadTableIds)))
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_INLINED_DATA_TABLES).where(DUCKLAKE_INLINED_DATA_TABLES.TABLE_ID.`in`(deadTableIds)))
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_COLUMN_MAPPING).where(DUCKLAKE_COLUMN_MAPPING.TABLE_ID.`in`(deadTableIds)))
        // The ducklake_table rows last (others reference table_id).
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_TABLE).where(DUCKLAKE_TABLE.TABLE_ID.`in`(deadTableIds)))
    }

    private fun findDeadTableIds(ctx: DSLContext): List<Long> {
        val t1 = DUCKLAKE_TABLE.`as`("t1")
        val t2 = DUCKLAKE_TABLE.`as`("t2")
        val surv = DUCKLAKE_SNAPSHOT.`as`("surv")
        val surv2 = DUCKLAKE_SNAPSHOT.`as`("surv2")
        return ctx.select(t1.TABLE_ID).from(t1)
            .where(t1.END_SNAPSHOT.isNotNull)
            .and(DSL.notExists(ctx.selectOne().from(surv)
                .where(surv.SNAPSHOT_ID.ge(t1.BEGIN_SNAPSHOT)).and(surv.SNAPSHOT_ID.lt(t1.END_SNAPSHOT))))
            .and(DSL.notExists(ctx.selectOne().from(t2)
                .where(t2.TABLE_ID.eq(t1.TABLE_ID))
                .and(t2.END_SNAPSHOT.isNull.or(DSL.exists(ctx.selectOne().from(surv2)
                    .where(surv2.SNAPSHOT_ID.ge(t2.BEGIN_SNAPSHOT)).and(surv2.SNAPSHOT_ID.lt(t2.END_SNAPSHOT)))))))
            .fetch(t1.TABLE_ID)
            .distinct()
    }

    /**
     * GC of dead schema/view/macro metadata + name-mapping rows orphaned by [deleteDeadTableMetadata].
     * Views and macros are versioned like tables (same half-open survivor test); a schema carries a
     * single PK row. Runs AFTER the table-metadata GC so it sees the post-cleanup state (a dead
     * schema can only be removed once its tables' rows are gone; orphaned name mappings are those
     * whose `mapping_id` lost its `ducklake_column_mapping` owner). All "no file leak" tidiness.
     */
    private fun deleteDeadSchemaViewMacroMetadata(ctx: DSLContext) {
        val deadViewIds = findDeadViewIds(ctx)
        if (deadViewIds.isNotEmpty()) {
            metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_VIEW).where(DUCKLAKE_VIEW.VIEW_ID.`in`(deadViewIds)))
        }
        val deadMacroIds = findDeadMacroIds(ctx)
        if (deadMacroIds.isNotEmpty()) {
            metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_MACRO_IMPL).where(DUCKLAKE_MACRO_IMPL.MACRO_ID.`in`(deadMacroIds)))
            metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_MACRO_PARAMETERS).where(DUCKLAKE_MACRO_PARAMETERS.MACRO_ID.`in`(deadMacroIds)))
            metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_MACRO).where(DUCKLAKE_MACRO.MACRO_ID.`in`(deadMacroIds)))
        }
        // Name-mapping rows whose mapping_id no longer has a column_mapping owner (the table GC
        // removed it). mapping_id originates in ducklake_column_mapping, so an unreferenced one is
        // dead. Routed through a NOT IN subquery on the surviving column_mapping owners.
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_NAME_MAPPING)
            .where(DUCKLAKE_NAME_MAPPING.MAPPING_ID.notIn(
                ctx.select(DUCKLAKE_COLUMN_MAPPING.MAPPING_ID).from(DUCKLAKE_COLUMN_MAPPING))))
        // Dead schemas last: every row dead AND nothing (live or dead-but-surviving) still references
        // schema_id. The survivor test already implies emptiness, but the extra guard keeps the
        // delete from ever stranding a referencing row.
        val deadSchemaIds = findDeadSchemaIds(ctx)
        if (deadSchemaIds.isNotEmpty()) {
            val tab = DUCKLAKE_TABLE.`as`("tab")
            val vw = DUCKLAKE_VIEW.`as`("vw")
            val mac = DUCKLAKE_MACRO.`as`("mac")
            val referenced: Set<Long> = (
                ctx.select(tab.SCHEMA_ID).from(tab).where(tab.SCHEMA_ID.`in`(deadSchemaIds)).fetch(tab.SCHEMA_ID) +
                ctx.select(vw.SCHEMA_ID).from(vw).where(vw.SCHEMA_ID.`in`(deadSchemaIds)).fetch(vw.SCHEMA_ID) +
                ctx.select(mac.SCHEMA_ID).from(mac).where(mac.SCHEMA_ID.`in`(deadSchemaIds)).fetch(mac.SCHEMA_ID)
                ).filterNotNull().toSet()
            val removable = deadSchemaIds.filterNot { it in referenced }
            if (removable.isNotEmpty()) {
                metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_SCHEMA).where(DUCKLAKE_SCHEMA.SCHEMA_ID.`in`(removable)))
            }
        }
    }

    private fun findDeadViewIds(ctx: DSLContext): List<Long> {
        val v1 = DUCKLAKE_VIEW.`as`("v1")
        val v2 = DUCKLAKE_VIEW.`as`("v2")
        val surv = DUCKLAKE_SNAPSHOT.`as`("surv")
        val surv2 = DUCKLAKE_SNAPSHOT.`as`("surv2")
        return ctx.select(v1.VIEW_ID).from(v1)
            .where(v1.END_SNAPSHOT.isNotNull)
            .and(DSL.notExists(ctx.selectOne().from(surv)
                .where(surv.SNAPSHOT_ID.ge(v1.BEGIN_SNAPSHOT)).and(surv.SNAPSHOT_ID.lt(v1.END_SNAPSHOT))))
            .and(DSL.notExists(ctx.selectOne().from(v2)
                .where(v2.VIEW_ID.eq(v1.VIEW_ID))
                .and(v2.END_SNAPSHOT.isNull.or(DSL.exists(ctx.selectOne().from(surv2)
                    .where(surv2.SNAPSHOT_ID.ge(v2.BEGIN_SNAPSHOT)).and(surv2.SNAPSHOT_ID.lt(v2.END_SNAPSHOT)))))))
            .fetch(v1.VIEW_ID)
            .distinct()
    }

    private fun findDeadMacroIds(ctx: DSLContext): List<Long> {
        val m1 = DUCKLAKE_MACRO.`as`("m1")
        val m2 = DUCKLAKE_MACRO.`as`("m2")
        val surv = DUCKLAKE_SNAPSHOT.`as`("surv")
        val surv2 = DUCKLAKE_SNAPSHOT.`as`("surv2")
        return ctx.select(m1.MACRO_ID).from(m1)
            .where(m1.END_SNAPSHOT.isNotNull)
            .and(DSL.notExists(ctx.selectOne().from(surv)
                .where(surv.SNAPSHOT_ID.ge(m1.BEGIN_SNAPSHOT)).and(surv.SNAPSHOT_ID.lt(m1.END_SNAPSHOT))))
            .and(DSL.notExists(ctx.selectOne().from(m2)
                .where(m2.MACRO_ID.eq(m1.MACRO_ID))
                .and(m2.END_SNAPSHOT.isNull.or(DSL.exists(ctx.selectOne().from(surv2)
                    .where(surv2.SNAPSHOT_ID.ge(m2.BEGIN_SNAPSHOT)).and(surv2.SNAPSHOT_ID.lt(m2.END_SNAPSHOT)))))))
            .fetch(m1.MACRO_ID)
            .distinct()
    }

    private fun findDeadSchemaIds(ctx: DSLContext): List<Long> {
        val s1 = DUCKLAKE_SCHEMA.`as`("s1")
        val surv = DUCKLAKE_SNAPSHOT.`as`("surv")
        // ducklake_schema has a PK on schema_id (one row per id), so a dead schema is simply an
        // end-snapshotted row with no surviving snapshot in [begin,end).
        return ctx.select(s1.SCHEMA_ID).from(s1)
            .where(s1.END_SNAPSHOT.isNotNull)
            .and(DSL.notExists(ctx.selectOne().from(surv)
                .where(surv.SNAPSHOT_ID.ge(s1.BEGIN_SNAPSHOT)).and(surv.SNAPSHOT_ID.lt(s1.END_SNAPSHOT))))
            .fetch(s1.SCHEMA_ID)
            .distinct()
    }

    private fun findDeadDataFiles(ctx: DSLContext, deadTableIds: List<Long>): List<DeadFile> {
        val file = DUCKLAKE_DATA_FILE.`as`("df")
        val surv = DUCKLAKE_SNAPSHOT.`as`("surv")
        val noSurvivor = file.END_SNAPSHOT.isNotNull.and(DSL.notExists(ctx.selectOne().from(surv)
            .where(surv.SNAPSHOT_ID.ge(file.BEGIN_SNAPSHOT)).and(surv.SNAPSHOT_ID.lt(file.END_SNAPSHOT))))
        return ctx.select(file.DATA_FILE_ID, file.TABLE_ID, file.PATH, file.PATH_IS_RELATIVE)
            .from(file)
            .where(file.TABLE_ID.`in`(deadTableIds).or(noSurvivor))
            .fetch { DeadFile(it.value1(), it.value2(), it.value3(), it.value4() ?: false) }
    }

    private fun findDeadDeleteFiles(ctx: DSLContext, deadTableIds: List<Long>, deadDataIds: List<Long>): List<DeadFile> {
        val file = DUCKLAKE_DELETE_FILE.`as`("delf")
        val surv = DUCKLAKE_SNAPSHOT.`as`("surv")
        val noSurvivor = file.END_SNAPSHOT.isNotNull.and(DSL.notExists(ctx.selectOne().from(surv)
            .where(surv.SNAPSHOT_ID.ge(file.BEGIN_SNAPSHOT)).and(surv.SNAPSHOT_ID.lt(file.END_SNAPSHOT))))
        return ctx.select(file.DELETE_FILE_ID, file.TABLE_ID, file.PATH, file.PATH_IS_RELATIVE)
            .from(file)
            .where(file.TABLE_ID.`in`(deadTableIds).or(file.DATA_FILE_ID.`in`(deadDataIds)).or(noSurvivor))
            .fetch { DeadFile(it.value1(), it.value2(), it.value3(), it.value4() ?: false) }
    }

    /**
     * Schedules a dead file into `ducklake_files_scheduled_for_deletion` as an ABSOLUTE path
     * (path_is_relative=false), resolving table-relative paths against the file's table data dir.
     * Returns false (skips) when the path can't be resolved (no data_path root) — the file then
     * simply becomes an orphan reclaimable by remove_orphan_files. Cross-engine readable: DuckLake
     * cleanup reads absolute (path_is_relative=false) rows directly.
     */
    private fun scheduleFile(ctx: DSLContext, f: DeadFile, cache: HashMap<Long, String?>): Boolean {
        val absolute: String = if (!f.pathIsRelative) {
            f.path
        }
        else {
            val tableDataPath = cache.getOrPut(f.tableId) { resolveTableDataPathById(ctx, f.tableId) }
                ?: return false
            joinPaths(tableDataPath, f.path)
        }
        metadata.execute(ctx, ctx.insertInto(DUCKLAKE_FILES_SCHEDULED_FOR_DELETION)
            .set(DUCKLAKE_FILES_SCHEDULED_FOR_DELETION.DATA_FILE_ID, f.fileId)
            .set(DUCKLAKE_FILES_SCHEDULED_FOR_DELETION.PATH, absolute)
            .set(DUCKLAKE_FILES_SCHEDULED_FOR_DELETION.PATH_IS_RELATIVE, false)
            .set(DUCKLAKE_FILES_SCHEDULED_FOR_DELETION.SCHEDULE_START, DSL.currentOffsetDateTime()))
        return true
    }

    /** Resolves a table's absolute data dir (root + schema.path + table.path), latest row wins. */
    private fun resolveTableDataPathById(ctx: DSLContext, tableId: Long): String? {
        val root = readDataPath(ctx) ?: return null
        val tab = DUCKLAKE_TABLE.`as`("tab")
        val tableRow = ctx.select(tab.SCHEMA_ID, tab.PATH, tab.PATH_IS_RELATIVE).from(tab)
            .where(tab.TABLE_ID.eq(tableId)).orderBy(tab.BEGIN_SNAPSHOT.desc()).limit(1).fetchOne()
            ?: return null
        val sch = DUCKLAKE_SCHEMA.`as`("sch")
        val schemaRow = ctx.select(sch.PATH, sch.PATH_IS_RELATIVE).from(sch)
            .where(sch.SCHEMA_ID.eq(tableRow.value1())).orderBy(sch.BEGIN_SNAPSHOT.desc()).limit(1).fetchOne()
        val schemaDataPath = resolveScopedPath(schemaRow?.value1(), schemaRow?.value2(), root)
        return resolveScopedPath(tableRow.value2(), tableRow.value3(), schemaDataPath)
    }

    private fun readDataPath(ctx: DSLContext): String? {
        val meta = DUCKLAKE_METADATA.`as`("meta")
        return ctx.select(meta.VALUE).from(meta).where(meta.KEY.eq("data_path")).fetchOne(meta.VALUE)
    }

    override fun listFilesScheduledForDeletion(olderThan: Instant): List<DucklakeScheduledFile> {
        val sched = DUCKLAKE_FILES_SCHEDULED_FOR_DELETION.`as`("sched")
        return dsl.select(sched.DATA_FILE_ID, sched.PATH, sched.PATH_IS_RELATIVE).from(sched)
            .where(sched.SCHEDULE_START.lt(OffsetDateTime.ofInstant(olderThan, ZoneOffset.UTC)))
            .fetch { DucklakeScheduledFile(it.value1() ?: -1L, it.value2() ?: "", it.value3() ?: false) }
            .filter { it.path.isNotEmpty() }
    }

    override fun removeScheduledFileRows(dataFileIds: Collection<Long>) {
        if (dataFileIds.isEmpty()) {
            return
        }
        val sched = DUCKLAKE_FILES_SCHEDULED_FOR_DELETION
        dsl.deleteFrom(sched).where(sched.DATA_FILE_ID.`in`(dataFileIds)).execute()
    }

    private fun resolveScopedPath(path: String?, isRelative: Boolean?, parentPath: String): String {
        if (path.isNullOrBlank()) {
            return parentPath
        }
        return if (isRelative == true) joinPaths(parentPath, path) else path
    }

    private fun joinPaths(parent: String, child: String): String =
        if (parent.endsWith("/")) "$parent$child" else "$parent/$child"

    override fun getLatestDataFileFormat(tableId: Long, snapshotId: Long): String? {
        // "Latest" = highest data_file_id among rows still active at the requested snapshot.
        // data_file_id is allocated from a monotonic catalog sequence at insert time, so DESC
        // order on it picks the most recently committed file (cross-snapshot, cross-partition).
        val file = DUCKLAKE_DATA_FILE.`as`("file")
        return dsl.select(file.FILE_FORMAT)
            .from(file)
            .where(file.TABLE_ID.eq(tableId))
            .and(activeAt(file, snapshotId))
            .orderBy(file.DATA_FILE_ID.desc())
            .limit(1)
            .fetchOne(file.FILE_FORMAT)
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

    override fun getTableStats(tableId: Long): DucklakeTableStats? {
        val tabstats = DUCKLAKE_TABLE_STATS.`as`("tabstats")
        return dsl.selectFrom(tabstats)
            .where(tabstats.TABLE_ID.eq(tableId))
            .fetchOne()
            ?.let { toDucklakeTableStats(it) }
    }

    override fun getColumnStats(tableId: Long, snapshotId: Long): List<DucklakeColumnStats> {
        val columnTypes: Map<Long, String> = getTableColumns(tableId, snapshotId)
            .associate { it.columnId to it.columnType }

        val countAccumulators: MutableMap<Long, LongArray> = mutableMapOf() // [valueCount, nullCount, sizeBytes]
        val minAccumulators: MutableMap<Long, String> = mutableMapOf()
        val maxAccumulators: MutableMap<Long, String> = mutableMapOf()

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

        val result: MutableList<DucklakeColumnStats> = mutableListOf()
        for ((columnId, counts) in countAccumulators) {
            result.add(
                DucklakeColumnStats(
                    columnId,
                    counts[0],
                    counts[1],
                    counts[2],
                    minAccumulators[columnId],
                    maxAccumulators[columnId],
                ),
            )
        }

        return result
    }

    override fun analyzeTable(tableId: Long, rowCount: Long) {
        // Stats tables are mutable, non-snapshot-versioned side tables, so this is a plain catalog
        // transaction — no new snapshot, no `changes_made` entry (see the interface contract). It
        // mirrors `attemptWriteTransaction`'s connection handling without the snapshot/conflict
        // machinery.
        dataSource.connection.use { conn ->
            conn.autoCommit = false
            try {
                val ctx = forConnection(conn)
                val snapshotId = readLatestSnapshotId(ctx)
                recomputeTableStats(ctx, tableId, snapshotId, rowCount)
                recomputeTableColumnStats(ctx, tableId, snapshotId)
                conn.commit()
            }
            catch (e: Exception) {
                conn.rollback()
                throw e
            }
        }
    }

    /**
     * `ducklake_table_stats`: set record_count to the live [rowCount], recompute file_size_bytes
     * from the active data files, and PRESERVE next_row_id (the row-id allocator high-water mark).
     * No PK/UNIQUE on table_id → explicit probe + INSERT-or-UPDATE (matches the write path).
     */
    private fun recomputeTableStats(ctx: DSLContext, tableId: Long, snapshotId: Long, rowCount: Long) {
        val tabstats = DUCKLAKE_TABLE_STATS.`as`("tabstats")
        val file = DUCKLAKE_DATA_FILE.`as`("file")
        // Sum the active data files' byte sizes (single-table read — plain ctx).
        val totalFileSize: Long = ctx.select(file.FILE_SIZE_BYTES)
            .from(file)
            .where(file.TABLE_ID.eq(tableId))
            .and(activeAt(file, snapshotId))
            .fetch(file.FILE_SIZE_BYTES)
            .sumOf { orZero(it) }

        val existing: DucklakeTableStatsRecord? = ctx.selectFrom(tabstats)
            .where(tabstats.TABLE_ID.eq(tableId))
            .fetchOne()
        if (existing != null) {
            ctx.update(tabstats)
                .set(tabstats.RECORD_COUNT, rowCount)
                .set(tabstats.FILE_SIZE_BYTES, totalFileSize)
                .where(tabstats.TABLE_ID.eq(tableId))
                .execute()
        }
        else {
            ctx.insertInto(tabstats)
                .set(tabstats.TABLE_ID, tableId)
                .set(tabstats.RECORD_COUNT, rowCount)
                // No prior row (table never went through the insert path): seed the row-id
                // allocator high-water mark at the live row count.
                .set(tabstats.NEXT_ROW_ID, rowCount)
                .set(tabstats.FILE_SIZE_BYTES, totalFileSize)
                .execute()
        }
    }

    /**
     * `ducklake_table_column_stats`: full replace with aggregates freshly recomputed from the
     * active data files' authoritative per-file stats. Tightens any min/max that incremental
     * maintenance left stale after a delete.
     */
    private fun recomputeTableColumnStats(ctx: DSLContext, tableId: Long, snapshotId: Long) {
        val tabcolst = DUCKLAKE_TABLE_COLUMN_STATS.`as`("tabcolst")
        val rows = aggregateActiveColumnStats(ctx, tableId, snapshotId)
        // DELETE routed through `metadata` for Quack (matches dropTable's metadata-table delete).
        metadata.execute(
            ctx,
            ctx.deleteFrom(tabcolst).where(tabcolst.TABLE_ID.eq(tableId)),
        )
        if (rows.isNotEmpty()) {
            ctx.batchInsert(rows).execute()
        }
    }

    /**
     * Fold the active data files' per-file column stats (`ducklake_file_column_stats`) into one
     * `ducklake_table_column_stats` row per column, using typed (numeric, not lexicographic)
     * min/max comparison. A column with no active per-file stats produces no row.
     */
    private fun aggregateActiveColumnStats(
        ctx: DSLContext,
        tableId: Long,
        snapshotId: Long,
    ): List<DucklakeTableColumnStatsRecord> {
        val tabcolst = DUCKLAKE_TABLE_COLUMN_STATS.`as`("tabcolst")
        val colstats = DUCKLAKE_FILE_COLUMN_STATS.`as`("colstats")
        val file = DUCKLAKE_DATA_FILE.`as`("file")
        // Column types drive the typed min/max fold.
        val columnTypes: Map<Long, String> = getTableColumns(tableId, snapshotId)
            .associate { it.columnId to it.columnType }

        val seenColumns: MutableSet<Long> = linkedSetOf()
        val containsNull: MutableMap<Long, Boolean> = mutableMapOf()
        val containsNan: MutableMap<Long, Boolean> = mutableMapOf()
        val mins: MutableMap<Long, String> = mutableMapOf()
        val maxs: MutableMap<Long, String> = mutableMapOf()
        // Multi-table JOIN — routed through `metadata` for Quack compatibility.
        metadata.fetch(
            ctx,
            ctx.select(
                colstats.COLUMN_ID,
                colstats.NULL_COUNT,
                colstats.CONTAINS_NAN,
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
            seenColumns.add(columnId)
            if (orZero(r.get(colstats.NULL_COUNT)) > 0) {
                containsNull[columnId] = true
            }
            if (r.get(colstats.CONTAINS_NAN) == true) {
                containsNan[columnId] = true
            }
            val columnType = columnTypes.getOrDefault(columnId, "")
            r.get(colstats.MIN_VALUE)?.let { v ->
                mins.merge(columnId, v) { a, b -> typedMin(a, b, columnType) }
            }
            r.get(colstats.MAX_VALUE)?.let { v ->
                maxs.merge(columnId, v) { a, b -> typedMax(a, b, columnType) }
            }
            null
        }

        return seenColumns.map { columnId ->
            ctx.newRecord(tabcolst).apply {
                setTableId(tableId)
                setColumnId(columnId)
                setContainsNull(containsNull.getOrDefault(columnId, false))
                // Asymmetric with contains_null: TRUE when set, SQL NULL otherwise (upstream convention).
                setContainsNan(if (containsNan.getOrDefault(columnId, false)) java.lang.Boolean.TRUE else null)
                setMinValue(mins[columnId])
                setMaxValue(maxs[columnId])
            }
        }
    }

    override fun getPartitionSpecs(tableId: Long, snapshotId: Long): List<DucklakePartitionSpec> {
        val fieldsByPartition: MutableMap<Long, MutableList<DucklakePartitionField>> = linkedMapOf()
        val tableIdByPartition: MutableMap<Long, Long> = mutableMapOf()

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
            fieldsByPartition.getOrPut(partitionId) { mutableListOf() }
                .add(
                    DucklakePartitionField(
                        orZero(r.get(partcol.PARTITION_KEY_INDEX)).toInt(),
                        orZero(r.get(partcol.COLUMN_ID)),
                        parsed.transform,
                        if (parsed.arity.isPresent) parsed.arity.asInt else null,
                    ),
                )
            null // mapper return discarded — using fold-into-maps idiom
        }

        val specs: MutableList<DucklakePartitionSpec> = mutableListOf()
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
        val result: MutableMap<Long, MutableList<DucklakeFilePartitionValue>> = mutableMapOf()

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
            result.getOrPut(dataFileId) { mutableListOf() }
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

    override fun getNameMaps(mappingIds: Set<Long>): Map<Long, Map<Long, String>> {
        if (mappingIds.isEmpty()) {
            return emptyMap()
        }
        val nm = DUCKLAKE_NAME_MAPPING.`as`("nm")
        val result: MutableMap<Long, MutableMap<Long, String>> = mutableMapOf()
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
                    result.getOrPut(mappingId) { mutableMapOf() }[fieldId] = sourceName
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
                .mapNotNull { r ->
                    val rowTableId = orZero(r.get(inlined.TABLE_ID))
                    val rowSchemaVersion = orZero(r.get(inlined.SCHEMA_VERSION))
                    val inlinedTable = InlinedDataTable.of(rowTableId, rowSchemaVersion)
                    // One EXISTS probe per schema version yields BOTH bits callers need: a
                    // DataAccessException means the catalog metadata points to a dropped/
                    // non-materialized table (skip it, as the old existsAsTable filter did);
                    // otherwise the boolean tells whether any rows are live at this snapshot.
                    // Carrying hasLiveRows here lets split planning / hasLiveInlinedRows avoid
                    // the redundant second per-table probe they used to make.
                    val hasLiveRows: Boolean = try {
                        dsl.fetchExists(
                            DSL.selectOne()
                                .from(inlinedTable.table)
                                .where(inlinedTable.activeAt(snapshotId)),
                        )
                    }
                    catch (e: DataAccessException) {
                        log.log(
                            System.Logger.Level.DEBUG,
                            "Inlined data table {0} not available for table {1}: {2}",
                            inlinedTable.name, tableId, e.message,
                        )
                        return@mapNotNull null
                    }
                    DucklakeInlinedDataInfo(
                        rowTableId,
                        r.get(inlined.TABLE_NAME),
                        rowSchemaVersion,
                        hasLiveRows,
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

    override fun countInlinedRows(tableId: Long, schemaVersion: Long, snapshotId: Long): Long {
        val inlined = InlinedDataTable.of(tableId, schemaVersion)
        return try {
            dsl.fetchCount(
                DSL.selectOne()
                    .from(inlined.table)
                    .where(inlined.activeAt(snapshotId)),
            ).toLong()
        }
        catch (e: DataAccessException) {
            log.log(
                System.Logger.Level.DEBUG,
                "Could not count inlined data rows from {0} (table may not exist): {1}",
                inlined.name, e.message,
            )
            0L
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

    override fun getInlinedDeletes(tableId: Long, snapshotId: Long): Map<Long, Set<Long>> {
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
            val grouped: MutableMap<Long, MutableSet<Long>> = mutableMapOf()
            for (rec in result) {
                val fid = rec.get(fileId)
                val rid = rec.get(rowId)
                if (fid == null || rid == null) {
                    continue
                }
                grouped.getOrPut(fid) { mutableSetOf() }.add(rid)
            }
            grouped
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
            ?: return emptyList()

        val sourceColumnsById: Map<Long, DucklakeColumn> = getTableColumns(tableId, sourceSchemaSnapshot)
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

    override fun readInlinedBeginSnapshots(
        tableId: Long,
        schemaVersion: Long,
        snapshotId: Long,
    ): List<Long> {
        val inlined = InlinedDataTable.of(tableId, schemaVersion)
        return try {
            // Same filter + row_id ordering as readInlinedData, so the returned begin_snapshots
            // line up positionally with that method's rows.
            dsl.select(inlined.beginSnapshot)
                .from(inlined.table)
                .where(inlined.activeAt(snapshotId))
                .orderBy(DSL.field(DSL.name("row_id")))
                .fetch()
                .map { it.get(inlined.beginSnapshot) }
        }
        catch (e: DataAccessException) {
            log.log(
                System.Logger.Level.DEBUG,
                "Could not read inlined begin_snapshots from {0} (table may not exist): {1}",
                inlined.name, e.message,
            )
            emptyList()
        }
    }

    private fun getSnapshotIdForSchemaVersion(tableId: Long, schemaVersion: Long, snapshotId: Long): Long? {
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
                return tableScoped
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
        return fallback
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
            return activeAt(beginSnapshot, endSnapshot, snapshotId)
        }

        companion object {
            fun of(tableId: Long, schemaVersion: Long): InlinedDataTable {
                val name = "ducklake_inlined_data_${tableId}_$schemaVersion"
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

    override fun getDataPath(): String? {
        val meta = DUCKLAKE_METADATA.`as`("meta")
        return dsl.select(meta.VALUE)
            .from(meta)
            .where(meta.KEY.eq("data_path"))
            .fetchOne(meta.VALUE)
    }

    // ==================== View operations ====================

    override fun listViews(schemaId: Long, snapshotId: Long): List<DucklakeView> {
        val view = DUCKLAKE_VIEW.`as`("view")
        return dsl.selectFrom(view)
            .where(view.SCHEMA_ID.eq(schemaId))
            .and(activeAt(view, snapshotId))
            .fetch { toDucklakeView(it) }
    }

    override fun getView(schemaName: String, viewName: String, snapshotId: Long): DucklakeView? {
        val schema = getSchema(schemaName, snapshotId) ?: return null

        val view = DUCKLAKE_VIEW.`as`("view")
        return dsl.selectFrom(view)
            .where(view.SCHEMA_ID.eq(schema.schemaId))
            .and(view.VIEW_NAME.eq(viewName))
            .and(activeAt(view, snapshotId))
            .fetchOne()
            ?.let { toDucklakeView(it) }
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
        val message = "Concurrent DuckLake commit while attempting to $operationDescription" +
            ": expected base snapshot $expectedSnapshotId" +
            ", but current snapshot is $currentSnapshotId" +
            ". Intervening changes: $interveningChanges"
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
        partitionSpec: List<PartitionFieldSpec>?,
        location: TableLocationSpec?,
        dataFileFormat: String?,
    ) {
        val tab = DUCKLAKE_TABLE.`as`("tab")
        val partinfo = DUCKLAKE_PARTITION_INFO.`as`("partinfo")
        val partcol = DUCKLAKE_PARTITION_COLUMN.`as`("partcol")
        val tablePath: String = location?.path ?: "$tableName/"
        val pathIsRelative: Boolean = location?.isRelative ?: true
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
            val topLevelColumnIds: MutableMap<String, Long> = linkedMapOf()
            var columnOrder: Long = 1
            for (column in columns) {
                val columnId = insertColumnTree(tx, tableId, column, columnOrder++, OptionalLong.empty())
                topLevelColumnIds[column.name] = columnId
            }

            // 3. Table stats are NOT created at CREATE TABLE time — DuckDB creates them
            // only when data is first inserted. Creating them here with zeros causes
            // DuckDB's GetGlobalTableStats to crash (GetValueInternal on NULL).

            // 4. Insert partition spec if provided
            if (partitionSpec != null && partitionSpec.isNotEmpty()) {
                val partitionId = tx.allocateCatalogId()

                ctx.insertInto(partinfo)
                    .set(partinfo.PARTITION_ID, partitionId)
                    .set(partinfo.TABLE_ID, tableId)
                    .set(partinfo.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                    .execute()

                var keyIndex: Long = 0
                for (field in partitionSpec) {
                    val columnId = topLevelColumnIds[field.columnName]
                        ?: throw RuntimeException("Partition column not found: ${field.columnName}")
                    ctx.insertInto(partcol)
                        .set(partcol.PARTITION_ID, partitionId)
                        .set(partcol.TABLE_ID, tableId)
                        .set(partcol.PARTITION_KEY_INDEX, keyIndex++)
                        .set(partcol.COLUMN_ID, columnId)
                        .set(
                            partcol.TRANSFORM,
                            field.transform.toCatalogString(
                                field.arity?.let { OptionalInt.of(it) } ?: OptionalInt.empty(),
                            ),
                        )
                        .execute()
                }
            }

            // 5. Persist the declared data file format as a table-scoped setting. Unversioned
            // (no snapshot range) like upstream's set_option rows; upstream DuckDB loads
            // table-scoped settings into an untyped options map at ATTACH, so the key is
            // interop-safe even though only this connector consumes it.
            if (dataFileFormat != null) {
                val meta = DUCKLAKE_METADATA.`as`("meta")
                ctx.insertInto(meta)
                    .set(meta.KEY, TABLE_DATA_FILE_FORMAT_KEY)
                    .set(meta.VALUE, dataFileFormat)
                    .set(meta.SCOPE, TABLE_SETTING_SCOPE)
                    .set(meta.SCOPE_ID, tableId)
                    .execute()
            }

            tx.incrementSchemaVersion(tableId)
            tx.recordChange(WriteChange.CreatedTable(schemaId, schemaName, tableName))
        }
    }

    override fun getTableDataFileFormat(tableId: Long): String? {
        val meta = DUCKLAKE_METADATA.`as`("meta")
        return dsl.select(meta.VALUE)
            .from(meta)
            .where(meta.KEY.eq(TABLE_DATA_FILE_FORMAT_KEY))
            .and(meta.SCOPE.eq(TABLE_SETTING_SCOPE))
            .and(meta.SCOPE_ID.eq(tableId))
            .fetchOne(meta.VALUE)
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

            // Table-scoped settings rows are unversioned, so remove them outright (table ids
            // are never reused; a leftover row would be junk, not a time-travel artifact).
            val meta = DUCKLAKE_METADATA.`as`("meta")
            metadata.execute(
                ctx,
                ctx.deleteFrom(meta)
                    .where(meta.SCOPE.eq(TABLE_SETTING_SCOPE))
                    .and(meta.SCOPE_ID.eq(tableId)),
            )

            tx.incrementSchemaVersion(tableId)
            tx.recordChange(WriteChange.DroppedTable(tableId))
        }
    }

    override fun truncateTable(schemaName: String, tableName: String) {
        val file = DUCKLAKE_DATA_FILE.`as`("file")
        val delfile = DUCKLAKE_DELETE_FILE.`as`("delfile")
        executeWriteTransaction("truncate table $schemaName.$tableName") { tx ->
            val schemaId = tx.resolveSchemaId(schemaName)
            val tableId = tx.resolveTableId(schemaId, tableName)
            val ctx = tx.dsl()
            val newSnapshotId = tx.getNewSnapshotId()

            // Capture the active data-file ids before clearing — for the conflict matrix's
            // deleted_from_table entry (it keys on table id, but the set keeps the change honest).
            val clearedFileIds: Set<Long> = ctx.select(file.DATA_FILE_ID)
                .from(file)
                .where(file.TABLE_ID.eq(tableId))
                .and(activeAt(file, tx.getCurrentSnapshotId()))
                .fetchSet(file.DATA_FILE_ID)
                .filterNotNull()
                .toSet()

            // End-snapshot active delete files first (they reference data files), then the data
            // files themselves. Routed through `metadata` so the Quack RPC binder accepts the
            // UPDATE on attached-metadata tables (pass-through on PG / local DuckDB).
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
            metadata.execute(
                ctx,
                ctx.update(file)
                    .set(file.END_SNAPSHOT, newSnapshotId)
                    .where(file.TABLE_ID.eq(tableId))
                    .and(file.END_SNAPSHOT.isNull),
            )

            endSnapshotLiveInlinedRows(ctx, tableId, newSnapshotId, "truncate")

            // Data change, not schema — do NOT bump the schema version (matches DELETE/MERGE).
            tx.recordChange(WriteChange.DeletedFromTable(tableId, clearedFileIds))
        }
    }

    override fun flushInlinedData(tableId: Long, fragments: List<DucklakeWriteFragment>) {
        executeWriteTransaction("flush inlined data for table $tableId") { tx ->
            val ctx = tx.dsl()
            val newSnapshotId = tx.getNewSnapshotId()

            // Register the data file(s) the caller materialized from the inlined rows, then
            // end-snapshot the live inlined rows — atomically. The conflict matrix
            // (checkFlushedInlinedData) aborts if an intervening commit changed this table's
            // inlined data or schema, so the read-then-write can't duplicate or drop rows.
            if (fragments.isNotEmpty()) {
                applyInsertFragments(tx, tableId, fragments)
            }
            endSnapshotLiveInlinedRows(ctx, tableId, newSnapshotId, "flush")

            // Data move, not schema — no schema-version bump.
            tx.recordChange(WriteChange.FlushedInlinedData(tableId))
        }
    }

    /**
     * End-snapshot every live inlined row across all of a table's per-schema-version inlined
     * tables, at [newSnapshotId]. Shared by truncate and flush. The per-version table is
     * dynamically named; a DataAccessException means the metadata points at a dropped /
     * never-materialized table — skip it, as getInlinedDataInfos does. Routed through
     * `metadata` so the Quack RPC binder accepts the UPDATE on attached-metadata tables.
     */
    private fun endSnapshotLiveInlinedRows(ctx: DSLContext, tableId: Long, newSnapshotId: Long, operation: String) {
        val inlinedTables = DUCKLAKE_INLINED_DATA_TABLES.`as`("inlined")
        val schemaVersions: List<Long> = ctx.select(inlinedTables.SCHEMA_VERSION)
            .from(inlinedTables)
            .where(inlinedTables.TABLE_ID.eq(tableId))
            .fetch(inlinedTables.SCHEMA_VERSION)
            .filterNotNull()
        for (schemaVersion in schemaVersions) {
            val inlined = InlinedDataTable.of(tableId, schemaVersion)
            try {
                metadata.execute(
                    ctx,
                    ctx.update(inlined.table)
                        .set(inlined.endSnapshot, newSnapshotId)
                        .where(inlined.endSnapshot.isNull),
                )
            }
            catch (e: DataAccessException) {
                log.log(System.Logger.Level.DEBUG,
                    "Skipping inlined-data table for table $tableId schema version $schemaVersion during $operation", e)
            }
        }
    }

    override fun renameTable(tableId: Long, targetSchemaName: String, newTableName: String) {
        val tab = DUCKLAKE_TABLE.`as`("tab")
        executeWriteTransaction("rename table $tableId to $targetSchemaName.$newTableName") { tx ->
            val ctx = tx.dsl()

            val existing = metadata.fetchOne(
                ctx,
                ctx.selectFrom(tab)
                    .where(tab.TABLE_ID.eq(tableId))
                    .and(activeAt(tab, tx.getCurrentSnapshotId())),
            ) ?: throw RuntimeException("Table not found: $tableId")

            val targetSchemaId = tx.resolveSchemaId(targetSchemaName)
            if (targetSchemaId != existing.get(tab.SCHEMA_ID)) {
                // Table data paths are SCHEMA-relative (resolved as schemaPath + tablePath), so
                // re-pointing schema_id would leave the data files unreachable under the new
                // schema's path. Upstream has no cross-schema rename either.
                throw RuntimeException(
                    "Renaming a table across schemas is not supported: table data paths are schema-relative")
            }
            val clash = metadata.fetchOne(
                ctx,
                ctx.select(tab.TABLE_ID)
                    .from(tab)
                    .where(tab.SCHEMA_ID.eq(targetSchemaId))
                    .and(tab.TABLE_NAME.eq(newTableName))
                    .and(activeAt(tab, tx.getCurrentSnapshotId())),
            )
            if (clash != null) {
                throw RuntimeException("Table already exists: $targetSchemaName.$newTableName")
            }

            // End-snapshot the current version; re-insert under the same table_id/uuid/path —
            // only name (and possibly schema_id) change, so data files and history stay put.
            metadata.execute(
                ctx,
                ctx.update(tab)
                    .set(tab.END_SNAPSHOT, tx.getNewSnapshotId())
                    .where(tab.TABLE_ID.eq(tableId))
                    .and(tab.END_SNAPSHOT.isNull),
            )
            ctx.insertInto(tab)
                .set(tab.TABLE_ID, tableId)
                .set(tab.TABLE_UUID, existing.get(tab.TABLE_UUID))
                .set(tab.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                .set(tab.SCHEMA_ID, targetSchemaId)
                .set(tab.TABLE_NAME, newTableName)
                .set(tab.PATH, existing.get(tab.PATH))
                .set(tab.PATH_IS_RELATIVE, existing.get(tab.PATH_IS_RELATIVE))
                .execute()

            tx.incrementSchemaVersion(tableId)
            tx.recordChange(WriteChange.AlteredTable(tableId))
        }
    }

    override fun renameSchema(schemaName: String, newName: String) {
        val sch = DUCKLAKE_SCHEMA.`as`("sch")
        val tab = DUCKLAKE_TABLE.`as`("tab")
        val view = DUCKLAKE_VIEW.`as`("view")
        val macro = DUCKLAKE_MACRO.`as`("macro")
        executeWriteTransaction("rename schema $schemaName to $newName") { tx ->
            val ctx = tx.dsl()
            val schemaId = tx.resolveSchemaId(schemaName)

            val existing = metadata.fetchOne(
                ctx,
                ctx.selectFrom(sch)
                    .where(sch.SCHEMA_ID.eq(schemaId))
                    .and(activeAt(sch, tx.getCurrentSnapshotId())),
            ) ?: throw RuntimeException("Schema not found: $schemaName")

            val clash = metadata.fetchOne(
                ctx,
                ctx.select(sch.SCHEMA_ID)
                    .from(sch)
                    .where(sch.SCHEMA_NAME.eq(newName))
                    .and(activeAt(sch, tx.getCurrentSnapshotId())),
            )
            if (clash != null) {
                throw RuntimeException("Schema already exists: $newName")
            }

            // ducklake_schema has a PRIMARY KEY on schema_id (upstream DDL), so a rename
            // cannot be a same-id versioned-row replacement like table renames. Instead the
            // renamed schema gets a NEW schema_id — old row end-snapshotted, so time travel
            // keeps resolving the old name — and every active table/view/macro row in it is
            // re-pointed via its own versioned-row replacement (those tables have no PK).
            // The new schema row keeps the OLD path, so schema-relative table paths resolve
            // unchanged and no data moves. Recorded as dropped+created: upstream's change
            // vocabulary has no schema-rename type (its parser throws on unknown types), and
            // that pair is the honest conflict surface either way.
            val newSchemaId = tx.allocateCatalogId()
            tx.recordChange(WriteChange.DroppedSchema(schemaId, schemaName))
            tx.recordChange(WriteChange.CreatedSchema(newName))

            metadata.execute(
                ctx,
                ctx.update(sch)
                    .set(sch.END_SNAPSHOT, tx.getNewSnapshotId())
                    .where(sch.SCHEMA_ID.eq(schemaId))
                    .and(sch.END_SNAPSHOT.isNull),
            )
            ctx.insertInto(sch)
                .set(sch.SCHEMA_ID, newSchemaId)
                .set(sch.SCHEMA_UUID, UUID.fromString(newCatalogUuid()))
                .set(sch.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                .set(sch.SCHEMA_NAME, newName)
                .set(sch.PATH, existing.get(sch.PATH))
                .set(sch.PATH_IS_RELATIVE, existing.get(sch.PATH_IS_RELATIVE))
                .execute()

            val activeTables = metadata.fetch(
                ctx,
                ctx.selectFrom(tab)
                    .where(tab.SCHEMA_ID.eq(schemaId))
                    .and(activeAt(tab, tx.getCurrentSnapshotId())),
            )
            for (t in activeTables) {
                val tableId = t.get(tab.TABLE_ID)!!
                metadata.execute(
                    ctx,
                    ctx.update(tab)
                        .set(tab.END_SNAPSHOT, tx.getNewSnapshotId())
                        .where(tab.TABLE_ID.eq(tableId))
                        .and(tab.END_SNAPSHOT.isNull),
                )
                ctx.insertInto(tab)
                    .set(tab.TABLE_ID, tableId)
                    .set(tab.TABLE_UUID, t.get(tab.TABLE_UUID))
                    .set(tab.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                    .set(tab.SCHEMA_ID, newSchemaId)
                    .set(tab.TABLE_NAME, t.get(tab.TABLE_NAME))
                    .set(tab.PATH, t.get(tab.PATH))
                    .set(tab.PATH_IS_RELATIVE, t.get(tab.PATH_IS_RELATIVE))
                    .execute()
                tx.recordChange(WriteChange.AlteredTable(tableId))
            }

            val activeViews = metadata.fetch(
                ctx,
                ctx.selectFrom(view)
                    .where(view.SCHEMA_ID.eq(schemaId))
                    .and(activeAt(view, tx.getCurrentSnapshotId())),
            )
            for (v in activeViews) {
                val viewId = v.get(view.VIEW_ID)!!
                metadata.execute(
                    ctx,
                    ctx.update(view)
                        .set(view.END_SNAPSHOT, tx.getNewSnapshotId())
                        .where(view.VIEW_ID.eq(viewId))
                        .and(view.END_SNAPSHOT.isNull),
                )
                ctx.insertInto(view)
                    .set(view.VIEW_ID, viewId)
                    .set(view.VIEW_UUID, v.get(view.VIEW_UUID))
                    .set(view.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                    .set(view.SCHEMA_ID, newSchemaId)
                    .set(view.VIEW_NAME, v.get(view.VIEW_NAME))
                    .set(view.DIALECT, v.get(view.DIALECT))
                    .set(view.SQL, v.get(view.SQL))
                    .set(view.COLUMN_ALIASES, v.get(view.COLUMN_ALIASES))
                    .execute()
                tx.recordChange(WriteChange.AlteredView(viewId))
            }

            // Macros also carry schema_id. This connector never creates them, but a DuckDB
            // writer may have; their impl/parameter rows key on macro_id, so re-pointing the
            // ducklake_macro row is the whole move. No macro-alter change type exists — the
            // schema-level dropped+created already covers the conflict surface.
            val activeMacros = metadata.fetch(
                ctx,
                ctx.selectFrom(macro)
                    .where(macro.SCHEMA_ID.eq(schemaId))
                    .and(activeAt(macro, tx.getCurrentSnapshotId())),
            )
            for (m in activeMacros) {
                val macroId = m.get(macro.MACRO_ID)
                metadata.execute(
                    ctx,
                    ctx.update(macro)
                        .set(macro.END_SNAPSHOT, tx.getNewSnapshotId())
                        .where(macro.SCHEMA_ID.eq(schemaId))
                        .and(macro.MACRO_ID.eq(macroId))
                        .and(macro.END_SNAPSHOT.isNull),
                )
                ctx.insertInto(macro)
                    .set(macro.SCHEMA_ID, newSchemaId)
                    .set(macro.MACRO_ID, macroId)
                    .set(macro.MACRO_NAME, m.get(macro.MACRO_NAME))
                    .set(macro.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                    .execute()
            }

            tx.incrementSchemaVersion()
        }
    }

    override fun setTableComment(tableId: Long, comment: String?) {
        val tag = DUCKLAKE_TAG.`as`("tag")
        executeWriteTransaction("set comment on table $tableId") { tx ->
            val ctx = tx.dsl()
            metadata.execute(
                ctx,
                ctx.update(tag)
                    .set(tag.END_SNAPSHOT, tx.getNewSnapshotId())
                    .where(tag.OBJECT_ID.eq(tableId))
                    .and(tag.KEY.eq(COMMENT_TAG_KEY))
                    .and(tag.END_SNAPSHOT.isNull),
            )
            if (comment != null) {
                ctx.insertInto(tag)
                    .set(tag.OBJECT_ID, tableId)
                    .set(tag.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                    .set(tag.KEY, COMMENT_TAG_KEY)
                    .set(tag.VALUE, comment)
                    .execute()
            }
            tx.recordChange(WriteChange.AlteredTable(tableId))
        }
    }

    override fun getTableComment(tableId: Long, snapshotId: Long): String? {
        val tag = DUCKLAKE_TAG.`as`("tag")
        return dsl.select(tag.VALUE)
            .from(tag)
            .where(tag.OBJECT_ID.eq(tableId))
            .and(tag.KEY.eq(COMMENT_TAG_KEY))
            .and(activeAt(tag, snapshotId))
            .fetchOne(tag.VALUE)
    }

    override fun setColumnComment(tableId: Long, columnId: Long, comment: String?) {
        val ctag = DUCKLAKE_COLUMN_TAG.`as`("ctag")
        executeWriteTransaction("set comment on column $columnId of table $tableId") { tx ->
            val ctx = tx.dsl()
            metadata.execute(
                ctx,
                ctx.update(ctag)
                    .set(ctag.END_SNAPSHOT, tx.getNewSnapshotId())
                    .where(ctag.TABLE_ID.eq(tableId))
                    .and(ctag.COLUMN_ID.eq(columnId))
                    .and(ctag.KEY.eq(COMMENT_TAG_KEY))
                    .and(ctag.END_SNAPSHOT.isNull),
            )
            if (comment != null) {
                ctx.insertInto(ctag)
                    .set(ctag.TABLE_ID, tableId)
                    .set(ctag.COLUMN_ID, columnId)
                    .set(ctag.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                    .set(ctag.KEY, COMMENT_TAG_KEY)
                    .set(ctag.VALUE, comment)
                    .execute()
            }
            tx.recordChange(WriteChange.AlteredTable(tableId))
        }
    }

    override fun getColumnComments(tableId: Long, snapshotId: Long): Map<Long, String> {
        val ctag = DUCKLAKE_COLUMN_TAG.`as`("ctag")
        return dsl.select(ctag.COLUMN_ID, ctag.VALUE)
            .from(ctag)
            .where(ctag.TABLE_ID.eq(tableId))
            .and(ctag.KEY.eq(COMMENT_TAG_KEY))
            .and(activeAt(ctag, snapshotId))
            .fetch()
            .mapNotNull { r ->
                val columnId = r.get(ctag.COLUMN_ID) ?: return@mapNotNull null
                val value = r.get(ctag.VALUE) ?: return@mapNotNull null
                columnId to value
            }
            .toMap()
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
            val nullsAllowed = existing.get(col.NULLS_ALLOWED) == true

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

    override fun addField(tableId: Long, parentPath: List<String>, field: TableColumnSpec, ignoreExisting: Boolean) {
        // IF NOT EXISTS pre-check (advisory) so the no-op case doesn't mint an empty snapshot.
        if (ignoreExisting) {
            val current = activeColumnRows(dsl, tableId, currentSnapshotId)
            val parentId = resolveColumnIdByPath(current, parentPath)
            if (current.any { it.parentColumn == parentId && it.columnName == field.name }) {
                return
            }
        }
        val col = DUCKLAKE_COLUMN.`as`("col")
        executeWriteTransaction("add field ${(parentPath + field.name).joinToString(".")} to table $tableId") { tx ->
            val columns = activeColumnRows(tx.dsl(), tableId, tx.getCurrentSnapshotId())
            val parentId = resolveColumnIdByPath(columns, parentPath)
            val parent = columns.first { it.columnId == parentId }
            if (!parent.columnType.equals("struct", ignoreCase = true)) {
                throw IllegalArgumentException(
                    "Cannot add a field to non-struct column ${parentPath.joinToString(".")} (type=${parent.columnType})",
                )
            }
            if (columns.any { it.parentColumn == parentId && it.columnName == field.name }) {
                throw IllegalArgumentException("Field ${(parentPath + field.name).joinToString(".")} already exists")
            }
            // Children carry their own column_order within the parent; append at max+1 (0 if first).
            val maxChildOrder: Long? = tx.dsl().select(DSL.max(col.COLUMN_ORDER))
                .from(col)
                .where(col.TABLE_ID.eq(tableId))
                .and(col.PARENT_COLUMN.eq(parentId))
                .and(activeAt(col, tx.getCurrentSnapshotId()))
                .fetchOne(0, Long::class.java)
            val childOrder = if (maxChildOrder == null) 0L else maxChildOrder + 1
            insertColumnTree(tx, tableId, field, childOrder, OptionalLong.of(parentId))
            tx.incrementSchemaVersion(tableId)
            tx.recordChange(WriteChange.AlteredTable(tableId))
        }
    }

    override fun dropField(tableId: Long, fieldPath: List<String>) {
        val col = DUCKLAKE_COLUMN.`as`("col")
        executeWriteTransaction("drop field ${fieldPath.joinToString(".")} from table $tableId") { tx ->
            val ctx = tx.dsl()
            val columns = activeColumnRows(ctx, tableId, tx.getCurrentSnapshotId())
            val targetId = resolveColumnIdByPath(columns, fieldPath)
            // End-snapshot the field and every transitive descendant (struct subfields nest
            // arbitrarily — improves on dropColumn's single-level cascade).
            val subtree = collectSubtreeIds(columns, targetId)
            ctx.update(col)
                .set(col.END_SNAPSHOT, tx.getNewSnapshotId())
                .where(col.TABLE_ID.eq(tableId))
                .and(col.COLUMN_ID.`in`(subtree))
                .and(col.END_SNAPSHOT.isNull)
                .execute()
            tx.incrementSchemaVersion(tableId)
            tx.recordChange(WriteChange.AlteredTable(tableId))
        }
    }

    /** Active `ducklake_column` rows for a table at [snapshotId], flat (children keep parent_column). */
    private fun activeColumnRows(ctx: DSLContext, tableId: Long, snapshotId: Long): List<DucklakeColumn> {
        val col = DUCKLAKE_COLUMN.`as`("col")
        return ctx.selectFrom(col)
            .where(col.TABLE_ID.eq(tableId))
            .and(activeAt(col, snapshotId))
            .orderBy(col.COLUMN_ORDER, col.COLUMN_ID)
            .fetch { toDucklakeColumn(it) }
    }

    /**
     * Walk a dotted field [path] (top-level column name first, then nested field names) through the
     * `parent_column` links of [columns], returning the leaf's column_id. Throws if any step misses.
     */
    private fun resolveColumnIdByPath(columns: List<DucklakeColumn>, path: List<String>): Long {
        if (path.isEmpty()) {
            throw IllegalArgumentException("Empty field path")
        }
        var parentId: Long? = null
        var currentId: Long? = null
        for (name in path) {
            val match = columns.firstOrNull { it.columnName == name && it.parentColumn == parentId }
                ?: throw IllegalArgumentException("Field not found: ${path.joinToString(".")} (no '$name')")
            currentId = match.columnId
            parentId = match.columnId
        }
        return currentId!!
    }

    /** [rootId] plus every transitive descendant via `parent_column` (for a recursive field drop). */
    private fun collectSubtreeIds(columns: List<DucklakeColumn>, rootId: Long): Set<Long> {
        val byParent: Map<Long, List<DucklakeColumn>> = columns
            .filter { it.parentColumn != null }
            .groupBy { it.parentColumn!! }
        val result: MutableSet<Long> = linkedSetOf()
        val stack: ArrayDeque<Long> = ArrayDeque()
        stack.addLast(rootId)
        while (stack.isNotEmpty()) {
            val id = stack.removeLast()
            if (result.add(id)) {
                byParent[id]?.forEach { stack.addLast(it.columnId) }
            }
        }
        return result
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
        val partitionValueRecords: MutableList<DucklakeFilePartitionValueRecord> = mutableListOf()
        val fileColumnStatsRecords: MutableList<DucklakeFileColumnStatsRecord> = mutableListOf()
        val dataFileRecords: MutableList<DucklakeDataFileRecord> = mutableListOf()
        // Dedupe identical NameMap structures within this call. Upstream's
        // ducklake_add_data_files writes one ducklake_column_mapping row per
        // unique parquet schema seen in a batch; matching that here lets a
        // glob over homogeneous parquet files share one mapping_id.
        val nameMapToId: MutableMap<DucklakeNameMap, Long> = mutableMapOf()
        val columnMappingRecords: MutableList<DucklakeColumnMappingRecord> = mutableListOf()
        val nameMappingRecords: MutableList<DucklakeNameMappingRecord> = mutableListOf()

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
            fragment.partitionId?.let { dataFile.setPartitionId(it) }
            fragment.nameMap?.let { nameMap ->
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
                r.setMinValue(columnStats.minValue)
                r.setMaxValue(columnStats.maxValue)
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
        val columnAggregates: MutableMap<Long, AggregatedColumnStats> = linkedMapOf()
        for (fragment in fragments) {
            for (colStats in fragment.columnStats) {
                columnAggregates.getOrPut(colStats.columnId) { AggregatedColumnStats() }.merge(colStats)
            }
        }

        val existingColumnStats: Set<Long> = loadExistingColumnStatsColumnIds(tx, tableId, columnAggregates.keys)
        val insertRecords: MutableList<DucklakeTableColumnStatsRecord> = mutableListOf()
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

    override fun rewriteDataFiles(
        tableId: Long,
        sourceDataFileIds: Set<Long>,
        fragments: List<DucklakeWriteFragment>,
        readSnapshotId: Long,
    ) {
        if (sourceDataFileIds.isEmpty() || fragments.isEmpty()) {
            return
        }
        executeWriteTransaction("rewrite data files for table $tableId") { tx ->
            assertNoNewerDeleteOnRewriteSources(tx, sourceDataFileIds, readSnapshotId)
            val retiredFileSize = sumActiveSourceFileSize(tx, tableId, sourceDataFileIds)

            // Register the merged file(s): begin = newSnapshotId, ordinary (no partial_max). This
            // also bumps table_stats record_count/file_size/next_row_id UP by the merged amounts and
            // widens the per-column table stats (a no-op since merged ⊆ source range).
            applyInsertFragments(tx, tableId, fragments)
            endSnapshotRewriteSources(tx, tableId, sourceDataFileIds)
            netRewriteStats(tx, tableId, fragments.sumOf { it.recordCount }, retiredFileSize)

            // Recorded as delete + insert: reuses the full conflict machinery (LogicalConflictCheck
            // verifies the sources are still active at commit → stale-read aborts non-retryably;
            // ConflictMatrix aborts on concurrent drop/alter). No new snapshot-change vocabulary.
            tx.recordChange(WriteChange.DeletedFromTable(tableId, sourceDataFileIds))
            tx.recordChange(WriteChange.InsertedIntoTable(tableId, referencedColumnIds(fragments)))
        }
    }

    override fun rewriteDataFilesPartial(
        tableId: Long,
        sourceDataFileIds: Set<Long>,
        mergedFiles: List<PartialMergedFile>,
        readSnapshotId: Long,
    ) {
        if (sourceDataFileIds.isEmpty() || mergedFiles.isEmpty()) {
            return
        }
        val file = DUCKLAKE_DATA_FILE.`as`("file")
        executeWriteTransaction("partial-rewrite data files for table $tableId") { tx ->
            val ctx = tx.dsl()
            // Up-front validation (re-checked on retry): sources still active + no deletion newer
            // than the caller's read. Recorded only as InsertedIntoTable (the sources are DELETED,
            // not end-snapshotted, so the DeletedFromTable active-file check would misfire).
            assertRewriteSourcesStillPresent(tx, tableId, sourceDataFileIds)
            assertNoNewerDeleteOnRewriteSources(tx, sourceDataFileIds, readSnapshotId)
            val retiredFileSize = sumActiveSourceFileSize(tx, tableId, sourceDataFileIds)

            for (merged in mergedFiles) {
                applyInsertFragments(tx, tableId, listOf(merged.fragment))
                // Back-date the just-registered merged file to begin = MIN row snapshot + tag it
                // partial (partial_max = MAX row snapshot); rows newer than a time-travel read are
                // filtered via the file's _ducklake_internal_snapshot_id column.
                ctx.update(file)
                    .set(file.BEGIN_SNAPSHOT, merged.beginSnapshot)
                    .set(file.PARTIAL_MAX, merged.partialMax)
                    .where(file.TABLE_ID.eq(tableId))
                    .and(file.PATH.eq(merged.fragment.path))
                    .and(file.BEGIN_SNAPSHOT.eq(tx.getNewSnapshotId()))
                    .execute()
            }

            scheduleAndDeleteRewriteSources(tx, sourceDataFileIds)
            netRewriteStats(tx, tableId, mergedFiles.sumOf { it.fragment.recordCount }, retiredFileSize)
            val columnIds: Set<Long> = mergedFiles.flatMap { it.fragment.columnStats }.mapTo(HashSet()) { it.columnId }
            tx.recordChange(WriteChange.InsertedIntoTable(tableId, columnIds))
        }
    }

    /** Abort non-retryably if any source is no longer active (a concurrent drop/compaction removed it). */
    private fun assertRewriteSourcesStillPresent(tx: DucklakeWriteTransaction, tableId: Long, sourceDataFileIds: Set<Long>) {
        val file = DUCKLAKE_DATA_FILE.`as`("file")
        val active: Set<Long> = tx.dsl().select(file.DATA_FILE_ID)
            .from(file)
            .where(file.TABLE_ID.eq(tableId))
            .and(file.DATA_FILE_ID.`in`(sourceDataFileIds))
            .and(activeAt(file, tx.getCurrentSnapshotId()))
            .fetchSet(file.DATA_FILE_ID)
            .filterNotNull().toSet()
        val missing = sourceDataFileIds.filterNot { it in active }
        if (missing.isNotEmpty()) {
            throw LogicalConflictException(
                "Failed to partial-rewrite data files for table $tableId: source data_file_id(s) $missing " +
                    "are no longer active (a concurrent DROP/compaction removed them). Not retried.")
        }
    }

    /**
     * Schedule the source data files AND their delete files for physical deletion (immediate
     * reclaim, age-gated cleanup), then delete every catalog row keyed by the source data_file_ids
     * (data file, column/variant stats, partition values, delete files). Mirrors WriteMergeAdjacent.
     */
    private fun scheduleAndDeleteRewriteSources(tx: DucklakeWriteTransaction, sourceDataFileIds: Set<Long>) {
        val ctx = tx.dsl()
        val cache = HashMap<Long, String?>()
        val dataFiles = findRewriteSourceFiles(ctx, DUCKLAKE_DATA_FILE.DATA_FILE_ID, DUCKLAKE_DATA_FILE.TABLE_ID,
            DUCKLAKE_DATA_FILE.PATH, DUCKLAKE_DATA_FILE.PATH_IS_RELATIVE, sourceDataFileIds)
        val delFiles = findRewriteSourceFiles(ctx, DUCKLAKE_DELETE_FILE.DELETE_FILE_ID, DUCKLAKE_DELETE_FILE.TABLE_ID,
            DUCKLAKE_DELETE_FILE.PATH, DUCKLAKE_DELETE_FILE.PATH_IS_RELATIVE,
            sourceDataFileIds, DUCKLAKE_DELETE_FILE.DATA_FILE_ID)
        for (f in dataFiles + delFiles) {
            scheduleFile(ctx, f, cache)
        }
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_FILE_COLUMN_STATS).where(DUCKLAKE_FILE_COLUMN_STATS.DATA_FILE_ID.`in`(sourceDataFileIds)))
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_FILE_VARIANT_STATS).where(DUCKLAKE_FILE_VARIANT_STATS.DATA_FILE_ID.`in`(sourceDataFileIds)))
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_FILE_PARTITION_VALUE).where(DUCKLAKE_FILE_PARTITION_VALUE.DATA_FILE_ID.`in`(sourceDataFileIds)))
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_DELETE_FILE).where(DUCKLAKE_DELETE_FILE.DATA_FILE_ID.`in`(sourceDataFileIds)))
        metadata.execute(ctx, ctx.deleteFrom(DUCKLAKE_DATA_FILE).where(DUCKLAKE_DATA_FILE.DATA_FILE_ID.`in`(sourceDataFileIds)))
    }

    private fun findRewriteSourceFiles(
        ctx: DSLContext,
        idField: org.jooq.TableField<*, Long>,
        tableIdField: org.jooq.TableField<*, Long>,
        pathField: org.jooq.TableField<*, String>,
        pathRelField: org.jooq.TableField<*, Boolean>,
        sourceDataFileIds: Set<Long>,
        keyField: org.jooq.TableField<*, Long> = idField,
    ): List<DeadFile> =
        ctx.select(idField, tableIdField, pathField, pathRelField)
            .from(idField.table)
            .where(keyField.`in`(sourceDataFileIds))
            .fetch { DeadFile(it.value1(), it.value2(), it.value3(), it.value4() ?: false) }

    /**
     * Stale-read guard for [rewriteDataFiles]: a concurrent DELETE/MERGE attaches a delete file to a
     * source WITHOUT end-snapshotting the data file, so the `DeletedFromTable` active-file check
     * would miss it and the pre-built merged file (which didn't apply that delete) would resurrect
     * the deleted rows. Abort non-retryably if any delete file on a source is newer than the
     * snapshot the caller read at. Runs inside the action so it re-checks on every retry.
     */
    private fun assertNoNewerDeleteOnRewriteSources(
        tx: DucklakeWriteTransaction,
        sourceDataFileIds: Set<Long>,
        readSnapshotId: Long,
    ) {
        val delfile = DUCKLAKE_DELETE_FILE.`as`("delfile")
        val hasNewerDelete: Boolean = tx.dsl().fetchExists(
            DSL.selectOne()
                .from(delfile)
                .where(delfile.DATA_FILE_ID.`in`(sourceDataFileIds))
                .and(delfile.BEGIN_SNAPSHOT.gt(readSnapshotId)),
        )
        if (hasNewerDelete) {
            throw LogicalConflictException(
                "Failed to rewrite data files: a concurrent commit added a delete file to a " +
                    "compaction source after this operation read it (read snapshot $readSnapshotId). " +
                    "The merged file would resurrect the newly-deleted rows; re-running with the " +
                    "same merged payload would fail identically, so this conflict is not retried.",
            )
        }
    }

    /**
     * Total `file_size_bytes` of the active source files about to be retired — used to net
     * `ducklake_table_stats.file_size_bytes`. record_count is intentionally NOT read here:
     * compaction is row-count-preserving, so [netRewriteStats] backs the merged file's record_count
     * out and leaves record_count unchanged.
     */
    private fun sumActiveSourceFileSize(tx: DucklakeWriteTransaction, tableId: Long, sourceDataFileIds: Set<Long>): Long {
        val file = DUCKLAKE_DATA_FILE.`as`("file")
        return tx.dsl().select(file.FILE_SIZE_BYTES)
            .from(file)
            .where(file.TABLE_ID.eq(tableId))
            .and(file.DATA_FILE_ID.`in`(sourceDataFileIds))
            .and(activeAt(file, tx.getCurrentSnapshotId()))
            .fetch(file.FILE_SIZE_BYTES)
            .filterNotNull()
            .sum()
    }

    /**
     * End-snapshot the retired sources' active delete files first (they reference the data files),
     * then the source data files themselves, at the new snapshot. The merged file has those deletes
     * applied to its bytes, so the delete files are no longer needed going forward (kept for
     * time-travel until expire/cleanup reclaims them). Routed through `metadata` so the Quack RPC
     * binder accepts the UPDATE.
     */
    private fun endSnapshotRewriteSources(tx: DucklakeWriteTransaction, tableId: Long, sourceDataFileIds: Set<Long>) {
        val ctx = tx.dsl()
        val file = DUCKLAKE_DATA_FILE.`as`("file")
        val delfile = DUCKLAKE_DELETE_FILE.`as`("delfile")
        val newSnapshotId = tx.getNewSnapshotId()
        metadata.execute(
            ctx,
            ctx.update(delfile)
                .set(delfile.END_SNAPSHOT, newSnapshotId)
                .where(delfile.DATA_FILE_ID.`in`(sourceDataFileIds))
                .and(delfile.END_SNAPSHOT.isNull),
        )
        metadata.execute(
            ctx,
            ctx.update(file)
                .set(file.END_SNAPSHOT, newSnapshotId)
                .where(file.TABLE_ID.eq(tableId))
                .and(file.DATA_FILE_ID.`in`(sourceDataFileIds))
                .and(file.END_SNAPSHOT.isNull),
        )
    }

    /**
     * Net the table stats back to a row-count-preserving result: subtract the merged file's
     * record_count ([applyInsertFragments] added it; the live row set is unchanged by compaction)
     * and subtract the retired sources' total file_size. GREATEST(0, …) defends against underflow
     * if stats were ever inconsistent with the file ledger.
     */
    private fun netRewriteStats(tx: DucklakeWriteTransaction, tableId: Long, mergedRecordCount: Long, retiredFileSize: Long) {
        val tabstats = DUCKLAKE_TABLE_STATS.`as`("tabstats")
        tx.dsl().update(tabstats)
            .set(
                tabstats.RECORD_COUNT,
                DSL.greatest(DSL.inline(0L), tabstats.RECORD_COUNT.minus(mergedRecordCount)),
            )
            .set(
                tabstats.FILE_SIZE_BYTES,
                DSL.greatest(DSL.inline(0L), tabstats.FILE_SIZE_BYTES.minus(retiredFileSize)),
            )
            .where(tabstats.TABLE_ID.eq(tableId))
            .execute()
    }

    private fun applyDeleteFragments(tx: DucklakeWriteTransaction, tableId: Long, deleteFragments: List<DucklakeDeleteFragment>) {
        val ctx = tx.dsl()
        val delfile = DUCKLAKE_DELETE_FILE.`as`("delfile")
        val tabstats = DUCKLAKE_TABLE_STATS.`as`("tabstats")
        var totalNewDeleteCount: Long = 0
        val deleteFileRecords: MutableList<DucklakeDeleteFileRecord> = mutableListOf()

        // End-snapshot any prior active delete files for the data_file_ids we're about to
        // write new delete files for. DuckLake's spec invariant is ≤1 active delete file per
        // data_file_id per snapshot (README:223, checkDeleteFileOverlap:1311-1312); the sink
        // unions prior-active positions with this commit's new positions into the new file,
        // so superseding the prior is correct (no rows resurrect — the new file carries the
        // union). Record-count math below uses the DELTA (newDeleteCount), not the union
        // total, because the prior's positions were already deducted at first commit.
        val touchedDataFileIds: Set<Long> = deleteFragments.mapTo(HashSet()) { it.dataFileId }
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
        var containsNull: Boolean = false
        var containsNan: Boolean = false
        var minValue: String? = null
        var maxValue: String? = null

        fun merge(stats: DucklakeFileColumnStats) {
            if (stats.nullCount > 0) {
                containsNull = true
            }
            if (stats.containsNan) {
                containsNan = true
            }
            stats.minValue?.let { v ->
                val current = minValue
                if (current == null || v < current) {
                    minValue = v
                }
            }
            stats.maxValue?.let { v ->
                val current = maxValue
                if (current == null || v > current) {
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

    private fun parseStatValue(columnType: String, value: String?): Comparable<*>? =
        DucklakeStatTypes.parseStat(columnType, value)

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

        // Table-scoped ducklake_metadata setting persisted at CREATE TABLE when the user
        // declares WITH (data_file_format = ...); read back by write-format resolution.
        private const val TABLE_DATA_FILE_FORMAT_KEY = "data_file_format"
        private const val TABLE_SETTING_SCOPE = "table"

        // Tag key upstream COMMENT ON writes into ducklake_tag / ducklake_column_tag.
        private const val COMMENT_TAG_KEY = "comment"

        // Optimistic-retry tuning. Defaults match the upstream DuckLake C++ extension
        // (`ducklake_max_retry_count` / `retry_wait_ms` / `retry_backoff` in
        // src/storage/ducklake_transaction.cpp), so behavior under contention matches
        // what callers familiar with upstream expect.
        private const val MAX_RETRY_COUNT = 10
        private const val INITIAL_RETRY_WAIT_MS: Long = 100
        private const val RETRY_BACKOFF_MULTIPLIER: Double = 1.5

        private val V7_UUIDS: TimeBasedEpochGenerator = Generators.timeBasedEpochGenerator()

        // Test seam: callers pin the UUID version via the companion. Kotlin callers use
        // the unqualified form `JdbcDucklakeCatalog.newCatalogUuid()`.
        fun newCatalogUuid(): String =
            V7_UUIDS.generate().toString()

        private fun typedMin(a: String, b: String, columnType: String): String =
            DucklakeStatTypes.min(a, b, columnType)

        private fun typedMax(a: String, b: String, columnType: String): String =
            DucklakeStatTypes.max(a, b, columnType)

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

        private fun referencedColumnIds(fragments: List<DucklakeWriteFragment>): Set<Long> =
            fragments.flatMap { it.columnStats }.mapTo(HashSet()) { it.columnId }

        private fun referencedDataFileIds(fragments: List<DucklakeDeleteFragment>): Set<Long> =
            fragments.mapTo(HashSet()) { it.dataFileId }

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
            val myFileIds: MutableSet<Long> = mutableSetOf()
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
        private fun orZero(value: Long?): Long =
            value ?: 0L

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
                r.endSnapshot,
                orZero(r.tableId),
                orZero(r.columnOrder),
                r.columnName!!,
                r.columnType!!,
                r.nullsAllowed == true,
                r.parentColumn,
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
                r.changesMade,
                r.author,
                r.commitMessage,
                r.commitExtraInfo,
            )
        }

        private fun toDucklakeSchema(r: DucklakeSchemaRecord): DucklakeSchema {
            return DucklakeSchema(
                r.schemaId,
                r.schemaUuid!!,
                orZero(r.beginSnapshot),
                r.endSnapshot,
                r.schemaName!!,
                r.path,
                r.pathIsRelative,
            )
        }

        private fun toDucklakeTable(r: DucklakeTableRecord): DucklakeTable {
            return DucklakeTable(
                orZero(r.tableId),
                r.tableUuid!!,
                orZero(r.beginSnapshot),
                r.endSnapshot,
                orZero(r.schemaId),
                r.tableName!!,
                r.path,
                r.pathIsRelative,
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
                r.columnAliases,
                orZero(r.beginSnapshot),
                r.endSnapshot,
            )
        }
    }
}
