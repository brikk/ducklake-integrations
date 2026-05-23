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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeColumnMappingRecord;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeColumnRecord;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeDataFileRecord;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeDeleteFileRecord;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeFileColumnStatsRecord;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeFilePartitionValueRecord;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeNameMappingRecord;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeTableColumnStatsRecord;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeSchemaRecord;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeSnapshotChangesRecord;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeSnapshotRecord;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeTableRecord;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeTableStatsRecord;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeViewRecord;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.JDBCUtils;
import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedEpochGenerator;

import static dev.brikk.ducklake.catalog.SnapshotRange.activeAt;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_COLUMN;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_COLUMN_MAPPING;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DATA_FILE;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DELETE_FILE;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_FILE_COLUMN_STATS;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_FILE_PARTITION_VALUE;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_INLINED_DATA_TABLES;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_METADATA;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_NAME_MAPPING;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_PARTITION_COLUMN;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_PARTITION_INFO;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SCHEMA;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SCHEMA_VERSIONS;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SORT_EXPRESSION;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SORT_INFO;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SNAPSHOT;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SNAPSHOT_CHANGES;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE_COLUMN_STATS;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE_STATS;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_VIEW;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * JDBC implementation of DucklakeCatalog.
 * Queries the Ducklake metadata tables via JDBC.
 */
public class JdbcDucklakeCatalog
        implements DucklakeCatalog
{
    private static final System.Logger log = System.getLogger(JdbcDucklakeCatalog.class.getName());
    private static final int CONFLICT_CHANGE_SUMMARY_LIMIT = 10;

    // Optimistic-retry tuning. Defaults match the upstream DuckLake C++ extension
    // (`ducklake_max_retry_count` / `retry_wait_ms` / `retry_backoff` in
    // src/storage/ducklake_transaction.cpp), so behavior under contention matches
    // what callers familiar with upstream expect.
    private static final int MAX_RETRY_COUNT = 10;
    private static final long INITIAL_RETRY_WAIT_MS = 100;
    private static final double RETRY_BACKOFF_MULTIPLIER = 1.5;

    private static final TimeBasedEpochGenerator V7_UUIDS = Generators.timeBasedEpochGenerator();

    // Package-private so tests can pin the version without reflection.
    static String newCatalogUuid()
    {
        return V7_UUIDS.generate().toString();
    }

    private final DataSource dataSource;
    private final HikariDataSource hikariDataSource;
    private final SQLDialect dialect;
    private final Settings jooqSettings;
    private final DSLContext dsl;
    private final MetadataQuery metadata;

    public JdbcDucklakeCatalog(DucklakeCatalogConfig config)
    {
        requireNonNull(config, "config is null");

        HikariConfig hikariConfig = new HikariConfig();
        String configuredUrl = config.getCatalogDatabaseUrl();
        String dialectInferenceUrl;
        MetadataQuery metadataQuery;
        if (QuackBackedDuckDbCatalogUrl.matches(configuredUrl)) {
            // Synthetic URL — `jdbc:duckdb:quack://host:port[?metadata_catalog=name]`.
            // Open a plain in-memory DuckDB JDBC connection and let HikariCP run a
            // per-connection init script that loads quack + ducklake, creates the
            // Quack auth secret, ATTACHes the remote DuckLake catalog with a
            // METADATA_CATALOG name, and USEs that catalog's main schema. After init,
            // bare references to `ducklake_*` tables resolve directly to the remote
            // metadata storage — JdbcDucklakeCatalog's jOOQ DSL stays unchanged.
            QuackBackedDuckDbCatalogUrl quack = QuackBackedDuckDbCatalogUrl.parse(
                    configuredUrl, config.getCatalogDatabasePassword(), config.getDataPath());
            hikariConfig.setJdbcUrl(QuackBackedDuckDbCatalogUrl.UNDERLYING_JDBC_URL);
            hikariConfig.setConnectionInitSql(quack.connectionInitSql());
            // The user/password slots aren't used at the JDBC layer for this backend;
            // the token is interpolated into the CREATE SECRET statement inside the
            // init script.
            dialectInferenceUrl = QuackBackedDuckDbCatalogUrl.UNDERLYING_JDBC_URL;
            // Quack RPC's optimizer rejects SQL shapes that the local DuckDB binder
            // happily plans against the Quack-attached metadata catalog (same-table
            // multi-scan, multi-table JOINs). Route the call sites that hit those
            // shapes through quack_query_by_name so the inner SQL is executed
            // server-side as a single LogicalGet from the local plan's POV.
            metadataQuery = new QuackWrappedMetadataQuery(quack.metadataCatalog());
        }
        else {
            hikariConfig.setJdbcUrl(configuredUrl);
            if (config.getCatalogDatabaseUser() != null) {
                hikariConfig.setUsername(config.getCatalogDatabaseUser());
            }
            if (config.getCatalogDatabasePassword() != null) {
                hikariConfig.setPassword(config.getCatalogDatabasePassword());
            }
            dialectInferenceUrl = configuredUrl;
            metadataQuery = new DirectMetadataQuery();
        }
        hikariConfig.setMaximumPoolSize(config.getMaxCatalogConnections());
        hikariConfig.setMinimumIdle(1);
        hikariConfig.setConnectionTimeout(30000);

        this.hikariDataSource = new HikariDataSource(hikariConfig);
        this.dataSource = hikariDataSource;

        // Infer dialect from the JDBC URL. JDBCUtils.dialect() returns SQLDialect.DEFAULT for
        // backends jOOQ OSS doesn't recognize, which keeps query rendering portable.
        this.dialect = JDBCUtils.dialect(dialectInferenceUrl);
        this.jooqSettings = new Settings()
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
                .withRenderSchema(false);
        this.dsl = DSL.using(dataSource, dialect, jooqSettings);
        this.metadata = metadataQuery;

        log.log(System.Logger.Level.INFO,
                "Initialized Ducklake JDBC catalog: {0} (jOOQ dialect: {1})",
                config.getCatalogDatabaseUrl(), dialect);
    }

    /**
     * Returns a {@link DSLContext} scoped to a caller-supplied {@link Connection}.
     * Writes routed through this context run on the caller's transaction rather
     * than checking out a fresh pool connection, so commits and rollbacks on the
     * caller's connection apply to every jOOQ-issued statement.
     *
     * <p>Used by {@link DucklakeWriteTransaction} to keep all mutations on a
     * single transactional connection.
     */
    DSLContext forConnection(Connection connection)
    {
        return DSL.using(connection, dialect, jooqSettings);
    }

    @Override
    public long getCurrentSnapshotId()
    {
        var snap = DUCKLAKE_SNAPSHOT.as("snap");
        Long maxId = dsl.select(DSL.max(snap.SNAPSHOT_ID))
                .from(snap)
                .fetchOne(0, Long.class);
        if (maxId == null) {
            throw new IllegalStateException("No snapshots found in ducklake_snapshot table");
        }
        return maxId;
    }

    @Override
    public Optional<DucklakeSnapshot> getSnapshot(long snapshotId)
    {
        var snap = DUCKLAKE_SNAPSHOT.as("snap");
        return dsl.selectFrom(snap)
                .where(snap.SNAPSHOT_ID.eq(snapshotId))
                .fetchOptional()
                .map(JdbcDucklakeCatalog::toDucklakeSnapshot);
    }

    @Override
    public Optional<DucklakeSnapshot> getSnapshotAtOrBefore(Instant timestamp)
    {
        var snap = DUCKLAKE_SNAPSHOT.as("snap");
        return dsl.selectFrom(snap)
                .orderBy(snap.SNAPSHOT_ID.desc())
                .fetch(JdbcDucklakeCatalog::toDucklakeSnapshot)
                .stream()
                .filter(snapshot -> !snapshot.snapshotTime().isAfter(timestamp))
                .findFirst();
    }

    @Override
    public List<DucklakeSnapshot> listSnapshots()
    {
        var snap = DUCKLAKE_SNAPSHOT.as("snap");
        return dsl.selectFrom(snap)
                .orderBy(snap.SNAPSHOT_ID.desc())
                .fetch(JdbcDucklakeCatalog::toDucklakeSnapshot);
    }

    @Override
    public List<DucklakeSnapshotChange> listSnapshotChanges()
    {
        var snapchg = DUCKLAKE_SNAPSHOT_CHANGES.as("snapchg");
        return dsl.selectFrom(snapchg)
                .orderBy(snapchg.SNAPSHOT_ID.desc())
                .fetch(JdbcDucklakeCatalog::toDucklakeSnapshotChange);
    }

    @Override
    public List<DucklakeSchema> listSchemas(long snapshotId)
    {
        var sch = DUCKLAKE_SCHEMA.as("sch");
        return dsl.selectFrom(sch)
                .where(activeAt(sch, snapshotId))
                .fetch(JdbcDucklakeCatalog::toDucklakeSchema);
    }

    @Override
    public Optional<DucklakeSchema> getSchema(String schemaName, long snapshotId)
    {
        var sch = DUCKLAKE_SCHEMA.as("sch");
        return dsl.selectFrom(sch)
                .where(sch.SCHEMA_NAME.eq(schemaName))
                .and(activeAt(sch, snapshotId))
                .fetchOptional()
                .map(JdbcDucklakeCatalog::toDucklakeSchema);
    }

    @Override
    public List<DucklakeTable> listTables(long schemaId, long snapshotId)
    {
        var tab = DUCKLAKE_TABLE.as("tab");
        return dsl.selectFrom(tab)
                .where(tab.SCHEMA_ID.eq(schemaId))
                .and(activeAt(tab, snapshotId))
                .fetch(JdbcDucklakeCatalog::toDucklakeTable);
    }

    @Override
    public Optional<DucklakeTable> getTable(String schemaName, String tableName, long snapshotId)
    {
        Optional<DucklakeSchema> schema = getSchema(schemaName, snapshotId);
        if (schema.isEmpty()) {
            return Optional.empty();
        }

        var tab = DUCKLAKE_TABLE.as("tab");
        return dsl.selectFrom(tab)
                .where(tab.SCHEMA_ID.eq(schema.get().schemaId()))
                .and(tab.TABLE_NAME.eq(tableName))
                .and(activeAt(tab, snapshotId))
                .fetchOptional()
                .map(JdbcDucklakeCatalog::toDucklakeTable);
    }

    @Override
    public Optional<DucklakeTable> getTableById(long tableId, long snapshotId)
    {
        var tab = DUCKLAKE_TABLE.as("tab");
        return dsl.selectFrom(tab)
                .where(tab.TABLE_ID.eq(tableId))
                .and(activeAt(tab, snapshotId))
                .fetchOptional()
                .map(JdbcDucklakeCatalog::toDucklakeTable);
    }

    @Override
    public List<DucklakeColumn> getTableColumns(long tableId, long snapshotId)
    {
        List<DucklakeColumn> allColumns = fetchTableColumns(tableId, snapshotId);

        Map<Long, List<DucklakeColumn>> childrenByParent = new HashMap<>();
        for (DucklakeColumn column : allColumns) {
            column.parentColumn().ifPresent(parent ->
                    childrenByParent.computeIfAbsent(parent, ignored -> new ArrayList<>()).add(column));
        }

        List<DucklakeColumn> topLevelColumns = new ArrayList<>();
        for (DucklakeColumn column : allColumns) {
            if (column.parentColumn().isEmpty()) {
                topLevelColumns.add(new DucklakeColumn(
                        column.columnId(),
                        column.beginSnapshot(),
                        column.endSnapshot(),
                        column.tableId(),
                        column.columnOrder(),
                        column.columnName(),
                        resolveColumnType(column, childrenByParent),
                        column.nullsAllowed(),
                        Optional.empty()));
            }
        }

        return topLevelColumns;
    }

    @Override
    public List<DucklakeColumn> getAllColumnsWithParentage(long tableId, long snapshotId)
    {
        return fetchTableColumns(tableId, snapshotId);
    }

    private List<DucklakeColumn> fetchTableColumns(long tableId, long snapshotId)
    {
        var col = DUCKLAKE_COLUMN.as("col");
        return dsl.selectFrom(col)
                .where(col.TABLE_ID.eq(tableId))
                .and(activeAt(col, snapshotId))
                .orderBy(col.COLUMN_ORDER, col.COLUMN_ID)
                .fetch(JdbcDucklakeCatalog::toDucklakeColumn);
    }

    @Override
    public List<DucklakeDataFile> getDataFiles(long tableId, long snapshotId)
    {
        var file = DUCKLAKE_DATA_FILE.as("file");
        var delfile = DUCKLAKE_DELETE_FILE.as("delfile");
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
        var dataFilePath = file.PATH.as("data_file_path");
        var dataFilePathIsRelative = file.PATH_IS_RELATIVE.as("data_file_path_is_relative");
        var dataFileFooterSize = file.FOOTER_SIZE.as("data_file_footer_size");
        var deleteFilePath = delfile.PATH.as("delete_file_path");
        var deleteFilePathIsRelative = delfile.PATH_IS_RELATIVE.as("delete_file_path_is_relative");
        var deleteFileFooterSize = delfile.FOOTER_SIZE.as("delete_file_footer_size");
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
                                delfile.FORMAT)
                        .from(file)
                        .leftJoin(delfile)
                        .on(file.DATA_FILE_ID.eq(delfile.DATA_FILE_ID))
                        .and(activeAt(delfile, snapshotId))
                        .where(file.TABLE_ID.eq(tableId))
                        .and(activeAt(file, snapshotId))
                        .orderBy(file.FILE_ORDER),
                r -> new DucklakeDataFile(
                        orZero(r.get(file.DATA_FILE_ID)),
                        orZero(r.get(file.TABLE_ID)),
                        orZero(r.get(file.BEGIN_SNAPSHOT)),
                        Optional.ofNullable(r.get(file.END_SNAPSHOT)),
                        orZero(r.get(file.FILE_ORDER)),
                        r.get(dataFilePath),
                        Boolean.TRUE.equals(r.get(dataFilePathIsRelative)),
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
                        Optional.ofNullable(r.get(file.MAPPING_ID))));
    }

    @Override
    public Optional<String> getLatestDataFileFormat(long tableId, long snapshotId)
    {
        // "Latest" = highest data_file_id among rows still active at the requested snapshot.
        // data_file_id is allocated from a monotonic catalog sequence at insert time, so DESC
        // order on it picks the most recently committed file (cross-snapshot, cross-partition).
        var file = DUCKLAKE_DATA_FILE.as("file");
        return Optional.ofNullable(dsl.select(file.FILE_FORMAT)
                .from(file)
                .where(file.TABLE_ID.eq(tableId))
                .and(activeAt(file, snapshotId))
                .orderBy(file.DATA_FILE_ID.desc())
                .limit(1)
                .fetchOne(file.FILE_FORMAT));
    }

    @Override
    public List<Long> findDataFileIdsInRange(long tableId, long snapshotId, ColumnRangePredicate predicate)
    {
        long columnId = predicate.columnId();

        var col = DUCKLAKE_COLUMN.as("col");
        String columnType = dsl.select(col.COLUMN_TYPE)
                .from(col)
                .where(col.TABLE_ID.eq(tableId))
                .and(col.COLUMN_ID.eq(columnId))
                .and(activeAt(col, snapshotId))
                .fetchOne(col.COLUMN_TYPE);
        if (columnType == null) {
            return List.of();
        }

        Comparable<?> lowerBound = parseStatValue(columnType, predicate.minValue());
        Comparable<?> upperBound = parseStatValue(columnType, predicate.maxValue());

        var colstats = DUCKLAKE_FILE_COLUMN_STATS.as("colstats");
        var file = DUCKLAKE_DATA_FILE.as("file");
        // Multi-table JOIN — routed through `metadata` for Quack compatibility.
        return metadata.fetch(
                        dsl,
                        dsl.select(
                                        colstats.DATA_FILE_ID,
                                        colstats.MIN_VALUE,
                                        colstats.MAX_VALUE)
                                .from(colstats)
                                .innerJoin(file)
                                .on(colstats.DATA_FILE_ID.eq(file.DATA_FILE_ID))
                                .where(colstats.TABLE_ID.eq(tableId))
                                .and(colstats.COLUMN_ID.eq(columnId))
                                .and(file.TABLE_ID.eq(tableId))
                                .and(activeAt(file, snapshotId)),
                        r -> r)
                .stream()
                .filter(r -> isWithinBounds(
                        lowerBound,
                        upperBound,
                        parseStatValue(columnType, r.get(colstats.MIN_VALUE)),
                        parseStatValue(columnType, r.get(colstats.MAX_VALUE))))
                .map(r -> r.get(colstats.DATA_FILE_ID))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<DucklakeTableStats> getTableStats(long tableId)
    {
        var tabstats = DUCKLAKE_TABLE_STATS.as("tabstats");
        return dsl.selectFrom(tabstats)
                .where(tabstats.TABLE_ID.eq(tableId))
                .fetchOptional()
                .map(JdbcDucklakeCatalog::toDucklakeTableStats);
    }

    @Override
    public List<DucklakeColumnStats> getColumnStats(long tableId, long snapshotId)
    {
        Map<Long, String> columnTypes = getTableColumns(tableId, snapshotId).stream()
                .collect(Collectors.toMap(DucklakeColumn::columnId, DucklakeColumn::columnType));

        Map<Long, long[]> countAccumulators = new HashMap<>(); // [valueCount, nullCount, sizeBytes]
        Map<Long, String> minAccumulators = new HashMap<>();
        Map<Long, String> maxAccumulators = new HashMap<>();

        var colstats = DUCKLAKE_FILE_COLUMN_STATS.as("colstats");
        var file = DUCKLAKE_DATA_FILE.as("file");
        // Multi-table JOIN — routed through `metadata` for Quack compatibility.
        metadata.fetch(
                dsl,
                dsl.select(
                                colstats.COLUMN_ID,
                                colstats.VALUE_COUNT,
                                colstats.NULL_COUNT,
                                colstats.COLUMN_SIZE_BYTES,
                                colstats.MIN_VALUE,
                                colstats.MAX_VALUE)
                        .from(colstats)
                        .innerJoin(file)
                        .on(colstats.DATA_FILE_ID.eq(file.DATA_FILE_ID))
                        .where(colstats.TABLE_ID.eq(tableId))
                        .and(file.TABLE_ID.eq(tableId))
                        .and(activeAt(file, snapshotId)),
                r -> {
                    long columnId = orZero(r.get(colstats.COLUMN_ID));
                    long[] counts = countAccumulators.computeIfAbsent(columnId, k -> new long[3]);
                    counts[0] += orZero(r.get(colstats.VALUE_COUNT));
                    counts[1] += orZero(r.get(colstats.NULL_COUNT));
                    counts[2] += orZero(r.get(colstats.COLUMN_SIZE_BYTES));

                    String columnType = columnTypes.getOrDefault(columnId, "");
                    String minValue = r.get(colstats.MIN_VALUE);
                    String maxValue = r.get(colstats.MAX_VALUE);
                    if (minValue != null) {
                        minAccumulators.merge(columnId, minValue, (a, b) -> typedMin(a, b, columnType));
                    }
                    if (maxValue != null) {
                        maxAccumulators.merge(columnId, maxValue, (a, b) -> typedMax(a, b, columnType));
                    }
                    return null;
                });

        List<DucklakeColumnStats> result = new ArrayList<>();
        for (Map.Entry<Long, long[]> entry : countAccumulators.entrySet()) {
            long columnId = entry.getKey();
            long[] counts = entry.getValue();
            result.add(new DucklakeColumnStats(
                    columnId,
                    counts[0],
                    counts[1],
                    counts[2],
                    Optional.ofNullable(minAccumulators.get(columnId)),
                    Optional.ofNullable(maxAccumulators.get(columnId))));
        }

        return result;
    }

    private static String typedMin(String a, String b, String columnType)
    {
        return typedCompare(a, b, columnType) <= 0 ? a : b;
    }

    private static String typedMax(String a, String b, String columnType)
    {
        return typedCompare(a, b, columnType) >= 0 ? a : b;
    }

    private static int typedCompare(String a, String b, String columnType)
    {
        try {
            return switch (columnType.toLowerCase(java.util.Locale.ENGLISH)) {
                case "bigint", "integer", "int", "smallint", "tinyint", "hugeint" ->
                        Long.compare(Long.parseLong(a), Long.parseLong(b));
                case "double", "real", "float", "decimal" ->
                        Double.compare(Double.parseDouble(a), Double.parseDouble(b));
                case "date" ->
                        java.time.LocalDate.parse(a).compareTo(java.time.LocalDate.parse(b));
                default -> a.compareTo(b);
            };
        }
        catch (RuntimeException ignored) {
            // If parsing fails, fall back to string comparison (conservative)
            return a.compareTo(b);
        }
    }

    @Override
    public List<DucklakePartitionSpec> getPartitionSpecs(long tableId, long snapshotId)
    {
        Map<Long, List<DucklakePartitionField>> fieldsByPartition = new LinkedHashMap<>();
        Map<Long, Long> tableIdByPartition = new HashMap<>();

        var partinfo = DUCKLAKE_PARTITION_INFO.as("partinfo");
        var partcol = DUCKLAKE_PARTITION_COLUMN.as("partcol");
        // Multi-table JOIN — routed through `metadata` for Quack compatibility.
        metadata.fetch(
                dsl,
                dsl.select(
                                partinfo.PARTITION_ID,
                                partinfo.TABLE_ID,
                                partcol.PARTITION_KEY_INDEX,
                                partcol.COLUMN_ID,
                                partcol.TRANSFORM)
                        .from(partinfo)
                        .innerJoin(partcol)
                        .on(partinfo.PARTITION_ID.eq(partcol.PARTITION_ID))
                        .and(partinfo.TABLE_ID.eq(partcol.TABLE_ID))
                        .where(partinfo.TABLE_ID.eq(tableId))
                        .and(activeAt(partinfo, snapshotId))
                        .orderBy(partinfo.PARTITION_ID, partcol.PARTITION_KEY_INDEX),
                r -> {
                    long partitionId = orZero(r.get(partinfo.PARTITION_ID));
                    tableIdByPartition.put(partitionId, orZero(r.get(partinfo.TABLE_ID)));
                    DucklakePartitionTransform.ParsedTransform parsed =
                            DucklakePartitionTransform.parseCatalogTransform(r.get(partcol.TRANSFORM));
                    fieldsByPartition.computeIfAbsent(partitionId, k -> new ArrayList<>())
                            .add(new DucklakePartitionField(
                                    (int) orZero(r.get(partcol.PARTITION_KEY_INDEX)),
                                    orZero(r.get(partcol.COLUMN_ID)),
                                    parsed.transform(),
                                    parsed.arity()));
                    return null; // mapper return discarded — using fold-into-maps idiom
                });

        List<DucklakePartitionSpec> specs = new ArrayList<>();
        for (Map.Entry<Long, List<DucklakePartitionField>> entry : fieldsByPartition.entrySet()) {
            specs.add(new DucklakePartitionSpec(entry.getKey(), tableIdByPartition.get(entry.getKey()), entry.getValue()));
        }
        return specs;
    }

    @Override
    public List<DucklakeSortKey> getSortKeys(long tableId, long snapshotId)
    {
        var sortinfo = DUCKLAKE_SORT_INFO.as("sortinfo");
        var sortexpr = DUCKLAKE_SORT_EXPRESSION.as("sortexpr");
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
                                sortexpr.NULL_ORDER)
                        .from(sortinfo)
                        .innerJoin(sortexpr)
                        .on(sortinfo.SORT_ID.eq(sortexpr.SORT_ID))
                        .and(sortinfo.TABLE_ID.eq(sortexpr.TABLE_ID))
                        .where(sortinfo.TABLE_ID.eq(tableId))
                        .and(activeAt(sortinfo, snapshotId))
                        .orderBy(sortexpr.SORT_KEY_INDEX),
                r -> new DucklakeSortKey(
                        (int) orZero(r.get(sortexpr.SORT_KEY_INDEX)),
                        r.get(sortexpr.EXPRESSION),
                        r.get(sortexpr.DIALECT),
                        DucklakeSortDirection.fromCatalog(r.get(sortexpr.SORT_DIRECTION)),
                        DucklakeNullOrder.fromCatalog(r.get(sortexpr.NULL_ORDER))));
    }

    @Override
    public Map<Long, List<DucklakeFilePartitionValue>> getFilePartitionValues(long tableId, long snapshotId)
    {
        Map<Long, List<DucklakeFilePartitionValue>> result = new HashMap<>();

        var partval = DUCKLAKE_FILE_PARTITION_VALUE.as("partval");
        var file = DUCKLAKE_DATA_FILE.as("file");
        // Multi-table JOIN — routed through `metadata` for Quack compatibility.
        metadata.fetch(
                dsl,
                dsl.select(
                                partval.DATA_FILE_ID,
                                partval.PARTITION_KEY_INDEX,
                                partval.PARTITION_VALUE)
                        .from(partval)
                        .innerJoin(file)
                        .on(partval.DATA_FILE_ID.eq(file.DATA_FILE_ID))
                        .and(partval.TABLE_ID.eq(file.TABLE_ID))
                        .where(partval.TABLE_ID.eq(tableId))
                        .and(activeAt(file, snapshotId)),
                r -> {
                    long dataFileId = orZero(r.get(partval.DATA_FILE_ID));
                    result.computeIfAbsent(dataFileId, k -> new ArrayList<>())
                            .add(new DucklakeFilePartitionValue(
                                    dataFileId,
                                    (int) orZero(r.get(partval.PARTITION_KEY_INDEX)),
                                    r.get(partval.PARTITION_VALUE)));
                    return null;
                });

        return result;
    }

    @Override
    public Map<Long, Map<Long, String>> getNameMaps(Set<Long> mappingIds)
    {
        if (mappingIds == null || mappingIds.isEmpty()) {
            return Map.of();
        }
        var nm = DUCKLAKE_NAME_MAPPING.as("nm");
        Map<Long, Map<Long, String>> result = new HashMap<>();
        dsl.select(nm.MAPPING_ID, nm.TARGET_FIELD_ID, nm.SOURCE_NAME)
                .from(nm)
                .where(nm.MAPPING_ID.in(mappingIds))
                // Top-level entries only — nested struct/list/map source-name resolution
                // is handled by Trino's reader once we descend into a matched group field.
                .and(nm.PARENT_COLUMN.isNull())
                // Exclude hive partition entries — those have no parquet column to find.
                .and(nm.IS_PARTITION.isFalse().or(nm.IS_PARTITION.isNull()))
                .forEach(r -> {
                    Long mappingId = r.get(nm.MAPPING_ID);
                    Long fieldId = r.get(nm.TARGET_FIELD_ID);
                    String sourceName = r.get(nm.SOURCE_NAME);
                    if (mappingId != null && fieldId != null && sourceName != null) {
                        result.computeIfAbsent(mappingId, k -> new HashMap<>())
                                .put(fieldId, sourceName);
                    }
                });
        return result;
    }

    @Override
    public List<DucklakeInlinedDataInfo> getInlinedDataInfos(long tableId, long snapshotId)
    {
        // A table can have multiple inlined data tables (one per schema version).
        var inlined = DUCKLAKE_INLINED_DATA_TABLES.as("inlined");
        var snap = DUCKLAKE_SNAPSHOT.as("snap");
        try {
            return dsl.select(
                            inlined.TABLE_ID,
                            inlined.TABLE_NAME,
                            inlined.SCHEMA_VERSION)
                    .from(inlined)
                    .where(inlined.TABLE_ID.eq(tableId))
                    .and(inlined.SCHEMA_VERSION.le(
                            DSL.select(snap.SCHEMA_VERSION)
                                    .from(snap)
                                    .where(snap.SNAPSHOT_ID.eq(snapshotId))))
                    .orderBy(inlined.SCHEMA_VERSION)
                    .fetch()
                    .stream()
                    .filter(r -> {
                        InlinedDataTable inlinedTable = InlinedDataTable.of(
                                orZero(r.get(inlined.TABLE_ID)),
                                orZero(r.get(inlined.SCHEMA_VERSION)));
                        if (!inlinedTable.existsAsTable(dsl)) {
                            // Catalog metadata can point to a dropped/non-materialized inlined table.
                            // Treat this as "no inlined data" so scan planning does not emit a dead split.
                            log.log(System.Logger.Level.DEBUG, "Inlined data table {0} not available for table {1}", inlinedTable.name, tableId);
                            return false;
                        }
                        return true;
                    })
                    .map(r -> new DucklakeInlinedDataInfo(
                            orZero(r.get(inlined.TABLE_ID)),
                            r.get(inlined.TABLE_NAME),
                            orZero(r.get(inlined.SCHEMA_VERSION))))
                    .collect(Collectors.toList());
        }
        catch (DataAccessException e) {
            // ducklake_inlined_data_tables may not exist in catalogs that never used inlining
            log.log(System.Logger.Level.DEBUG, "Could not query inlined data tables (table may not exist): {0}", e.getMessage());
            return List.of();
        }
    }

    @Override
    public boolean hasInlinedRows(long tableId, long schemaVersion, long snapshotId)
    {
        InlinedDataTable inlined = InlinedDataTable.of(tableId, schemaVersion);
        try {
            return dsl.fetchExists(
                    DSL.selectOne()
                            .from(inlined.table)
                            .where(inlined.activeAt(snapshotId)));
        }
        catch (DataAccessException e) {
            log.log(System.Logger.Level.DEBUG, "Could not probe inlined data rows from {0} (table may not exist): {1}", inlined.name, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean hasInlinedDeletes(long tableId, long snapshotId)
    {
        // ducklake_inlined_delete_<tableId> is created lazily by DuckDB the first
        // time DATA_INLINING_ROW_LIMIT causes a deletion to be inlined; before
        // that it doesn't exist at all. The probe catches the
        // table-doesn't-exist case and returns false.
        // Schema (per upstream data_inlining.md): file_id, row_id, begin_snapshot.
        // No end_snapshot — once an inlined delete row exists for a snapshot, it's
        // permanent until compaction rewrites the data file.
        String inlinedDeleteName = "ducklake_inlined_delete_" + tableId;
        Table<?> tab = DSL.table(DSL.name(inlinedDeleteName));
        Field<Long> beginSnapshot = DSL.field(DSL.name("begin_snapshot"), Long.class);
        try {
            return dsl.fetchExists(
                    DSL.selectOne()
                            .from(tab)
                            .where(beginSnapshot.le(snapshotId)));
        }
        catch (DataAccessException e) {
            log.log(System.Logger.Level.DEBUG, "Could not probe inlined deletions from {0} (table may not exist): {1}", inlinedDeleteName, e.getMessage());
            return false;
        }
    }

    @Override
    public Map<Long, Set<Long>> getInlinedDeletes(long tableId, long snapshotId)
    {
        // Schema (per upstream data_inlining.md):
        //   ducklake_inlined_delete_<tableId>(file_id BIGINT, row_id BIGINT, begin_snapshot BIGINT)
        // file_id = ducklake_data_file.data_file_id
        // row_id  = deleted row's file-local position
        // No end_snapshot — rows accumulate until compaction rewrites the data file.
        // Table is created lazily by DuckDB the first time DATA_INLINING_ROW_LIMIT
        // causes a deletion to be inlined; absence is the common case.
        String inlinedDeleteName = "ducklake_inlined_delete_" + tableId;
        Table<?> tab = DSL.table(DSL.name(inlinedDeleteName));
        Field<Long> fileId = DSL.field(DSL.name("file_id"), Long.class);
        Field<Long> rowId = DSL.field(DSL.name("row_id"), Long.class);
        Field<Long> beginSnapshot = DSL.field(DSL.name("begin_snapshot"), Long.class);
        try {
            var result = dsl.select(fileId, rowId)
                    .from(tab)
                    .where(beginSnapshot.le(snapshotId))
                    .fetch();
            Map<Long, Set<Long>> grouped = new HashMap<>();
            for (var rec : result) {
                Long fid = rec.get(fileId);
                Long rid = rec.get(rowId);
                if (fid == null || rid == null) {
                    continue;
                }
                grouped.computeIfAbsent(fid, k -> new HashSet<>()).add(rid);
            }
            return grouped;
        }
        catch (DataAccessException e) {
            log.log(System.Logger.Level.DEBUG, "Could not read inlined deletions from {0} (table may not exist): {1}", inlinedDeleteName, e.getMessage());
            return Map.of();
        }
    }

    @Override
    public List<List<Object>> readInlinedData(long tableId, long schemaVersion, long snapshotId, List<DucklakeColumn> columns)
    {
        if (columns.isEmpty()) {
            return List.of();
        }

        InlinedDataTable inlined = InlinedDataTable.of(tableId, schemaVersion);

        OptionalLong sourceSchemaSnapshot = getSnapshotIdForSchemaVersion(tableId, schemaVersion, snapshotId);
        if (sourceSchemaSnapshot.isEmpty()) {
            return List.of();
        }

        Map<Long, DucklakeColumn> sourceColumnsById = getTableColumns(tableId, sourceSchemaSnapshot.getAsLong()).stream()
                .collect(Collectors.toMap(DucklakeColumn::columnId, column -> column));

        List<Field<?>> projected = new ArrayList<>(columns.size());
        for (int index = 0; index < columns.size(); index++) {
            String alias = "c" + index;
            DucklakeColumn sourceColumn = sourceColumnsById.get(columns.get(index).columnId());
            projected.add(sourceColumn == null
                    ? DSL.inline((Object) null).as(alias)
                    : DSL.field(DSL.name(sourceColumn.columnName())).as(alias));
        }

        try {
            Result<Record> result = dsl.select(projected)
                    .from(inlined.table)
                    .where(inlined.activeAt(snapshotId))
                    .orderBy(DSL.field(DSL.name("row_id")))
                    .fetch();

            int columnCount = columns.size();
            List<List<Object>> rows = new ArrayList<>(result.size());
            for (Record rec : result) {
                List<Object> row = new ArrayList<>(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    row.add(rec.get(i));
                }
                rows.add(row);
            }
            return rows;
        }
        catch (DataAccessException e) {
            // The inlined data table may not exist if the table was created but never had data inserted,
            // or if the inlined data was flushed to Parquet files. Return empty in these cases.
            log.log(System.Logger.Level.DEBUG, "Could not read inlined data from {0} (table may not exist): {1}", inlined.name, e.getMessage());
            return List.of();
        }
    }

    private OptionalLong getSnapshotIdForSchemaVersion(long tableId, long schemaVersion, long snapshotId)
    {
        // Prefer table-scoped schema version rows when available.
        // Some catalogs include ducklake_schema_versions.table_id (DuckDB behavior),
        // which gives an unambiguous snapshot where this table's schema version was introduced.
        var schver = DUCKLAKE_SCHEMA_VERSIONS.as("schver");
        try {
            Long tableScoped = dsl.select(schver.BEGIN_SNAPSHOT)
                    .from(schver)
                    .where(schver.TABLE_ID.eq(tableId))
                    .and(schver.SCHEMA_VERSION.eq(schemaVersion))
                    .and(schver.BEGIN_SNAPSHOT.le(snapshotId))
                    .orderBy(schver.BEGIN_SNAPSHOT.desc())
                    .limit(1)
                    .fetchOne(schver.BEGIN_SNAPSHOT);
            if (tableScoped != null) {
                return OptionalLong.of(tableScoped);
            }
        }
        catch (DataAccessException e) {
            // Fallback for catalogs without table_id in ducklake_schema_versions or older metadata.
            log.log(System.Logger.Level.DEBUG, "Could not resolve schema version via ducklake_schema_versions for table {0}: {1}", tableId, e.getMessage());
        }

        // Backward-compatible fallback: resolve by snapshot.schema_version only.
        var snap = DUCKLAKE_SNAPSHOT.as("snap");
        Long fallback = dsl.select(snap.SNAPSHOT_ID)
                .from(snap)
                .where(snap.SCHEMA_VERSION.eq(schemaVersion))
                .and(snap.SNAPSHOT_ID.le(snapshotId))
                .orderBy(snap.SNAPSHOT_ID.desc())
                .limit(1)
                .fetchOne(snap.SNAPSHOT_ID);
        return fallback == null ? OptionalLong.empty() : OptionalLong.of(fallback);
    }

    /**
     * Handle to a dynamic {@code ducklake_inlined_data_{tableId}_{schemaVersion}} table that
     * codegen doesn't know about. Bundles the table reference with its {@code begin_snapshot} /
     * {@code end_snapshot} fields so the per-method setup dance reduces to one line.
     */
    private record InlinedDataTable(
            String name,
            Table<?> table,
            Field<Long> beginSnapshot,
            Field<Long> endSnapshot)
    {
        static InlinedDataTable of(long tableId, long schemaVersion)
        {
            String name = String.format("ducklake_inlined_data_%d_%d", tableId, schemaVersion);
            return new InlinedDataTable(
                    name,
                    DSL.table(DSL.name(name)),
                    DSL.field(DSL.name("begin_snapshot"), Long.class),
                    DSL.field(DSL.name("end_snapshot"), Long.class));
        }

        Condition activeAt(long snapshotId)
        {
            return SnapshotRange.activeAt(beginSnapshot, endSnapshot, snapshotId);
        }

        boolean existsAsTable(DSLContext dsl)
        {
            try {
                dsl.selectOne().from(table).where(DSL.falseCondition()).fetch();
                return true;
            }
            catch (DataAccessException e) {
                return false;
            }
        }
    }

    // Snapshot-change recording lives on the typed {@link WriteChange} hierarchy.
    // Quoting and joining used to be local helpers here; they now live on
    // {@code WriteChange} so that the conflict-checking machinery and the
    // {@code ducklake_snapshot_changes.changes_made} serializer share one source
    // of truth.

    @Override
    public Optional<String> getDataPath()
    {
        var meta = DUCKLAKE_METADATA.as("meta");
        return dsl.select(meta.VALUE)
                .from(meta)
                .where(meta.KEY.eq("data_path"))
                .fetchOptional(meta.VALUE);
    }

    // ==================== View operations ====================

    @Override
    public List<DucklakeView> listViews(long schemaId, long snapshotId)
    {
        var view = DUCKLAKE_VIEW.as("view");
        return dsl.selectFrom(view)
                .where(view.SCHEMA_ID.eq(schemaId))
                .and(activeAt(view, snapshotId))
                .fetch(JdbcDucklakeCatalog::toDucklakeView);
    }

    @Override
    public Optional<DucklakeView> getView(String schemaName, String viewName, long snapshotId)
    {
        Optional<DucklakeSchema> schema = getSchema(schemaName, snapshotId);
        if (schema.isEmpty()) {
            return Optional.empty();
        }

        var view = DUCKLAKE_VIEW.as("view");
        return dsl.selectFrom(view)
                .where(view.SCHEMA_ID.eq(schema.get().schemaId()))
                .and(view.VIEW_NAME.eq(viewName))
                .and(activeAt(view, snapshotId))
                .fetchOptional()
                .map(JdbcDucklakeCatalog::toDucklakeView);
    }

    // Write transaction infrastructure

    @FunctionalInterface
    interface WriteTransactionAction
    {
        void execute(DucklakeWriteTransaction transaction)
                throws SQLException;
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
    volatile Runnable beforeWriteTransactionAction = () -> {};

    /**
     * Executes a write operation within an atomic snapshot transaction.
     * Handles connection management, snapshot creation, change tracking,
     * and commit/rollback. The caller provides a callback that performs
     * its mutations using the transaction context.
     *
     * <p>On a {@link TransactionConflictException} (PK collision on
     * {@code ducklake_snapshot} or detected lineage advance), the operation
     * is retried with exponential backoff up to {@link #MAX_RETRY_COUNT}
     * times. After exhaustion, the most recent conflict is rethrown wrapped
     * with an "exceeded retry count" message. This matches upstream DuckLake's
     * retry semantics so that low-rate contention is absorbed transparently.
     */
    private void executeWriteTransaction(String operationDescription, WriteTransactionAction action)
    {
        // Captured once on attempt 1, propagated across retries so the
        // change-vs-change conflict matrix on retry can ask "what committed
        // since this transaction started?" — mirrors upstream's
        // `transaction_snapshot` captured outside the retry loop in
        // ducklake_transaction.cpp:2455.
        long[] transactionStartSnapshotId = {-1L};
        WriteTransactionRetry.retryOnConflict(
                MAX_RETRY_COUNT,
                INITIAL_RETRY_WAIT_MS,
                RETRY_BACKOFF_MULTIPLIER,
                Thread::sleep,
                operationDescription,
                () -> attemptWriteTransaction(operationDescription, action, transactionStartSnapshotId));
    }

    private void attemptWriteTransaction(
            String operationDescription,
            WriteTransactionAction action,
            long[] transactionStartSnapshotId)
    {
        var snap = DUCKLAKE_SNAPSHOT.as("snap");
        var schver = DUCKLAKE_SCHEMA_VERSIONS.as("schver");
        var snapchg = DUCKLAKE_SNAPSHOT_CHANGES.as("snapchg");
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            DSLContext txDsl = forConnection(conn);
            long baseSnapshotId = -1;
            try {
                // 1. Read current snapshot state. Routed through `metadata` because
                // this is a same-table multi-scan (`WHERE id = (SELECT max(id) FROM
                // same_table)`), which the Quack RPC optimizer rejects on attached
                // metadata catalogs. On PG / local DuckDB the helper is a pass-through.
                DucklakeSnapshotRecord snapshotRow = metadata.fetchOne(
                        txDsl,
                        txDsl.selectFrom(snap)
                                .where(snap.SNAPSHOT_ID.eq(
                                        DSL.select(DSL.max(snap.SNAPSHOT_ID)).from(snap))));
                if (snapshotRow == null) {
                    throw new IllegalStateException("No snapshots found");
                }
                long currentSnapshotId = snapshotRow.getSnapshotId();
                baseSnapshotId = currentSnapshotId;
                if (transactionStartSnapshotId[0] == -1L) {
                    transactionStartSnapshotId[0] = currentSnapshotId;
                }
                long schemaVersion = orZero(snapshotRow.getSchemaVersion());
                long nextCatalogId = orZero(snapshotRow.getNextCatalogId());
                long nextFileId = orZero(snapshotRow.getNextFileId());

                // 2. Execute the caller's mutations
                DucklakeWriteTransaction tx = new DucklakeWriteTransaction(
                        conn, txDsl, currentSnapshotId, schemaVersion, nextCatalogId, nextFileId);

                // Test seam: lets concurrency tests park this attempt before it
                // does any writes, so a competing committer can advance the
                // snapshot without deadlocking on row locks this tx would hold.
                beforeWriteTransactionAction.run();

                action.execute(tx);

                // 3. Strict optimistic conflict check: if snapshot lineage advanced, abort.
                ensureSnapshotLineageUnchanged(txDsl, tx.getCurrentSnapshotId(), operationDescription);

                // 3b. Logical conflict check: validate the action's payload still
                // references entities that are active at the current snapshot. This
                // catches the case the strict lineage check + retry alone misses:
                // the retry's action re-runs with stale per-call args (table IDs,
                // fragment column / data-file IDs) captured before any prior
                // attempt. Throws non-retryable LogicalConflictException on a
                // mismatch so the retry loop bails out instead of burning the
                // retry budget on a guaranteed-fail.
                LogicalConflictCheck.run(tx, operationDescription);

                // 3c. Change-vs-change conflict matrix (port of upstream
                // CheckForConflicts at ducklake_transaction.cpp:1184-1314). Only
                // runs when an earlier attempt of THIS transaction's retry loop
                // saw an older snapshot — i.e. when other transactions committed
                // between transactionStartSnapshotId (captured on attempt 1) and
                // currentSnapshotId. Catches dueling-name commits the state-based
                // check above can't see (no UNIQUE on (schema_id, name) — see
                // ducklake_metadata_manager.cpp:198-200).
                if (currentSnapshotId > transactionStartSnapshotId[0]) {
                    runConflictMatrix(txDsl, tx.getChanges(),
                            transactionStartSnapshotId[0], currentSnapshotId);
                }

                // 4. Create new snapshot row (with final allocated IDs)
                insertSnapshotRow(txDsl, tx, operationDescription);

                // 5. Insert schema_versions row if schema version changed
                if (tx.getSchemaVersion() != schemaVersion) {
                    Long schemaVersionTableId = tx.getSchemaVersionTableId() >= 0
                            ? tx.getSchemaVersionTableId()
                            : null;
                    txDsl.insertInto(schver)
                            .set(schver.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                            .set(schver.SCHEMA_VERSION, tx.getSchemaVersion())
                            .set(schver.TABLE_ID, schemaVersionTableId)
                            .execute();
                }

                // 6. Insert snapshot changes (comma-separated per spec, one row per snapshot)
                if (!tx.getChanges().isEmpty()) {
                    txDsl.insertInto(snapchg)
                            .set(snapchg.SNAPSHOT_ID, tx.getNewSnapshotId())
                            .set(snapchg.CHANGES_MADE, WriteChange.formatChangesMade(tx.getChanges()))
                            .execute();
                }

                conn.commit();
            }
            catch (Exception e) {
                conn.rollback();
                if (hasTransactionConflict(e)) {
                    throw (RuntimeException) e;
                }
                if (isMetadataPrimaryKeyConflict(e)) {
                    long currentSnapshot = readLatestSnapshotId(txDsl);
                    throw transactionConflictException(txDsl, baseSnapshotId, currentSnapshot, operationDescription, e);
                }
                throw new RuntimeException("Failed to " + operationDescription, e);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to " + operationDescription, e);
        }
    }

    private void ensureSnapshotLineageUnchanged(DSLContext ctx, long expectedSnapshotId, String operationDescription)
    {
        long currentSnapshotId = readLatestSnapshotId(ctx);
        if (currentSnapshotId != expectedSnapshotId) {
            throw transactionConflictException(ctx, expectedSnapshotId, currentSnapshotId, operationDescription, null);
        }
    }

    private void insertSnapshotRow(DSLContext ctx, DucklakeWriteTransaction tx, String operationDescription)
    {
        var snap = DUCKLAKE_SNAPSHOT.as("snap");
        try {
            ctx.insertInto(snap)
                    .set(snap.SNAPSHOT_ID, tx.getNewSnapshotId())
                    .set(snap.SNAPSHOT_TIME, DSL.currentOffsetDateTime())
                    .set(snap.SCHEMA_VERSION, tx.getSchemaVersion())
                    .set(snap.NEXT_CATALOG_ID, tx.getFinalNextCatalogId())
                    .set(snap.NEXT_FILE_ID, tx.getFinalNextFileId())
                    .execute();
        }
        catch (DataAccessException e) {
            SQLException cause = findSqlException(e);
            if (cause != null && isDuplicateKeyViolation(cause)) {
                long currentSnapshotId = readLatestSnapshotId(ctx);
                throw transactionConflictException(ctx, tx.getCurrentSnapshotId(), currentSnapshotId, operationDescription, e);
            }
            throw e;
        }
    }

    private TransactionConflictException transactionConflictException(
            DSLContext ctx,
            long expectedSnapshotId,
            long currentSnapshotId,
            String operationDescription,
            Throwable cause)
    {
        String interveningChanges = getInterveningChangesSummary(ctx, expectedSnapshotId, currentSnapshotId);
        String message = "Concurrent DuckLake commit while attempting to " + operationDescription +
                ": expected base snapshot " + expectedSnapshotId +
                ", but current snapshot is " + currentSnapshotId +
                ". Intervening changes: " + interveningChanges;
        return new TransactionConflictException(message, cause);
    }

    private String getInterveningChangesSummary(DSLContext ctx, long fromSnapshotExclusive, long toSnapshotInclusive)
    {
        if (toSnapshotInclusive <= fromSnapshotExclusive) {
            return "none";
        }

        var snapchg = DUCKLAKE_SNAPSHOT_CHANGES.as("snapchg");
        List<String> changes = ctx.select(
                        snapchg.SNAPSHOT_ID,
                        snapchg.CHANGES_MADE)
                .from(snapchg)
                .where(snapchg.SNAPSHOT_ID.gt(fromSnapshotExclusive))
                .and(snapchg.SNAPSHOT_ID.le(toSnapshotInclusive))
                .orderBy(snapchg.SNAPSHOT_ID)
                .limit(CONFLICT_CHANGE_SUMMARY_LIMIT)
                .fetch()
                .stream()
                .map(r -> r.get(snapchg.SNAPSHOT_ID)
                        + ":" + r.get(snapchg.CHANGES_MADE))
                .collect(Collectors.toList());

        if (changes.isEmpty()) {
            return "snapshot advanced without snapshot_changes rows";
        }
        return String.join("; ", changes);
    }

    private static void runConflictMatrix(
            DSLContext ctx,
            List<WriteChange> myChanges,
            long fromSnapshotExclusive,
            long toSnapshotInclusive)
    {
        var snapchg = DUCKLAKE_SNAPSHOT_CHANGES.as("snapchg");
        List<String> rows = ctx.select(snapchg.CHANGES_MADE)
                .from(snapchg)
                .where(snapchg.SNAPSHOT_ID.gt(fromSnapshotExclusive))
                .and(snapchg.SNAPSHOT_ID.le(toSnapshotInclusive))
                .orderBy(snapchg.SNAPSHOT_ID)
                .fetch(snapchg.CHANGES_MADE);
        InterveningChanges other = InterveningChanges.parseAll(rows);
        ConflictMatrix.check(myChanges, other);

        // Finer-grained delete-vs-delete file-overlap check. Two transactions
        // that each write a delete file for the SAME data_file_id silently lose
        // one set of deletions (the second transaction's INSERT into
        // ducklake_delete_file end-snapshots the first). Upstream catches this
        // at ducklake_transaction.cpp:1259-1283 by intersecting MY new
        // delete-file data_file_ids with the set returned by
        // GetFilesDeletedOrDroppedAfterSnapshot.
        checkDeleteFileOverlap(ctx, myChanges, other, fromSnapshotExclusive, toSnapshotInclusive);
    }

    private static void checkDeleteFileOverlap(
            DSLContext ctx,
            List<WriteChange> myChanges,
            InterveningChanges other,
            long fromSnapshotExclusive,
            long toSnapshotInclusive)
    {
        // Collect my data_file_ids whose tables also had intervening deletes.
        // Outside that overlap, no contention is possible.
        Set<Long> myFileIds = new HashSet<>();
        for (WriteChange c : myChanges) {
            if (c instanceof WriteChange.DeletedFromTable d
                    && other.tablesDeletedFrom.contains(d.tableId())) {
                myFileIds.addAll(d.referencedDataFileIds());
            }
        }
        if (myFileIds.isEmpty()) {
            return;
        }

        // Any delete file inserted in the intervening snapshot range that
        // targets one of my data_file_ids is a conflict. Use begin_snapshot
        // (when the row was inserted) to find intervening deletes.
        var delfile = DUCKLAKE_DELETE_FILE.as("delfile");
        Set<Long> contendedFileIds = ctx.select(delfile.DATA_FILE_ID)
                .from(delfile)
                .where(delfile.BEGIN_SNAPSHOT.gt(fromSnapshotExclusive))
                .and(delfile.BEGIN_SNAPSHOT.le(toSnapshotInclusive))
                .and(delfile.DATA_FILE_ID.in(myFileIds))
                .fetchSet(delfile.DATA_FILE_ID);
        if (!contendedFileIds.isEmpty()) {
            throw new LogicalConflictException(
                    "Transaction conflict - attempting to delete from data_file_id(s) "
                            + new TreeSet<>(contendedFileIds)
                            + " - but another transaction also wrote delete files for the same"
                            + " data files. The second-committing delete would silently end-snapshot"
                            + " the first transaction's deletions, so this conflict is not retried.");
        }
    }

    private long readLatestSnapshotId(DSLContext ctx)
    {
        var snap = DUCKLAKE_SNAPSHOT.as("snap");
        Long maxId = ctx.select(DSL.max(snap.SNAPSHOT_ID))
                .from(snap)
                .fetchOne(0, Long.class);
        if (maxId == null) {
            throw new IllegalStateException("No snapshots found");
        }
        return maxId;
    }

    private static boolean hasTransactionConflict(Throwable throwable)
    {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof TransactionConflictException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static boolean isMetadataPrimaryKeyConflict(Throwable throwable)
    {
        SQLException sqlException = findSqlException(throwable);
        if (sqlException == null || !isDuplicateKeyViolation(sqlException)) {
            return false;
        }

        String message = sqlException.getMessage();
        if (message == null) {
            return false;
        }

        String lowerMessage = message.toLowerCase(ENGLISH);
        if (lowerMessage.contains("_pkey")) {
            return true;
        }

        return lowerMessage.contains("ducklake_snapshot.snapshot_id")
                || lowerMessage.contains("ducklake_schema.schema_id")
                || lowerMessage.contains("ducklake_table.table_id")
                || lowerMessage.contains("ducklake_view.view_id")
                || lowerMessage.contains("ducklake_column.column_id")
                || lowerMessage.contains("ducklake_partition_info.partition_id")
                || lowerMessage.contains("ducklake_data_file.data_file_id")
                || lowerMessage.contains("ducklake_delete_file.delete_file_id");
    }

    private static SQLException findSqlException(Throwable throwable)
    {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SQLException sqlException) {
                return sqlException;
            }
            current = current.getCause();
        }
        return null;
    }

    private static boolean isDuplicateKeyViolation(SQLException exception)
    {
        String sqlState = exception.getSQLState();
        if ("23505".equals(sqlState)) {
            return true;
        }

        if (exception.getErrorCode() == 19) {
            return true;
        }

        String message = exception.getMessage();
        return message != null && (
                message.contains("duplicate key value violates unique constraint") ||
                        message.contains("UNIQUE constraint failed"));
    }

    @Override
    public void createView(String schemaName, String viewName, String viewSql, String dialect, String viewMetadata)
    {
        executeWriteTransaction("create view " + schemaName + "." + viewName, tx -> {
            long schemaId = tx.resolveSchemaId(schemaName);
            long viewId = tx.allocateCatalogId();
            tx.recordChange(new WriteChange.CreatedView(schemaId, schemaName, viewName));

            insertViewRow(tx, viewId, UUID.fromString(newCatalogUuid()), schemaId, viewName, dialect, viewSql, viewMetadata);
            tx.incrementSchemaVersion();
        });
    }

    @Override
    public void dropView(String schemaName, String viewName)
    {
        executeWriteTransaction("drop view " + schemaName + "." + viewName, tx -> {
            long schemaId = tx.resolveSchemaId(schemaName);
            ActiveViewRow view = resolveActiveViewRow(tx, schemaId, viewName);
            endSnapshotActiveView(tx, view.viewId());
            tx.recordChange(new WriteChange.DroppedView(view.viewId()));
            tx.incrementSchemaVersion();
        });
    }

    @Override
    public void renameView(
            String sourceSchemaName,
            String sourceViewName,
            String targetSchemaName,
            String targetViewName)
    {
        executeWriteTransaction(
                "rename view " + sourceSchemaName + "." + sourceViewName + " to " + targetSchemaName + "." + targetViewName,
                tx -> {
                    long sourceSchemaId = tx.resolveSchemaId(sourceSchemaName);
                    ActiveViewRow sourceView = resolveActiveViewRow(tx, sourceSchemaId, sourceViewName);
                    long targetSchemaId = tx.resolveSchemaId(targetSchemaName);

                    if (hasActiveTable(tx, targetSchemaId, targetViewName) || hasActiveView(tx, targetSchemaId, targetViewName)) {
                        throw new RuntimeException("Relation already exists: " + targetSchemaName + "." + targetViewName);
                    }

                    endSnapshotActiveView(tx, sourceView.viewId());
                    insertViewRow(
                            tx,
                            sourceView.viewId(),
                            sourceView.viewUuid(),
                            targetSchemaId,
                            targetViewName,
                            sourceView.dialect(),
                            sourceView.sql(),
                            sourceView.viewMetadata().orElse(null));
                    // Upstream's ParseChangeType does not recognize `renamed_view`; a rename is
                    // semantically a schema/name change, so emit `altered_view` to stay
                    // spec-conformant with DuckDB's ducklake_snapshots() parser.
                    tx.recordChange(new WriteChange.AlteredView(sourceView.viewId()));
                    tx.incrementSchemaVersion();
                });
    }

    @Override
    public void replaceViewMetadata(String schemaName, String viewName, String viewSql, String dialect, String viewMetadata)
    {
        executeWriteTransaction("alter view " + schemaName + "." + viewName, tx -> {
            long schemaId = tx.resolveSchemaId(schemaName);
            ActiveViewRow view = resolveActiveViewRow(tx, schemaId, viewName);

            endSnapshotActiveView(tx, view.viewId());
            insertViewRow(tx, view.viewId(), view.viewUuid(), schemaId, viewName, dialect, viewSql, viewMetadata);
            tx.recordChange(new WriteChange.AlteredView(view.viewId()));
            tx.incrementSchemaVersion();
        });
    }

    private ActiveViewRow resolveActiveViewRow(DucklakeWriteTransaction tx, long schemaId, String viewName)
    {
        var view = DUCKLAKE_VIEW.as("view");
        Record row = tx.dsl().select(
                        view.VIEW_ID,
                        view.VIEW_UUID,
                        view.SCHEMA_ID,
                        view.VIEW_NAME,
                        view.DIALECT,
                        view.SQL,
                        view.COLUMN_ALIASES)
                .from(view)
                .where(view.SCHEMA_ID.eq(schemaId))
                .and(view.VIEW_NAME.eq(viewName))
                .and(activeAt(view, tx.getCurrentSnapshotId()))
                .fetchOne();
        if (row == null) {
            throw new RuntimeException("View not found: schema_id=" + schemaId + ", view_name=" + viewName);
        }
        return new ActiveViewRow(
                orZero(row.get(view.VIEW_ID)),
                row.get(view.VIEW_UUID),
                orZero(row.get(view.SCHEMA_ID)),
                row.get(view.VIEW_NAME),
                row.get(view.DIALECT),
                row.get(view.SQL),
                Optional.ofNullable(row.get(view.COLUMN_ALIASES)));
    }

    private void insertViewRow(
            DucklakeWriteTransaction tx,
            long viewId,
            UUID viewUuid,
            long schemaId,
            String viewName,
            String dialect,
            String viewSql,
            String viewMetadata)
    {
        var view = DUCKLAKE_VIEW.as("view");
        tx.dsl().insertInto(view)
                .set(view.VIEW_ID, viewId)
                .set(view.VIEW_UUID, viewUuid)
                .set(view.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                .set(view.SCHEMA_ID, schemaId)
                .set(view.VIEW_NAME, viewName)
                .set(view.DIALECT, dialect)
                .set(view.SQL, viewSql)
                .set(view.COLUMN_ALIASES, viewMetadata)
                .execute();
    }

    private void endSnapshotActiveView(DucklakeWriteTransaction tx, long viewId)
    {
        var view = DUCKLAKE_VIEW.as("view");
        int updatedRows = tx.dsl().update(view)
                .set(view.END_SNAPSHOT, tx.getNewSnapshotId())
                .where(view.VIEW_ID.eq(viewId))
                .and(view.END_SNAPSHOT.isNull())
                .execute();
        if (updatedRows == 0) {
            throw new RuntimeException("View not found: " + viewId);
        }
    }

    private static boolean hasActiveView(DucklakeWriteTransaction tx, long schemaId, String viewName)
    {
        var view = DUCKLAKE_VIEW.as("view");
        return tx.dsl().fetchExists(
                DSL.selectOne()
                        .from(view)
                        .where(view.SCHEMA_ID.eq(schemaId))
                        .and(view.VIEW_NAME.eq(viewName))
                        .and(activeAt(view, tx.getCurrentSnapshotId())));
    }

    private static boolean hasActiveTable(DucklakeWriteTransaction tx, long schemaId, String tableName)
    {
        var tab = DUCKLAKE_TABLE.as("tab");
        return tx.dsl().fetchExists(
                DSL.selectOne()
                        .from(tab)
                        .where(tab.SCHEMA_ID.eq(schemaId))
                        .and(tab.TABLE_NAME.eq(tableName))
                        .and(activeAt(tab, tx.getCurrentSnapshotId())));
    }

    private record ActiveViewRow(
            long viewId,
            UUID viewUuid,
            long schemaId,
            String viewName,
            String dialect,
            String sql,
            Optional<String> viewMetadata) {}

    // ==================== Schema DDL ====================

    @Override
    public void createSchema(String schemaName)
    {
        var sch = DUCKLAKE_SCHEMA.as("sch");
        executeWriteTransaction("create schema " + schemaName, tx -> {
            long schemaId = tx.allocateCatalogId();
            tx.recordChange(new WriteChange.CreatedSchema(schemaName));

            tx.dsl().insertInto(sch)
                    .set(sch.SCHEMA_ID, schemaId)
                    .set(sch.SCHEMA_UUID, UUID.fromString(newCatalogUuid()))
                    .set(sch.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                    .set(sch.SCHEMA_NAME, schemaName)
                    .set(sch.PATH, schemaName + "/")
                    .set(sch.PATH_IS_RELATIVE, true)
                    .execute();

            tx.incrementSchemaVersion();
        });
    }

    @Override
    public void dropSchema(String schemaName)
    {
        var sch = DUCKLAKE_SCHEMA.as("sch");
        executeWriteTransaction("drop schema " + schemaName, tx -> {
            long schemaId = tx.resolveSchemaId(schemaName);

            if (tx.hasTablesInSchema(schemaId)) {
                throw new RuntimeException("Cannot drop schema " + schemaName + ": schema is not empty");
            }

            tx.recordChange(new WriteChange.DroppedSchema(schemaId, schemaName));

            tx.dsl().update(sch)
                    .set(sch.END_SNAPSHOT, tx.getNewSnapshotId())
                    .where(sch.SCHEMA_ID.eq(schemaId))
                    .and(sch.END_SNAPSHOT.isNull())
                    .execute();

            tx.incrementSchemaVersion();
        });
    }

    // ==================== Table DDL ====================

    @Override
    public void createTable(String schemaName, String tableName,
            List<TableColumnSpec> columns,
            Optional<List<PartitionFieldSpec>> partitionSpec,
            Optional<TableLocationSpec> location)
    {
        var tab = DUCKLAKE_TABLE.as("tab");
        var partinfo = DUCKLAKE_PARTITION_INFO.as("partinfo");
        var partcol = DUCKLAKE_PARTITION_COLUMN.as("partcol");
        String tablePath = location.map(TableLocationSpec::path).orElse(tableName + "/");
        boolean pathIsRelative = location.map(TableLocationSpec::isRelative).orElse(true);
        executeWriteTransaction("create table " + schemaName + "." + tableName, tx -> {
            long schemaId = tx.resolveSchemaId(schemaName);
            long tableId = tx.allocateCatalogId();
            DSLContext ctx = tx.dsl();

            // 1. Insert table row
            ctx.insertInto(tab)
                    .set(tab.TABLE_ID, tableId)
                    .set(tab.TABLE_UUID, UUID.fromString(newCatalogUuid()))
                    .set(tab.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                    .set(tab.SCHEMA_ID, schemaId)
                    .set(tab.TABLE_NAME, tableName)
                    .set(tab.PATH, tablePath)
                    .set(tab.PATH_IS_RELATIVE, pathIsRelative)
                    .execute();

            // 2. Insert column rows (flattening nested types with parent links)
            // column_order is 1-based per DuckDB convention
            Map<String, Long> topLevelColumnIds = new LinkedHashMap<>();
            long columnOrder = 1;
            for (TableColumnSpec column : columns) {
                long columnId = insertColumnTree(tx, tableId, column, columnOrder++, OptionalLong.empty());
                topLevelColumnIds.put(column.name(), columnId);
            }

            // 3. Table stats are NOT created at CREATE TABLE time — DuckDB creates them
            // only when data is first inserted. Creating them here with zeros causes
            // DuckDB's GetGlobalTableStats to crash (GetValueInternal on NULL).

            // 4. Insert partition spec if provided
            if (partitionSpec.isPresent() && !partitionSpec.get().isEmpty()) {
                long partitionId = tx.allocateCatalogId();

                ctx.insertInto(partinfo)
                        .set(partinfo.PARTITION_ID, partitionId)
                        .set(partinfo.TABLE_ID, tableId)
                        .set(partinfo.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                        .execute();

                long keyIndex = 0;
                for (PartitionFieldSpec field : partitionSpec.get()) {
                    Long columnId = topLevelColumnIds.get(field.columnName());
                    if (columnId == null) {
                        throw new RuntimeException("Partition column not found: " + field.columnName());
                    }
                    ctx.insertInto(partcol)
                            .set(partcol.PARTITION_ID, partitionId)
                            .set(partcol.TABLE_ID, tableId)
                            .set(partcol.PARTITION_KEY_INDEX, keyIndex++)
                            .set(partcol.COLUMN_ID, columnId)
                            .set(partcol.TRANSFORM, field.transform().toCatalogString(field.arity()))
                            .execute();
                }
            }

            tx.incrementSchemaVersion(tableId);
            tx.recordChange(new WriteChange.CreatedTable(schemaId, schemaName, tableName));
        });
    }

    /**
     * Recursively inserts a column and its children into ducklake_column.
     * Returns the column_id of the inserted column.
     */
    private long insertColumnTree(DucklakeWriteTransaction tx, long tableId,
            TableColumnSpec column, long columnOrder, OptionalLong parentColumnId)
    {
        long columnId = tx.allocateCatalogId();

        // `default_value = 'NULL'` (the four-char string, not SQL NULL) is upstream's "no
        // default" sentinel; `default_value_type = 'literal'` is mandatory per upstream's
        // migration (ducklake_metadata_manager.cpp backfills NULL → 'literal'). `initial_default`
        // and `default_value_dialect` are left unset (SQL NULL): the dialect is informational
        // and only meaningful when there's a real expression to interpret. If we ever wire up
        // user-written Trino DEFAULT expressions, set 'trino' here — the dialect names the SQL
        // syntax of the expression, which would be plain Trino SQL.
        var col = DUCKLAKE_COLUMN.as("col");
        tx.dsl().insertInto(col)
                .set(col.COLUMN_ID, columnId)
                .set(col.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                .set(col.TABLE_ID, tableId)
                .set(col.COLUMN_ORDER, columnOrder)
                .set(col.COLUMN_NAME, column.name())
                .set(col.COLUMN_TYPE, column.ducklakeType())
                .set(col.DEFAULT_VALUE, "NULL")
                .set(col.NULLS_ALLOWED, column.nullable())
                .set(col.PARENT_COLUMN, parentColumnId.isPresent() ? parentColumnId.getAsLong() : null)
                .set(col.DEFAULT_VALUE_TYPE, "literal")
                .execute();

        // Insert children with their own column_order (0-based within parent)
        long childOrder = 0;
        for (TableColumnSpec child : column.children()) {
            insertColumnTree(tx, tableId, child, childOrder++, OptionalLong.of(columnId));
        }

        return columnId;
    }

    @Override
    public void dropTable(String schemaName, String tableName)
    {
        var tab = DUCKLAKE_TABLE.as("tab");
        var col = DUCKLAKE_COLUMN.as("col");
        var file = DUCKLAKE_DATA_FILE.as("file");
        var delfile = DUCKLAKE_DELETE_FILE.as("delfile");
        var partinfo = DUCKLAKE_PARTITION_INFO.as("partinfo");
        executeWriteTransaction("drop table " + schemaName + "." + tableName, tx -> {
            long schemaId = tx.resolveSchemaId(schemaName);
            long tableId = tx.resolveTableId(schemaId, tableName);
            DSLContext ctx = tx.dsl();
            long newSnapshotId = tx.getNewSnapshotId();

            // End-snapshot the table row. Routed through `metadata` because the
            // Quack RPC binder rejects UPDATE on attached-metadata tables with
            // "Can only update base table". Pass-through on PG / local DuckDB.
            metadata.execute(ctx, ctx.update(tab)
                    .set(tab.END_SNAPSHOT, newSnapshotId)
                    .where(tab.TABLE_ID.eq(tableId))
                    .and(tab.END_SNAPSHOT.isNull()));

            // End-snapshot all active columns
            metadata.execute(ctx, ctx.update(col)
                    .set(col.END_SNAPSHOT, newSnapshotId)
                    .where(col.TABLE_ID.eq(tableId))
                    .and(col.END_SNAPSHOT.isNull()));

            // End-snapshot all active data files
            metadata.execute(ctx, ctx.update(file)
                    .set(file.END_SNAPSHOT, newSnapshotId)
                    .where(file.TABLE_ID.eq(tableId))
                    .and(file.END_SNAPSHOT.isNull()));

            // End-snapshot all active delete files (matched via data_file subquery)
            metadata.execute(ctx, ctx.update(delfile)
                    .set(delfile.END_SNAPSHOT, newSnapshotId)
                    .where(delfile.DATA_FILE_ID.in(
                            DSL.select(file.DATA_FILE_ID)
                                    .from(file)
                                    .where(file.TABLE_ID.eq(tableId))))
                    .and(delfile.END_SNAPSHOT.isNull()));

            // End-snapshot partition info
            metadata.execute(ctx, ctx.update(partinfo)
                    .set(partinfo.END_SNAPSHOT, newSnapshotId)
                    .where(partinfo.TABLE_ID.eq(tableId))
                    .and(partinfo.END_SNAPSHOT.isNull()));

            tx.incrementSchemaVersion(tableId);
            tx.recordChange(new WriteChange.DroppedTable(tableId));
        });
    }

    @Override
    public void addColumn(long tableId, TableColumnSpec column)
    {
        var col = DUCKLAKE_COLUMN.as("col");
        executeWriteTransaction("add column to table " + tableId, tx -> {
            // Find the current max column_order for top-level columns
            Long maxOrder = tx.dsl().select(DSL.max(col.COLUMN_ORDER))
                    .from(col)
                    .where(col.TABLE_ID.eq(tableId))
                    .and(col.PARENT_COLUMN.isNull())
                    .and(activeAt(col, tx.getCurrentSnapshotId()))
                    .fetchOne(0, Long.class);

            insertColumnTree(tx, tableId, column, orZero(maxOrder) + 1, OptionalLong.empty());
            tx.incrementSchemaVersion(tableId);
            tx.recordChange(new WriteChange.AlteredTable(tableId));
        });
    }

    @Override
    public void dropColumn(long tableId, long columnId)
    {
        var col = DUCKLAKE_COLUMN.as("col");
        executeWriteTransaction("drop column from table " + tableId, tx -> {
            // End-snapshot the column and all its children (for nested types)
            tx.dsl().update(col)
                    .set(col.END_SNAPSHOT, tx.getNewSnapshotId())
                    .where(col.TABLE_ID.eq(tableId))
                    .and(col.COLUMN_ID.eq(columnId)
                            .or(col.PARENT_COLUMN.eq(columnId)))
                    .and(col.END_SNAPSHOT.isNull())
                    .execute();

            tx.incrementSchemaVersion(tableId);
            tx.recordChange(new WriteChange.AlteredTable(tableId));
        });
    }

    @Override
    public void renameColumn(long tableId, long columnId, String newName)
    {
        var col = DUCKLAKE_COLUMN.as("col");
        executeWriteTransaction("rename column in table " + tableId, tx -> {
            DSLContext ctx = tx.dsl();

            // Read the current column metadata
            Record existing = ctx.select(col.COLUMN_ORDER, col.COLUMN_TYPE, col.NULLS_ALLOWED)
                    .from(col)
                    .where(col.TABLE_ID.eq(tableId))
                    .and(col.COLUMN_ID.eq(columnId))
                    .and(activeAt(col, tx.getCurrentSnapshotId()))
                    .fetchOne();
            if (existing == null) {
                throw new RuntimeException("Column not found: " + columnId);
            }
            long columnOrder = orZero(existing.get(col.COLUMN_ORDER));
            String columnType = existing.get(col.COLUMN_TYPE);
            boolean nullsAllowed = Boolean.TRUE.equals(existing.get(col.NULLS_ALLOWED));

            // End-snapshot the current version
            ctx.update(col)
                    .set(col.END_SNAPSHOT, tx.getNewSnapshotId())
                    .where(col.TABLE_ID.eq(tableId))
                    .and(col.COLUMN_ID.eq(columnId))
                    .and(col.END_SNAPSHOT.isNull())
                    .and(col.PARENT_COLUMN.isNull())
                    .execute();

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
                    .execute();

            tx.incrementSchemaVersion(tableId);
            tx.recordChange(new WriteChange.AlteredTable(tableId));
        });
    }

    @Override
    public void commitInsert(long tableId, List<DucklakeWriteFragment> fragments)
    {
        if (fragments.isEmpty()) {
            return;
        }

        executeWriteTransaction("insert into table " + tableId, tx -> {
            applyInsertFragments(tx, tableId, fragments);
            tx.recordChange(new WriteChange.InsertedIntoTable(tableId, referencedColumnIds(fragments)));
        });
    }

    @Override
    public void commitAddFiles(long tableId, List<DucklakeWriteFragment> fragments)
    {
        if (fragments.isEmpty()) {
            return;
        }

        executeWriteTransaction("add files to table " + tableId, tx -> {
            applyInsertFragments(tx, tableId, fragments);
            tx.recordChange(new WriteChange.InsertedIntoTable(tableId, referencedColumnIds(fragments)));
        });
    }

    private static Set<Long> referencedColumnIds(List<DucklakeWriteFragment> fragments)
    {
        return fragments.stream()
                .flatMap(f -> f.columnStats().stream())
                .map(DucklakeFileColumnStats::columnId)
                .collect(Collectors.toUnmodifiableSet());
    }

    private static Set<Long> referencedDataFileIds(List<DucklakeDeleteFragment> fragments)
    {
        return fragments.stream()
                .map(DucklakeDeleteFragment::dataFileId)
                .collect(Collectors.toUnmodifiableSet());
    }

    private void applyInsertFragments(DucklakeWriteTransaction tx, long tableId, List<DucklakeWriteFragment> fragments)
    {
        DSLContext ctx = tx.dsl();
        var tabstats = DUCKLAKE_TABLE_STATS.as("tabstats");
        var file = DUCKLAKE_DATA_FILE.as("file");
        var partval = DUCKLAKE_FILE_PARTITION_VALUE.as("partval");
        var colstats = DUCKLAKE_FILE_COLUMN_STATS.as("colstats");
        var tabcolst = DUCKLAKE_TABLE_COLUMN_STATS.as("tabcolst");

        // Read current table stats (may not exist yet — DuckDB creates them on first insert).
        // Note: ducklake_table_stats has no PK/UNIQUE on table_id, so Postgres `ON CONFLICT`
        // isn't usable here; we do an explicit existence probe + INSERT-or-UPDATE.
        DucklakeTableStatsRecord existingStats = ctx.selectFrom(tabstats)
                .where(tabstats.TABLE_ID.eq(tableId))
                .fetchOne();

        long runningRowId = existingStats == null ? 0L : orZero(existingStats.getNextRowId());
        long totalRecords = 0;
        long totalFileSize = 0;
        List<DucklakeFilePartitionValueRecord> partitionValueRecords = new ArrayList<>();
        List<DucklakeFileColumnStatsRecord> fileColumnStatsRecords = new ArrayList<>();
        List<DucklakeDataFileRecord> dataFileRecords = new ArrayList<>();
        // Dedupe identical NameMap structures within this call. Upstream's
        // ducklake_add_data_files writes one ducklake_column_mapping row per
        // unique parquet schema seen in a batch; matching that here lets a
        // glob over homogeneous parquet files share one mapping_id.
        Map<DucklakeNameMap, Long> nameMapToId = new HashMap<>();
        List<DucklakeColumnMappingRecord> columnMappingRecords = new ArrayList<>();
        List<DucklakeNameMappingRecord> nameMappingRecords = new ArrayList<>();

        for (DucklakeWriteFragment fragment : fragments) {
            long dataFileId = tx.allocateFileId();

            DucklakeDataFileRecord dataFile = ctx.newRecord(file);
            dataFile.setDataFileId(dataFileId);
            dataFile.setTableId(tableId);
            dataFile.setBeginSnapshot(tx.getNewSnapshotId());
            // file_order: NULL matches DuckDB convention
            dataFile.setPath(fragment.path());
            dataFile.setPathIsRelative(fragment.pathIsRelative());
            dataFile.setFileFormat(fragment.fileFormat());
            dataFile.setRecordCount(fragment.recordCount());
            dataFile.setFileSizeBytes(fragment.fileSizeBytes());
            // footer_size is a hint column: SQL NULL means "no hint" and the reader
            // falls back to a blind tail read. A literal 0 is wrong and crashes DuckDB's
            // reader ("Invalid footer length"). Callers that don't compute the size
            // (today: add_files) pass 0; map it to NULL here.
            if (fragment.footerSize() > 0) {
                dataFile.setFooterSize(fragment.footerSize());
            }
            dataFile.setRowIdStart(runningRowId);
            if (fragment.partitionId().isPresent()) {
                dataFile.setPartitionId(fragment.partitionId().getAsLong());
            }
            if (fragment.nameMap().isPresent()) {
                DucklakeNameMap nameMap = fragment.nameMap().get();
                Long mappingId = nameMapToId.get(nameMap);
                if (mappingId == null) {
                    mappingId = tx.allocateCatalogId();
                    nameMapToId.put(nameMap, mappingId);
                    DucklakeColumnMappingRecord cm = new DucklakeColumnMappingRecord();
                    cm.setMappingId(mappingId);
                    cm.setTableId(tableId);
                    cm.setType("map_by_name");
                    columnMappingRecords.add(cm);
                    addNameMappingRows(nameMappingRecords, mappingId, nameMap.entries(), null);
                }
                dataFile.setMappingId(mappingId);
            }
            dataFileRecords.add(dataFile);

            for (Map.Entry<Integer, String> partitionValue : fragment.partitionValues().entrySet()) {
                DucklakeFilePartitionValueRecord r = ctx.newRecord(partval);
                r.setTableId(tableId);
                r.setDataFileId(dataFileId);
                r.setPartitionKeyIndex((long) partitionValue.getKey());
                r.setPartitionValue(partitionValue.getValue());
                partitionValueRecords.add(r);
            }

            for (DucklakeFileColumnStats columnStats : fragment.columnStats()) {
                DucklakeFileColumnStatsRecord r = ctx.newRecord(colstats);
                r.setDataFileId(dataFileId);
                r.setTableId(tableId);
                r.setColumnId(columnStats.columnId());
                r.setColumnSizeBytes(columnStats.columnSizeBytes());
                r.setValueCount(columnStats.valueCount());
                r.setNullCount(columnStats.nullCount());
                r.setMinValue(columnStats.minValue().orElse(null));
                r.setMaxValue(columnStats.maxValue().orElse(null));
                // contains_nan: TRUE when set, SQL NULL otherwise (upstream convention).
                r.setContainsNan(columnStats.containsNan() ? Boolean.TRUE : null);
                fileColumnStatsRecords.add(r);
            }

            runningRowId += fragment.recordCount();
            totalRecords += fragment.recordCount();
            totalFileSize += fragment.fileSizeBytes();
        }

        // Name-map rows must land before the data_file rows that reference them via
        // mapping_id (no FK in upstream's schema, but kept in order for readability
        // and to make crash recovery deterministic).
        if (!columnMappingRecords.isEmpty()) {
            ctx.batchInsert(columnMappingRecords).execute();
        }
        if (!nameMappingRecords.isEmpty()) {
            ctx.batchInsert(nameMappingRecords).execute();
        }
        if (!dataFileRecords.isEmpty()) {
            ctx.batchInsert(dataFileRecords).execute();
        }
        if (!partitionValueRecords.isEmpty()) {
            ctx.batchInsert(partitionValueRecords).execute();
        }
        if (!fileColumnStatsRecords.isEmpty()) {
            ctx.batchInsert(fileColumnStatsRecords).execute();
        }

        // Insert or update ducklake_table_stats (no PK/UNIQUE → can't use ON CONFLICT)
        if (existingStats != null) {
            ctx.update(tabstats)
                    .set(tabstats.RECORD_COUNT, orZero(existingStats.getRecordCount()) + totalRecords)
                    .set(tabstats.NEXT_ROW_ID, orZero(existingStats.getNextRowId()) + totalRecords)
                    .set(tabstats.FILE_SIZE_BYTES, orZero(existingStats.getFileSizeBytes()) + totalFileSize)
                    .where(tabstats.TABLE_ID.eq(tableId))
                    .execute();
        }
        else {
            ctx.insertInto(tabstats)
                    .set(tabstats.TABLE_ID, tableId)
                    .set(tabstats.RECORD_COUNT, totalRecords)
                    .set(tabstats.NEXT_ROW_ID, totalRecords)
                    .set(tabstats.FILE_SIZE_BYTES, totalFileSize)
                    .execute();
        }

        // Upsert ducklake_table_column_stats: aggregate min/max/null across all fragments per column
        Map<Long, AggregatedColumnStats> columnAggregates = new LinkedHashMap<>();
        for (DucklakeWriteFragment fragment : fragments) {
            for (DucklakeFileColumnStats colStats : fragment.columnStats()) {
                columnAggregates.computeIfAbsent(colStats.columnId(), id -> new AggregatedColumnStats())
                        .merge(colStats);
            }
        }

        Set<Long> existingColumnStats = loadExistingColumnStatsColumnIds(tx, tableId, columnAggregates.keySet());
        List<DucklakeTableColumnStatsRecord> insertRecords = new ArrayList<>();
        for (Map.Entry<Long, AggregatedColumnStats> entry : columnAggregates.entrySet()) {
            long columnId = entry.getKey();
            AggregatedColumnStats agg = entry.getValue();

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
                Param<String> minParam = DSL.val(agg.minValue, tabcolst.MIN_VALUE.getDataType());
                Param<String> maxParam = DSL.val(agg.maxValue, tabcolst.MAX_VALUE.getDataType());
                var upd = ctx.update(tabcolst)
                        .set(tabcolst.MIN_VALUE,
                                DSL.when(tabcolst.MIN_VALUE.isNull(), minParam)
                                        .when(minParam.isNull(), tabcolst.MIN_VALUE)
                                        .when(minParam.lt(tabcolst.MIN_VALUE), minParam)
                                        .otherwise(tabcolst.MIN_VALUE))
                        .set(tabcolst.MAX_VALUE,
                                DSL.when(tabcolst.MAX_VALUE.isNull(), maxParam)
                                        .when(maxParam.isNull(), tabcolst.MAX_VALUE)
                                        .when(maxParam.gt(tabcolst.MAX_VALUE), maxParam)
                                        .otherwise(tabcolst.MAX_VALUE));
                if (agg.containsNull) {
                    upd = upd.set(tabcolst.CONTAINS_NULL, true);
                }
                if (agg.containsNan) {
                    upd = upd.set(tabcolst.CONTAINS_NAN, true);
                }
                upd.where(tabcolst.TABLE_ID.eq(tableId))
                        .and(tabcolst.COLUMN_ID.eq(columnId))
                        .execute();
            }
            else {
                DucklakeTableColumnStatsRecord r = ctx.newRecord(tabcolst);
                r.setTableId(tableId);
                r.setColumnId(columnId);
                r.setContainsNull(agg.containsNull);
                // Asymmetric with contains_null: mirror the original INSERT which wrote
                // SQL NULL for contains_nan when false (and TRUE when true).
                r.setContainsNan(agg.containsNan ? Boolean.TRUE : null);
                r.setMinValue(agg.minValue);
                r.setMaxValue(agg.maxValue);
                insertRecords.add(r);
            }
        }
        if (!insertRecords.isEmpty()) {
            ctx.batchInsert(insertRecords).execute();
        }
    }

    /**
     * Walks a {@link DucklakeNameMap} entry tree and emits one
     * {@code ducklake_name_mapping} row per node. Children point at their parent's
     * column_id (allocated here) so a reader can reconstruct the tree from the flat
     * rows. Mirrors upstream's {@code DuckLakeNameMapEntry} → row flattening in
     * {@code DuckLakeTransaction::AppendFiles} (via {@code WriteNameMap}).
     *
     * <p>{@code column_id} is per-{@code mapping_id} (not global), so each top-level
     * invocation starts a fresh {@code long[1]} counter.
     */
    private void addNameMappingRows(
            List<DucklakeNameMappingRecord> sink,
            long mappingId,
            List<DucklakeNameMapEntry> entries,
            Long parentColumnId)
    {
        appendNameMappingRows(sink, mappingId, entries, parentColumnId, new long[]{1L});
    }

    private void appendNameMappingRows(
            List<DucklakeNameMappingRecord> sink,
            long mappingId,
            List<DucklakeNameMapEntry> entries,
            Long parentColumnId,
            long[] nextColumnId)
    {
        for (DucklakeNameMapEntry entry : entries) {
            long columnId = nextColumnId[0]++;
            DucklakeNameMappingRecord r = new DucklakeNameMappingRecord();
            r.setMappingId(mappingId);
            r.setColumnId(columnId);
            r.setSourceName(entry.sourceName());
            r.setTargetFieldId(entry.targetFieldId());
            r.setParentColumn(parentColumnId);
            // DuckDB's GetColumnMappings reader crashes on SQL NULL for is_partition —
            // the column's DDL has DEFAULT false, and upstream always writes a literal bool.
            // Mirror that contract: TRUE for hive-partition entries, FALSE for regular ones.
            r.setIsPartition(entry.isPartition());
            sink.add(r);
            if (!entry.children().isEmpty()) {
                appendNameMappingRows(sink, mappingId, entry.children(), columnId, nextColumnId);
            }
        }
    }

    private static Set<Long> loadExistingColumnStatsColumnIds(DucklakeWriteTransaction tx, long tableId, Set<Long> candidateColumnIds)
    {
        if (candidateColumnIds.isEmpty()) {
            return Set.of();
        }
        var tabcolst = DUCKLAKE_TABLE_COLUMN_STATS.as("tabcolst");
        return tx.dsl().select(tabcolst.COLUMN_ID)
                .from(tabcolst)
                .where(tabcolst.TABLE_ID.eq(tableId))
                .and(tabcolst.COLUMN_ID.in(candidateColumnIds))
                .fetchSet(tabcolst.COLUMN_ID);
    }

    @Override
    public void commitDelete(long tableId, List<DucklakeDeleteFragment> deleteFragments)
    {
        if (deleteFragments.isEmpty()) {
            return;
        }

        executeWriteTransaction("delete from table " + tableId, tx -> {
            applyDeleteFragments(tx, tableId, deleteFragments);
            tx.recordChange(new WriteChange.DeletedFromTable(tableId, referencedDataFileIds(deleteFragments)));
        });
    }

    @Override
    public void commitMerge(long tableId,
            List<DucklakeDeleteFragment> deleteFragments, List<DucklakeWriteFragment> insertFragments)
    {
        if (deleteFragments.isEmpty() && insertFragments.isEmpty()) {
            return;
        }

        executeWriteTransaction("merge into table " + tableId, tx -> {
            if (!deleteFragments.isEmpty()) {
                applyDeleteFragments(tx, tableId, deleteFragments);
                tx.recordChange(new WriteChange.DeletedFromTable(tableId, referencedDataFileIds(deleteFragments)));
            }
            if (!insertFragments.isEmpty()) {
                applyInsertFragments(tx, tableId, insertFragments);
                tx.recordChange(new WriteChange.InsertedIntoTable(tableId, referencedColumnIds(insertFragments)));
            }
        });
    }

    private void applyDeleteFragments(DucklakeWriteTransaction tx, long tableId, List<DucklakeDeleteFragment> deleteFragments)
    {
        DSLContext ctx = tx.dsl();
        var delfile = DUCKLAKE_DELETE_FILE.as("delfile");
        var tabstats = DUCKLAKE_TABLE_STATS.as("tabstats");
        long totalDeleteCount = 0;
        List<DucklakeDeleteFileRecord> deleteFileRecords = new ArrayList<>();

        for (DucklakeDeleteFragment fragment : deleteFragments) {
            long deleteFileId = tx.allocateFileId();

            DucklakeDeleteFileRecord r = ctx.newRecord(delfile);
            r.setDeleteFileId(deleteFileId);
            r.setTableId(tableId);
            r.setBeginSnapshot(tx.getNewSnapshotId());
            r.setDataFileId(fragment.dataFileId());
            r.setPath(fragment.path());
            r.setPathIsRelative(true);
            r.setFormat("parquet");
            r.setDeleteCount(fragment.deleteCount());
            r.setFileSizeBytes(fragment.fileSizeBytes());
            r.setFooterSize(fragment.footerSize());
            deleteFileRecords.add(r);

            totalDeleteCount += fragment.deleteCount();
        }

        if (!deleteFileRecords.isEmpty()) {
            ctx.batchInsert(deleteFileRecords).execute();
        }

        // Update table stats: decrement record count with GREATEST(0, record_count - ?)
        ctx.update(tabstats)
                .set(tabstats.RECORD_COUNT,
                        DSL.greatest(DSL.inline(0L), tabstats.RECORD_COUNT.minus(totalDeleteCount)))
                .where(tabstats.TABLE_ID.eq(tableId))
                .execute();
    }

    private static class AggregatedColumnStats
    {
        boolean containsNull;
        boolean containsNan;
        String minValue;
        String maxValue;

        void merge(DucklakeFileColumnStats stats)
        {
            if (stats.nullCount() > 0) {
                containsNull = true;
            }
            if (stats.containsNan()) {
                containsNan = true;
            }
            if (stats.minValue().isPresent()) {
                String val = stats.minValue().get();
                if (minValue == null || val.compareTo(minValue) < 0) {
                    minValue = val;
                }
            }
            if (stats.maxValue().isPresent()) {
                String val = stats.maxValue().get();
                if (maxValue == null || val.compareTo(maxValue) > 0) {
                    maxValue = val;
                }
            }
        }
    }

    // DuckLake's jOOQ codegen marks most BIGINT columns as nullable (no schema-level NOT NULL
    // constraint), so accessor methods return Long. The JDBC implementation historically used
    // ResultSet.getLong() which returns 0 on SQL NULL; preserve that fallback to keep fidelity
    // with records that don't populate optional columns (e.g. file_order).
    private static long orZero(Long value)
    {
        return value == null ? 0L : value;
    }

    private static DucklakeColumn toDucklakeColumn(DucklakeColumnRecord r)
    {
        return new DucklakeColumn(
                orZero(r.getColumnId()),
                orZero(r.getBeginSnapshot()),
                Optional.ofNullable(r.getEndSnapshot()),
                orZero(r.getTableId()),
                orZero(r.getColumnOrder()),
                r.getColumnName(),
                r.getColumnType(),
                Boolean.TRUE.equals(r.getNullsAllowed()),
                Optional.ofNullable(r.getParentColumn()));
    }

    private static DucklakeSnapshot toDucklakeSnapshot(DucklakeSnapshotRecord r)
    {
        return new DucklakeSnapshot(
                r.getSnapshotId(),
                r.getSnapshotTime().toInstant(),
                orZero(r.getSchemaVersion()),
                orZero(r.getNextCatalogId()),
                orZero(r.getNextFileId()));
    }

    private static DucklakeSnapshotChange toDucklakeSnapshotChange(DucklakeSnapshotChangesRecord r)
    {
        return new DucklakeSnapshotChange(
                r.getSnapshotId(),
                Optional.ofNullable(r.getChangesMade()),
                Optional.ofNullable(r.getAuthor()),
                Optional.ofNullable(r.getCommitMessage()),
                Optional.ofNullable(r.getCommitExtraInfo()));
    }

    private static DucklakeSchema toDucklakeSchema(DucklakeSchemaRecord r)
    {
        return new DucklakeSchema(
                r.getSchemaId(),
                r.getSchemaUuid(),
                orZero(r.getBeginSnapshot()),
                Optional.ofNullable(r.getEndSnapshot()),
                r.getSchemaName(),
                Optional.ofNullable(r.getPath()),
                Optional.ofNullable(r.getPathIsRelative()));
    }

    private static DucklakeTable toDucklakeTable(DucklakeTableRecord r)
    {
        return new DucklakeTable(
                orZero(r.getTableId()),
                r.getTableUuid(),
                orZero(r.getBeginSnapshot()),
                Optional.ofNullable(r.getEndSnapshot()),
                orZero(r.getSchemaId()),
                r.getTableName(),
                Optional.ofNullable(r.getPath()),
                Optional.ofNullable(r.getPathIsRelative()));
    }

    private static DucklakeTableStats toDucklakeTableStats(DucklakeTableStatsRecord r)
    {
        return new DucklakeTableStats(
                orZero(r.getTableId()),
                orZero(r.getRecordCount()),
                orZero(r.getFileSizeBytes()));
    }

    private static DucklakeView toDucklakeView(DucklakeViewRecord r)
    {
        Long endRaw = r.getEndSnapshot();
        OptionalLong endSnapshot = endRaw == null ? OptionalLong.empty() : OptionalLong.of(endRaw);
        UUID viewUuid = r.getViewUuid();
        return new DucklakeView(
                orZero(r.getViewId()),
                viewUuid == null ? null : viewUuid.toString(),
                orZero(r.getSchemaId()),
                r.getViewName(),
                r.getSql(),
                r.getDialect(),
                Optional.ofNullable(r.getColumnAliases()),
                orZero(r.getBeginSnapshot()),
                endSnapshot);
    }

    @Override
    public void close()
    {
        if (hikariDataSource != null) {
            hikariDataSource.close();
        }
    }

    private String resolveColumnType(DucklakeColumn column, Map<Long, List<DucklakeColumn>> childrenByParent)
    {
        String columnType = column.columnType();
        switch (columnType.toLowerCase()) {
            case "list": {
                List<DucklakeColumn> children = childrenByParent.getOrDefault(column.columnId(), List.of());
                if (children.size() != 1) {
                    throw new IllegalStateException("List column must have exactly one child column: " + column.columnName());
                }
                return "list<" + resolveColumnType(children.get(0), childrenByParent) + ">";
            }
            case "struct": {
                List<DucklakeColumn> children = childrenByParent.getOrDefault(column.columnId(), List.of());
                String fields = children.stream()
                        .map(child -> child.columnName() + ":" + resolveColumnType(child, childrenByParent))
                        .collect(Collectors.joining(","));
                return "struct<" + fields + ">";
            }
            case "map": {
                List<DucklakeColumn> children = childrenByParent.getOrDefault(column.columnId(), List.of());
                if (children.size() != 2) {
                    throw new IllegalStateException("Map column must have exactly two child columns: " + column.columnName());
                }
                return "map<" + resolveColumnType(children.get(0), childrenByParent) + "," + resolveColumnType(children.get(1), childrenByParent) + ">";
            }
            default:
                return columnType;
        }
    }

    private Comparable<?> parseStatValue(String columnType, String value)
    {
        if (value == null) {
            return null;
        }

        String normalizedType = columnType.toLowerCase();
        try {
            if (isNumericType(normalizedType)) {
                return new java.math.BigDecimal(value);
            }
            if (normalizedType.equals("boolean")) {
                return parseBoolean(value);
            }
            return value;
        }
        catch (RuntimeException e) {
            // If parsing fails we avoid false negatives by not pruning on this value.
            return null;
        }
    }

    private boolean isNumericType(String type)
    {
        return type.equals("int8")
                || type.equals("int16")
                || type.equals("int32")
                || type.equals("int64")
                || type.equals("uint8")
                || type.equals("uint16")
                || type.equals("uint32")
                || type.equals("uint64")
                || type.equals("float32")
                || type.equals("float64")
                || type.startsWith("decimal(");
    }

    private Boolean parseBoolean(String value)
    {
        if (value.equalsIgnoreCase("true") || value.equals("1")) {
            return true;
        }
        if (value.equalsIgnoreCase("false") || value.equals("0")) {
            return false;
        }
        throw new IllegalArgumentException("Invalid boolean value: " + value);
    }

    private boolean isWithinBounds(
            Comparable<?> lowerBound,
            Comparable<?> upperBound,
            Comparable<?> minStat,
            Comparable<?> maxStat)
    {
        OptionalInt lowerVsMax = compareValues(lowerBound, maxStat);
        if (lowerVsMax.isPresent() && lowerVsMax.getAsInt() > 0) {
            return false;
        }

        OptionalInt upperVsMin = compareValues(upperBound, minStat);
        return upperVsMin.isEmpty() || upperVsMin.getAsInt() >= 0;
    }

    @SuppressWarnings("unchecked")
    private OptionalInt compareValues(Comparable<?> left, Comparable<?> right)
    {
        if (left == null || right == null) {
            return OptionalInt.empty();
        }
        try {
            return OptionalInt.of(((Comparable<Object>) left).compareTo(right));
        }
        catch (RuntimeException e) {
            // Type mismatch or non-comparable values: avoid pruning to prevent false negatives.
            return OptionalInt.empty();
        }
    }
}
