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
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeColumnRecord;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeDataFileRecord;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeDeleteFileRecord;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeFileColumnStatsRecord;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeFilePartitionValueRecord;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedEpochGenerator;

import static dev.brikk.ducklake.catalog.SnapshotRange.activeAt;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_COLUMN;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DATA_FILE;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_DELETE_FILE;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_FILE_COLUMN_STATS;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_FILE_PARTITION_VALUE;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_INLINED_DATA_TABLES;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_METADATA;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_PARTITION_COLUMN;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_PARTITION_INFO;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SCHEMA;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SCHEMA_VERSIONS;
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

    public JdbcDucklakeCatalog(DucklakeCatalogConfig config)
    {
        requireNonNull(config, "config is null");

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getCatalogDatabaseUrl());
        if (config.getCatalogDatabaseUser() != null) {
            hikariConfig.setUsername(config.getCatalogDatabaseUser());
        }
        if (config.getCatalogDatabasePassword() != null) {
            hikariConfig.setPassword(config.getCatalogDatabasePassword());
        }
        hikariConfig.setMaximumPoolSize(config.getMaxCatalogConnections());
        hikariConfig.setMinimumIdle(1);
        hikariConfig.setConnectionTimeout(30000);

        this.hikariDataSource = new HikariDataSource(hikariConfig);
        this.dataSource = hikariDataSource;

        // Infer dialect from the JDBC URL. JDBCUtils.dialect() returns SQLDialect.DEFAULT for
        // backends jOOQ OSS doesn't recognize, which keeps query rendering portable.
        this.dialect = JDBCUtils.dialect(config.getCatalogDatabaseUrl());
        this.jooqSettings = new Settings()
                // The generated DuckLake tables use lowercase unquoted identifiers. Quoting is
                // only needed on identifiers that collide with reserved words (none in the
                // ducklake_* schema today) — leaving it off keeps queries readable in logs.
                .withRenderQuotedNames(RenderQuotedNames.EXPLICIT_DEFAULT_UNQUOTED);
        this.dsl = DSL.using(dataSource, dialect, jooqSettings);

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
        return dsl.select(
                        file.DATA_FILE_ID,
                        file.TABLE_ID,
                        file.BEGIN_SNAPSHOT,
                        file.END_SNAPSHOT,
                        file.FILE_ORDER,
                        file.PATH,
                        file.PATH_IS_RELATIVE,
                        file.FILE_FORMAT,
                        file.RECORD_COUNT,
                        file.FILE_SIZE_BYTES,
                        file.FOOTER_SIZE,
                        file.ROW_ID_START,
                        file.PARTITION_ID,
                        delfile.PATH,
                        delfile.PATH_IS_RELATIVE,
                        delfile.FOOTER_SIZE,
                        delfile.FORMAT)
                .from(file)
                .leftJoin(delfile)
                .on(file.DATA_FILE_ID.eq(delfile.DATA_FILE_ID))
                .and(activeAt(delfile, snapshotId))
                .where(file.TABLE_ID.eq(tableId))
                .and(activeAt(file, snapshotId))
                .orderBy(file.FILE_ORDER)
                .fetch(r -> new DucklakeDataFile(
                        orZero(r.get(file.DATA_FILE_ID)),
                        orZero(r.get(file.TABLE_ID)),
                        orZero(r.get(file.BEGIN_SNAPSHOT)),
                        Optional.ofNullable(r.get(file.END_SNAPSHOT)),
                        orZero(r.get(file.FILE_ORDER)),
                        r.get(file.PATH),
                        Boolean.TRUE.equals(r.get(file.PATH_IS_RELATIVE)),
                        r.get(file.FILE_FORMAT),
                        orZero(r.get(file.RECORD_COUNT)),
                        orZero(r.get(file.FILE_SIZE_BYTES)),
                        orZero(r.get(file.FOOTER_SIZE)),
                        orZero(r.get(file.ROW_ID_START)),
                        Optional.ofNullable(r.get(file.PARTITION_ID)),
                        Optional.ofNullable(r.get(delfile.PATH)),
                        Optional.ofNullable(r.get(delfile.PATH_IS_RELATIVE)),
                        Optional.ofNullable(r.get(delfile.FOOTER_SIZE)),
                        Optional.ofNullable(r.get(delfile.FORMAT))));
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
        return dsl.select(
                        colstats.DATA_FILE_ID,
                        colstats.MIN_VALUE,
                        colstats.MAX_VALUE)
                .from(colstats)
                .innerJoin(file)
                .on(colstats.DATA_FILE_ID.eq(file.DATA_FILE_ID))
                .where(colstats.TABLE_ID.eq(tableId))
                .and(colstats.COLUMN_ID.eq(columnId))
                .and(file.TABLE_ID.eq(tableId))
                .and(activeAt(file, snapshotId))
                .fetch()
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
                .and(activeAt(file, snapshotId))
                .forEach(r -> {
                    long columnId = orZero(r.get(colstats.COLUMN_ID));
                    long[] counts = countAccumulators.computeIfAbsent(columnId, _ -> new long[3]);
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
        catch (RuntimeException _) {
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
                .orderBy(partinfo.PARTITION_ID, partcol.PARTITION_KEY_INDEX)
                .forEach(r -> {
                    long partitionId = orZero(r.get(partinfo.PARTITION_ID));
                    tableIdByPartition.put(partitionId, orZero(r.get(partinfo.TABLE_ID)));
                    fieldsByPartition.computeIfAbsent(partitionId, _ -> new ArrayList<>())
                            .add(new DucklakePartitionField(
                                    (int) orZero(r.get(partcol.PARTITION_KEY_INDEX)),
                                    orZero(r.get(partcol.COLUMN_ID)),
                                    DucklakePartitionTransform.fromString(r.get(partcol.TRANSFORM))));
                });

        List<DucklakePartitionSpec> specs = new ArrayList<>();
        for (Map.Entry<Long, List<DucklakePartitionField>> entry : fieldsByPartition.entrySet()) {
            specs.add(new DucklakePartitionSpec(entry.getKey(), tableIdByPartition.get(entry.getKey()), entry.getValue()));
        }
        return specs;
    }

    @Override
    public Map<Long, List<DucklakeFilePartitionValue>> getFilePartitionValues(long tableId, long snapshotId)
    {
        Map<Long, List<DucklakeFilePartitionValue>> result = new HashMap<>();

        var partval = DUCKLAKE_FILE_PARTITION_VALUE.as("partval");
        var file = DUCKLAKE_DATA_FILE.as("file");
        dsl.select(
                        partval.DATA_FILE_ID,
                        partval.PARTITION_KEY_INDEX,
                        partval.PARTITION_VALUE)
                .from(partval)
                .innerJoin(file)
                .on(partval.DATA_FILE_ID.eq(file.DATA_FILE_ID))
                .and(partval.TABLE_ID.eq(file.TABLE_ID))
                .where(partval.TABLE_ID.eq(tableId))
                .and(activeAt(file, snapshotId))
                .forEach(r -> {
                    long dataFileId = orZero(r.get(partval.DATA_FILE_ID));
                    result.computeIfAbsent(dataFileId, _ -> new ArrayList<>())
                            .add(new DucklakeFilePartitionValue(
                                    dataFileId,
                                    (int) orZero(r.get(partval.PARTITION_KEY_INDEX)),
                                    r.get(partval.PARTITION_VALUE)));
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

    // Matches upstream's KeywordHelper::WriteQuoted (DuckDB) as consumed by
    // DuckLakeUtil::ParseQuotedValue in third_party/ducklake/src/common/ducklake_util.cpp:
    // wrap in double quotes and escape embedded " by doubling.
    private static String writeQuotedValue(String value)
    {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    // Upstream's DuckLakeUtil::ParseCatalogEntry expects a fully-qualified "schema"."name" form,
    // so `created_table` / `created_view` values must carry both parts.
    static String changeCreatedTable(String schemaName, String tableName)
    {
        return "created_table:" + writeQuotedValue(schemaName) + "." + writeQuotedValue(tableName);
    }

    static String changeCreatedView(String schemaName, String viewName)
    {
        return "created_view:" + writeQuotedValue(schemaName) + "." + writeQuotedValue(viewName);
    }

    // `created_schema` is a single quoted value — see ducklake_transaction_changes.cpp:
    // `ParseQuotedValue(entry.change_value, pos)`.
    static String changeCreatedSchema(String schemaName)
    {
        return "created_schema:" + writeQuotedValue(schemaName);
    }

    // Serializes the list of change entries into the DuckLake `ducklake_snapshot_changes.changes_made`
    // text column. Per upstream `ParseChangesList` (ducklake_transaction_changes.cpp) it's a
    // comma-separated list of `<kind>:<value>` entries; `ParseChangeValue` tracks balanced double
    // quotes so commas inside quoted values are literal. Entries are passed through verbatim —
    // callers that carry user-supplied names (schema / table / view) must quote via
    // {@link #changeCreatedTable} / {@link #changeCreatedView} / {@link #changeCreatedSchema}.
    // Numeric-id entries (`dropped_*`, `altered_*`, `inserted_into_table`, `deleted_from_table`)
    // are unquoted per spec and can be joined with simple string concatenation at the call site.
    static String formatChangesMade(List<String> changes)
    {
        return String.join(",", changes);
    }

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
        WriteTransactionRetry.retryOnConflict(
                MAX_RETRY_COUNT,
                INITIAL_RETRY_WAIT_MS,
                RETRY_BACKOFF_MULTIPLIER,
                Thread::sleep,
                operationDescription,
                () -> attemptWriteTransaction(operationDescription, action));
    }

    private void attemptWriteTransaction(String operationDescription, WriteTransactionAction action)
    {
        var snap = DUCKLAKE_SNAPSHOT.as("snap");
        var schver = DUCKLAKE_SCHEMA_VERSIONS.as("schver");
        var snapchg = DUCKLAKE_SNAPSHOT_CHANGES.as("snapchg");
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            DSLContext txDsl = forConnection(conn);
            long baseSnapshotId = -1;
            try {
                // 1. Read current snapshot state
                DucklakeSnapshotRecord snapshotRow = txDsl.selectFrom(snap)
                        .where(snap.SNAPSHOT_ID.eq(
                                DSL.select(DSL.max(snap.SNAPSHOT_ID)).from(snap)))
                        .fetchOne();
                if (snapshotRow == null) {
                    throw new IllegalStateException("No snapshots found");
                }
                long currentSnapshotId = snapshotRow.getSnapshotId();
                baseSnapshotId = currentSnapshotId;
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
                            .set(snapchg.CHANGES_MADE, formatChangesMade(tx.getChanges()))
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
            tx.addChange(changeCreatedView(schemaName, viewName));

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
            tx.addChange("dropped_view:" + view.viewId());
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
                    tx.addChange("altered_view:" + sourceView.viewId());
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
            tx.addChange("altered_view:" + view.viewId());
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
            tx.addChange(changeCreatedSchema(schemaName));

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

            tx.addChange("dropped_schema:" + schemaId);

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
            Optional<List<PartitionFieldSpec>> partitionSpec)
    {
        var tab = DUCKLAKE_TABLE.as("tab");
        var partinfo = DUCKLAKE_PARTITION_INFO.as("partinfo");
        var partcol = DUCKLAKE_PARTITION_COLUMN.as("partcol");
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
                    .set(tab.PATH, tableName + "/")
                    .set(tab.PATH_IS_RELATIVE, true)
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
                            .set(partcol.TRANSFORM, field.transform().name().toLowerCase(ENGLISH))
                            .execute();
                }
            }

            tx.incrementSchemaVersion(tableId);
            tx.addChange(changeCreatedTable(schemaName, tableName));
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

            // End-snapshot the table row
            ctx.update(tab)
                    .set(tab.END_SNAPSHOT, newSnapshotId)
                    .where(tab.TABLE_ID.eq(tableId))
                    .and(tab.END_SNAPSHOT.isNull())
                    .execute();

            // End-snapshot all active columns
            ctx.update(col)
                    .set(col.END_SNAPSHOT, newSnapshotId)
                    .where(col.TABLE_ID.eq(tableId))
                    .and(col.END_SNAPSHOT.isNull())
                    .execute();

            // End-snapshot all active data files
            ctx.update(file)
                    .set(file.END_SNAPSHOT, newSnapshotId)
                    .where(file.TABLE_ID.eq(tableId))
                    .and(file.END_SNAPSHOT.isNull())
                    .execute();

            // End-snapshot all active delete files (matched via data_file subquery)
            ctx.update(delfile)
                    .set(delfile.END_SNAPSHOT, newSnapshotId)
                    .where(delfile.DATA_FILE_ID.in(
                            DSL.select(file.DATA_FILE_ID)
                                    .from(file)
                                    .where(file.TABLE_ID.eq(tableId))))
                    .and(delfile.END_SNAPSHOT.isNull())
                    .execute();

            // End-snapshot partition info
            ctx.update(partinfo)
                    .set(partinfo.END_SNAPSHOT, newSnapshotId)
                    .where(partinfo.TABLE_ID.eq(tableId))
                    .and(partinfo.END_SNAPSHOT.isNull())
                    .execute();

            tx.incrementSchemaVersion(tableId);
            tx.addChange("dropped_table:" + tableId);
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
            tx.addChange("altered_table:" + tableId);
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
            tx.addChange("altered_table:" + tableId);
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
            tx.addChange("altered_table:" + tableId);
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
            tx.addChange("inserted_into_table:" + tableId);
        });
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

        for (DucklakeWriteFragment fragment : fragments) {
            long dataFileId = tx.allocateFileId();

            DucklakeDataFileRecord dataFile = ctx.newRecord(file);
            dataFile.setDataFileId(dataFileId);
            dataFile.setTableId(tableId);
            dataFile.setBeginSnapshot(tx.getNewSnapshotId());
            // file_order: NULL matches DuckDB convention
            dataFile.setPath(fragment.path());
            dataFile.setPathIsRelative(true);
            dataFile.setFileFormat(fragment.fileFormat());
            dataFile.setRecordCount(fragment.recordCount());
            dataFile.setFileSizeBytes(fragment.fileSizeBytes());
            dataFile.setFooterSize(fragment.footerSize());
            dataFile.setRowIdStart(runningRowId);
            if (fragment.partitionId().isPresent()) {
                dataFile.setPartitionId(fragment.partitionId().getAsLong());
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
            tx.addChange("deleted_from_table:" + tableId);
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
                tx.addChange("deleted_from_table:" + tableId);
            }
            if (!insertFragments.isEmpty()) {
                applyInsertFragments(tx, tableId, insertFragments);
                tx.addChange("inserted_into_table:" + tableId);
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
