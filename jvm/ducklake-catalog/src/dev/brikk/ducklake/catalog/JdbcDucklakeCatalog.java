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
import dev.brikk.ducklake.catalog.schema.tables.DucklakeDeleteFileTable;
import dev.brikk.ducklake.catalog.schema.tables.records.DucklakeColumnRecord;
import org.jooq.Condition;
import org.jooq.Field;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
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

    // Matches the upstream DuckDB ducklake extension, which uses UUIDv7 for catalog-identity
    // UUIDs (schema_uuid, table_uuid, view_uuid). v7 embeds a millisecond timestamp in the high
    // bits, giving better B-tree locality on the catalog's PK indexes than v4.
    private static final TimeBasedEpochGenerator V7_UUIDS = Generators.timeBasedEpochGenerator();

    // Allocates a fresh catalog-identity UUID (schema_uuid / table_uuid / view_uuid). Always
    // UUIDv7. Exposed package-private so tests can pin the version without reflection.
    static String newCatalogUuid()
    {
        return V7_UUIDS.generate().toString();
    }

    private final DataSource dataSource;
    private final HikariDataSource hikariDataSource;
    private final boolean isPostgresql;
    private final SQLDialect dialect;
    private final Settings jooqSettings;
    private final DSLContext dsl;

    public JdbcDucklakeCatalog(DucklakeCatalogConfig config)
    {
        requireNonNull(config, "config is null");

        this.isPostgresql = config.getCatalogDatabaseUrl().startsWith("jdbc:postgresql:");

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
        Long maxId = dsl.select(DSL.max(DUCKLAKE_SNAPSHOT.SNAPSHOT_ID))
                .from(DUCKLAKE_SNAPSHOT)
                .fetchOne(0, Long.class);
        if (maxId == null) {
            throw new IllegalStateException("No snapshots found in ducklake_snapshot table");
        }
        return maxId;
    }

    @Override
    public Optional<DucklakeSnapshot> getSnapshot(long snapshotId)
    {
        return dsl.selectFrom(DUCKLAKE_SNAPSHOT)
                .where(DUCKLAKE_SNAPSHOT.SNAPSHOT_ID.eq(snapshotId))
                .fetchOptional()
                .map(JdbcDucklakeCatalog::toDucklakeSnapshot);
    }

    @Override
    public Optional<DucklakeSnapshot> getSnapshotAtOrBefore(Instant timestamp)
    {
        return dsl.selectFrom(DUCKLAKE_SNAPSHOT)
                .orderBy(DUCKLAKE_SNAPSHOT.SNAPSHOT_ID.desc())
                .fetch(JdbcDucklakeCatalog::toDucklakeSnapshot)
                .stream()
                .filter(snapshot -> !snapshot.snapshotTime().isAfter(timestamp))
                .findFirst();
    }

    @Override
    public List<DucklakeSnapshot> listSnapshots()
    {
        return dsl.selectFrom(DUCKLAKE_SNAPSHOT)
                .orderBy(DUCKLAKE_SNAPSHOT.SNAPSHOT_ID.desc())
                .fetch(JdbcDucklakeCatalog::toDucklakeSnapshot);
    }

    @Override
    public List<DucklakeSnapshotChange> listSnapshotChanges()
    {
        return dsl.selectFrom(DUCKLAKE_SNAPSHOT_CHANGES)
                .orderBy(DUCKLAKE_SNAPSHOT_CHANGES.SNAPSHOT_ID.desc())
                .fetch(JdbcDucklakeCatalog::toDucklakeSnapshotChange);
    }

    @Override
    public List<DucklakeSchema> listSchemas(long snapshotId)
    {
        return dsl.selectFrom(DUCKLAKE_SCHEMA)
                .where(activeAt(DUCKLAKE_SCHEMA, snapshotId))
                .fetch(JdbcDucklakeCatalog::toDucklakeSchema);
    }

    @Override
    public Optional<DucklakeSchema> getSchema(String schemaName, long snapshotId)
    {
        return dsl.selectFrom(DUCKLAKE_SCHEMA)
                .where(DUCKLAKE_SCHEMA.SCHEMA_NAME.eq(schemaName))
                .and(activeAt(DUCKLAKE_SCHEMA, snapshotId))
                .fetchOptional()
                .map(JdbcDucklakeCatalog::toDucklakeSchema);
    }

    @Override
    public List<DucklakeTable> listTables(long schemaId, long snapshotId)
    {
        return dsl.selectFrom(DUCKLAKE_TABLE)
                .where(DUCKLAKE_TABLE.SCHEMA_ID.eq(schemaId))
                .and(activeAt(DUCKLAKE_TABLE, snapshotId))
                .fetch(JdbcDucklakeCatalog::toDucklakeTable);
    }

    @Override
    public Optional<DucklakeTable> getTable(String schemaName, String tableName, long snapshotId)
    {
        Optional<DucklakeSchema> schema = getSchema(schemaName, snapshotId);
        if (schema.isEmpty()) {
            return Optional.empty();
        }

        return dsl.selectFrom(DUCKLAKE_TABLE)
                .where(DUCKLAKE_TABLE.SCHEMA_ID.eq(schema.get().schemaId()))
                .and(DUCKLAKE_TABLE.TABLE_NAME.eq(tableName))
                .and(activeAt(DUCKLAKE_TABLE, snapshotId))
                .fetchOptional()
                .map(JdbcDucklakeCatalog::toDucklakeTable);
    }

    @Override
    public Optional<DucklakeTable> getTableById(long tableId, long snapshotId)
    {
        return dsl.selectFrom(DUCKLAKE_TABLE)
                .where(DUCKLAKE_TABLE.TABLE_ID.eq(tableId))
                .and(activeAt(DUCKLAKE_TABLE, snapshotId))
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
        return dsl.selectFrom(DUCKLAKE_COLUMN)
                .where(DUCKLAKE_COLUMN.TABLE_ID.eq(tableId))
                .and(activeAt(DUCKLAKE_COLUMN, snapshotId))
                .orderBy(DUCKLAKE_COLUMN.COLUMN_ORDER, DUCKLAKE_COLUMN.COLUMN_ID)
                .fetch(JdbcDucklakeCatalog::toDucklakeColumn);
    }

    @Override
    public List<DucklakeDataFile> getDataFiles(long tableId, long snapshotId)
    {
        DucklakeDeleteFileTable del = DUCKLAKE_DELETE_FILE.as("del");
        return dsl.select(
                        DUCKLAKE_DATA_FILE.DATA_FILE_ID,
                        DUCKLAKE_DATA_FILE.TABLE_ID,
                        DUCKLAKE_DATA_FILE.BEGIN_SNAPSHOT,
                        DUCKLAKE_DATA_FILE.END_SNAPSHOT,
                        DUCKLAKE_DATA_FILE.FILE_ORDER,
                        DUCKLAKE_DATA_FILE.PATH,
                        DUCKLAKE_DATA_FILE.PATH_IS_RELATIVE,
                        DUCKLAKE_DATA_FILE.FILE_FORMAT,
                        DUCKLAKE_DATA_FILE.RECORD_COUNT,
                        DUCKLAKE_DATA_FILE.FILE_SIZE_BYTES,
                        DUCKLAKE_DATA_FILE.FOOTER_SIZE,
                        DUCKLAKE_DATA_FILE.ROW_ID_START,
                        DUCKLAKE_DATA_FILE.PARTITION_ID,
                        del.PATH,
                        del.PATH_IS_RELATIVE,
                        del.FOOTER_SIZE)
                .from(DUCKLAKE_DATA_FILE)
                .leftJoin(del)
                .on(DUCKLAKE_DATA_FILE.DATA_FILE_ID.eq(del.DATA_FILE_ID))
                .and(activeAt(del, snapshotId))
                .where(DUCKLAKE_DATA_FILE.TABLE_ID.eq(tableId))
                .and(activeAt(DUCKLAKE_DATA_FILE, snapshotId))
                .orderBy(DUCKLAKE_DATA_FILE.FILE_ORDER)
                .fetch(r -> new DucklakeDataFile(
                        orZero(r.get(DUCKLAKE_DATA_FILE.DATA_FILE_ID)),
                        orZero(r.get(DUCKLAKE_DATA_FILE.TABLE_ID)),
                        orZero(r.get(DUCKLAKE_DATA_FILE.BEGIN_SNAPSHOT)),
                        Optional.ofNullable(r.get(DUCKLAKE_DATA_FILE.END_SNAPSHOT)),
                        orZero(r.get(DUCKLAKE_DATA_FILE.FILE_ORDER)),
                        r.get(DUCKLAKE_DATA_FILE.PATH),
                        Boolean.TRUE.equals(r.get(DUCKLAKE_DATA_FILE.PATH_IS_RELATIVE)),
                        r.get(DUCKLAKE_DATA_FILE.FILE_FORMAT),
                        orZero(r.get(DUCKLAKE_DATA_FILE.RECORD_COUNT)),
                        orZero(r.get(DUCKLAKE_DATA_FILE.FILE_SIZE_BYTES)),
                        orZero(r.get(DUCKLAKE_DATA_FILE.FOOTER_SIZE)),
                        orZero(r.get(DUCKLAKE_DATA_FILE.ROW_ID_START)),
                        Optional.ofNullable(r.get(DUCKLAKE_DATA_FILE.PARTITION_ID)),
                        Optional.ofNullable(r.get(del.PATH)),
                        Optional.ofNullable(r.get(del.PATH_IS_RELATIVE)),
                        Optional.ofNullable(r.get(del.FOOTER_SIZE))));
    }

    @Override
    public List<Long> findDataFileIdsInRange(long tableId, long snapshotId, ColumnRangePredicate predicate)
    {
        long columnId = predicate.columnId();

        String columnType = dsl.select(DUCKLAKE_COLUMN.COLUMN_TYPE)
                .from(DUCKLAKE_COLUMN)
                .where(DUCKLAKE_COLUMN.TABLE_ID.eq(tableId))
                .and(DUCKLAKE_COLUMN.COLUMN_ID.eq(columnId))
                .and(activeAt(DUCKLAKE_COLUMN, snapshotId))
                .fetchOne(DUCKLAKE_COLUMN.COLUMN_TYPE);
        if (columnType == null) {
            return List.of();
        }

        Comparable<?> lowerBound = parseStatValue(columnType, predicate.minValue());
        Comparable<?> upperBound = parseStatValue(columnType, predicate.maxValue());

        return dsl.select(
                        DUCKLAKE_FILE_COLUMN_STATS.DATA_FILE_ID,
                        DUCKLAKE_FILE_COLUMN_STATS.MIN_VALUE,
                        DUCKLAKE_FILE_COLUMN_STATS.MAX_VALUE)
                .from(DUCKLAKE_FILE_COLUMN_STATS)
                .innerJoin(DUCKLAKE_DATA_FILE)
                .on(DUCKLAKE_FILE_COLUMN_STATS.DATA_FILE_ID.eq(DUCKLAKE_DATA_FILE.DATA_FILE_ID))
                .where(DUCKLAKE_FILE_COLUMN_STATS.TABLE_ID.eq(tableId))
                .and(DUCKLAKE_FILE_COLUMN_STATS.COLUMN_ID.eq(columnId))
                .and(DUCKLAKE_DATA_FILE.TABLE_ID.eq(tableId))
                .and(activeAt(DUCKLAKE_DATA_FILE, snapshotId))
                .fetch()
                .stream()
                .filter(r -> isWithinBounds(
                        lowerBound,
                        upperBound,
                        parseStatValue(columnType, r.get(DUCKLAKE_FILE_COLUMN_STATS.MIN_VALUE)),
                        parseStatValue(columnType, r.get(DUCKLAKE_FILE_COLUMN_STATS.MAX_VALUE))))
                .map(r -> r.get(DUCKLAKE_FILE_COLUMN_STATS.DATA_FILE_ID))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<DucklakeTableStats> getTableStats(long tableId)
    {
        return dsl.selectFrom(DUCKLAKE_TABLE_STATS)
                .where(DUCKLAKE_TABLE_STATS.TABLE_ID.eq(tableId))
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

        dsl.select(
                        DUCKLAKE_FILE_COLUMN_STATS.COLUMN_ID,
                        DUCKLAKE_FILE_COLUMN_STATS.VALUE_COUNT,
                        DUCKLAKE_FILE_COLUMN_STATS.NULL_COUNT,
                        DUCKLAKE_FILE_COLUMN_STATS.COLUMN_SIZE_BYTES,
                        DUCKLAKE_FILE_COLUMN_STATS.MIN_VALUE,
                        DUCKLAKE_FILE_COLUMN_STATS.MAX_VALUE)
                .from(DUCKLAKE_FILE_COLUMN_STATS)
                .innerJoin(DUCKLAKE_DATA_FILE)
                .on(DUCKLAKE_FILE_COLUMN_STATS.DATA_FILE_ID.eq(DUCKLAKE_DATA_FILE.DATA_FILE_ID))
                .where(DUCKLAKE_FILE_COLUMN_STATS.TABLE_ID.eq(tableId))
                .and(DUCKLAKE_DATA_FILE.TABLE_ID.eq(tableId))
                .and(activeAt(DUCKLAKE_DATA_FILE, snapshotId))
                .forEach(r -> {
                    long columnId = orZero(r.get(DUCKLAKE_FILE_COLUMN_STATS.COLUMN_ID));
                    long[] counts = countAccumulators.computeIfAbsent(columnId, _ -> new long[3]);
                    counts[0] += orZero(r.get(DUCKLAKE_FILE_COLUMN_STATS.VALUE_COUNT));
                    counts[1] += orZero(r.get(DUCKLAKE_FILE_COLUMN_STATS.NULL_COUNT));
                    counts[2] += orZero(r.get(DUCKLAKE_FILE_COLUMN_STATS.COLUMN_SIZE_BYTES));

                    String columnType = columnTypes.getOrDefault(columnId, "");
                    String minValue = r.get(DUCKLAKE_FILE_COLUMN_STATS.MIN_VALUE);
                    String maxValue = r.get(DUCKLAKE_FILE_COLUMN_STATS.MAX_VALUE);
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

        dsl.select(
                        DUCKLAKE_PARTITION_INFO.PARTITION_ID,
                        DUCKLAKE_PARTITION_INFO.TABLE_ID,
                        DUCKLAKE_PARTITION_COLUMN.PARTITION_KEY_INDEX,
                        DUCKLAKE_PARTITION_COLUMN.COLUMN_ID,
                        DUCKLAKE_PARTITION_COLUMN.TRANSFORM)
                .from(DUCKLAKE_PARTITION_INFO)
                .innerJoin(DUCKLAKE_PARTITION_COLUMN)
                .on(DUCKLAKE_PARTITION_INFO.PARTITION_ID.eq(DUCKLAKE_PARTITION_COLUMN.PARTITION_ID))
                .and(DUCKLAKE_PARTITION_INFO.TABLE_ID.eq(DUCKLAKE_PARTITION_COLUMN.TABLE_ID))
                .where(DUCKLAKE_PARTITION_INFO.TABLE_ID.eq(tableId))
                .and(activeAt(DUCKLAKE_PARTITION_INFO, snapshotId))
                .orderBy(DUCKLAKE_PARTITION_INFO.PARTITION_ID, DUCKLAKE_PARTITION_COLUMN.PARTITION_KEY_INDEX)
                .forEach(r -> {
                    long partitionId = orZero(r.get(DUCKLAKE_PARTITION_INFO.PARTITION_ID));
                    tableIdByPartition.put(partitionId, orZero(r.get(DUCKLAKE_PARTITION_INFO.TABLE_ID)));
                    fieldsByPartition.computeIfAbsent(partitionId, _ -> new ArrayList<>())
                            .add(new DucklakePartitionField(
                                    (int) orZero(r.get(DUCKLAKE_PARTITION_COLUMN.PARTITION_KEY_INDEX)),
                                    orZero(r.get(DUCKLAKE_PARTITION_COLUMN.COLUMN_ID)),
                                    DucklakePartitionTransform.fromString(r.get(DUCKLAKE_PARTITION_COLUMN.TRANSFORM))));
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

        dsl.select(
                        DUCKLAKE_FILE_PARTITION_VALUE.DATA_FILE_ID,
                        DUCKLAKE_FILE_PARTITION_VALUE.PARTITION_KEY_INDEX,
                        DUCKLAKE_FILE_PARTITION_VALUE.PARTITION_VALUE)
                .from(DUCKLAKE_FILE_PARTITION_VALUE)
                .innerJoin(DUCKLAKE_DATA_FILE)
                .on(DUCKLAKE_FILE_PARTITION_VALUE.DATA_FILE_ID.eq(DUCKLAKE_DATA_FILE.DATA_FILE_ID))
                .and(DUCKLAKE_FILE_PARTITION_VALUE.TABLE_ID.eq(DUCKLAKE_DATA_FILE.TABLE_ID))
                .where(DUCKLAKE_FILE_PARTITION_VALUE.TABLE_ID.eq(tableId))
                .and(activeAt(DUCKLAKE_DATA_FILE, snapshotId))
                .forEach(r -> {
                    long dataFileId = orZero(r.get(DUCKLAKE_FILE_PARTITION_VALUE.DATA_FILE_ID));
                    result.computeIfAbsent(dataFileId, _ -> new ArrayList<>())
                            .add(new DucklakeFilePartitionValue(
                                    dataFileId,
                                    (int) orZero(r.get(DUCKLAKE_FILE_PARTITION_VALUE.PARTITION_KEY_INDEX)),
                                    r.get(DUCKLAKE_FILE_PARTITION_VALUE.PARTITION_VALUE)));
                });

        return result;
    }

    @Override
    public List<DucklakeInlinedDataInfo> getInlinedDataInfos(long tableId, long snapshotId)
    {
        // A table can have multiple inlined data tables (one per schema version).
        try {
            return dsl.select(
                            DUCKLAKE_INLINED_DATA_TABLES.TABLE_ID,
                            DUCKLAKE_INLINED_DATA_TABLES.TABLE_NAME,
                            DUCKLAKE_INLINED_DATA_TABLES.SCHEMA_VERSION)
                    .from(DUCKLAKE_INLINED_DATA_TABLES)
                    .where(DUCKLAKE_INLINED_DATA_TABLES.TABLE_ID.eq(tableId))
                    .and(DUCKLAKE_INLINED_DATA_TABLES.SCHEMA_VERSION.le(
                            DSL.select(DUCKLAKE_SNAPSHOT.SCHEMA_VERSION)
                                    .from(DUCKLAKE_SNAPSHOT)
                                    .where(DUCKLAKE_SNAPSHOT.SNAPSHOT_ID.eq(snapshotId))))
                    .orderBy(DUCKLAKE_INLINED_DATA_TABLES.SCHEMA_VERSION)
                    .fetch()
                    .stream()
                    .filter(r -> {
                        InlinedDataTable inlined = InlinedDataTable.of(
                                orZero(r.get(DUCKLAKE_INLINED_DATA_TABLES.TABLE_ID)),
                                orZero(r.get(DUCKLAKE_INLINED_DATA_TABLES.SCHEMA_VERSION)));
                        if (!inlined.existsAsTable(dsl)) {
                            // Catalog metadata can point to a dropped/non-materialized inlined table.
                            // Treat this as "no inlined data" so scan planning does not emit a dead split.
                            log.log(System.Logger.Level.DEBUG, "Inlined data table {0} not available for table {1}", inlined.name, tableId);
                            return false;
                        }
                        return true;
                    })
                    .map(r -> new DucklakeInlinedDataInfo(
                            orZero(r.get(DUCKLAKE_INLINED_DATA_TABLES.TABLE_ID)),
                            r.get(DUCKLAKE_INLINED_DATA_TABLES.TABLE_NAME),
                            orZero(r.get(DUCKLAKE_INLINED_DATA_TABLES.SCHEMA_VERSION))))
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
        try {
            Long tableScoped = dsl.select(DUCKLAKE_SCHEMA_VERSIONS.BEGIN_SNAPSHOT)
                    .from(DUCKLAKE_SCHEMA_VERSIONS)
                    .where(DUCKLAKE_SCHEMA_VERSIONS.TABLE_ID.eq(tableId))
                    .and(DUCKLAKE_SCHEMA_VERSIONS.SCHEMA_VERSION.eq(schemaVersion))
                    .and(DUCKLAKE_SCHEMA_VERSIONS.BEGIN_SNAPSHOT.le(snapshotId))
                    .orderBy(DUCKLAKE_SCHEMA_VERSIONS.BEGIN_SNAPSHOT.desc())
                    .limit(1)
                    .fetchOne(DUCKLAKE_SCHEMA_VERSIONS.BEGIN_SNAPSHOT);
            if (tableScoped != null) {
                return OptionalLong.of(tableScoped);
            }
        }
        catch (DataAccessException e) {
            // Fallback for catalogs without table_id in ducklake_schema_versions or older metadata.
            log.log(System.Logger.Level.DEBUG, "Could not resolve schema version via ducklake_schema_versions for table {0}: {1}", tableId, e.getMessage());
        }

        // Backward-compatible fallback: resolve by snapshot.schema_version only.
        Long fallback = dsl.select(DUCKLAKE_SNAPSHOT.SNAPSHOT_ID)
                .from(DUCKLAKE_SNAPSHOT)
                .where(DUCKLAKE_SNAPSHOT.SCHEMA_VERSION.eq(schemaVersion))
                .and(DUCKLAKE_SNAPSHOT.SNAPSHOT_ID.le(snapshotId))
                .orderBy(DUCKLAKE_SNAPSHOT.SNAPSHOT_ID.desc())
                .limit(1)
                .fetchOne(DUCKLAKE_SNAPSHOT.SNAPSHOT_ID);
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
        return dsl.select(DUCKLAKE_METADATA.VALUE)
                .from(DUCKLAKE_METADATA)
                .where(DUCKLAKE_METADATA.KEY.eq("data_path"))
                .fetchOptional(DUCKLAKE_METADATA.VALUE);
    }

    // ==================== View operations ====================

    @Override
    public List<DucklakeView> listViews(long schemaId, long snapshotId)
    {
        return dsl.selectFrom(DUCKLAKE_VIEW)
                .where(DUCKLAKE_VIEW.SCHEMA_ID.eq(schemaId))
                .and(activeAt(DUCKLAKE_VIEW, snapshotId))
                .fetch(JdbcDucklakeCatalog::toDucklakeView);
    }

    @Override
    public Optional<DucklakeView> getView(String schemaName, String viewName, long snapshotId)
    {
        Optional<DucklakeSchema> schema = getSchema(schemaName, snapshotId);
        if (schema.isEmpty()) {
            return Optional.empty();
        }

        return dsl.selectFrom(DUCKLAKE_VIEW)
                .where(DUCKLAKE_VIEW.SCHEMA_ID.eq(schema.get().schemaId()))
                .and(DUCKLAKE_VIEW.VIEW_NAME.eq(viewName))
                .and(activeAt(DUCKLAKE_VIEW, snapshotId))
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
     * Executes a write operation within an atomic snapshot transaction.
     * Handles connection management, snapshot creation, change tracking,
     * and commit/rollback. The caller provides a callback that performs
     * its mutations using the transaction context.
     */
    private void executeWriteTransaction(String operationDescription, WriteTransactionAction action)
    {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            DSLContext txDsl = forConnection(conn);
            long baseSnapshotId = -1;
            try {
                // 1. Read current snapshot state
                DucklakeSnapshotRecord snapshotRow = txDsl.selectFrom(DUCKLAKE_SNAPSHOT)
                        .where(DUCKLAKE_SNAPSHOT.SNAPSHOT_ID.eq(
                                DSL.select(DSL.max(DUCKLAKE_SNAPSHOT.SNAPSHOT_ID)).from(DUCKLAKE_SNAPSHOT)))
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
                    txDsl.insertInto(DUCKLAKE_SCHEMA_VERSIONS)
                            .set(DUCKLAKE_SCHEMA_VERSIONS.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                            .set(DUCKLAKE_SCHEMA_VERSIONS.SCHEMA_VERSION, tx.getSchemaVersion())
                            .set(DUCKLAKE_SCHEMA_VERSIONS.TABLE_ID, schemaVersionTableId)
                            .execute();
                }

                // 6. Insert snapshot changes (comma-separated per spec, one row per snapshot)
                if (!tx.getChanges().isEmpty()) {
                    txDsl.insertInto(DUCKLAKE_SNAPSHOT_CHANGES)
                            .set(DUCKLAKE_SNAPSHOT_CHANGES.SNAPSHOT_ID, tx.getNewSnapshotId())
                            .set(DUCKLAKE_SNAPSHOT_CHANGES.CHANGES_MADE, formatChangesMade(tx.getChanges()))
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
        try {
            ctx.insertInto(DUCKLAKE_SNAPSHOT)
                    .set(DUCKLAKE_SNAPSHOT.SNAPSHOT_ID, tx.getNewSnapshotId())
                    .set(DUCKLAKE_SNAPSHOT.SNAPSHOT_TIME, DSL.currentOffsetDateTime())
                    .set(DUCKLAKE_SNAPSHOT.SCHEMA_VERSION, tx.getSchemaVersion())
                    .set(DUCKLAKE_SNAPSHOT.NEXT_CATALOG_ID, tx.getFinalNextCatalogId())
                    .set(DUCKLAKE_SNAPSHOT.NEXT_FILE_ID, tx.getFinalNextFileId())
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

        List<String> changes = ctx.select(
                        DUCKLAKE_SNAPSHOT_CHANGES.SNAPSHOT_ID,
                        DUCKLAKE_SNAPSHOT_CHANGES.CHANGES_MADE)
                .from(DUCKLAKE_SNAPSHOT_CHANGES)
                .where(DUCKLAKE_SNAPSHOT_CHANGES.SNAPSHOT_ID.gt(fromSnapshotExclusive))
                .and(DUCKLAKE_SNAPSHOT_CHANGES.SNAPSHOT_ID.le(toSnapshotInclusive))
                .orderBy(DUCKLAKE_SNAPSHOT_CHANGES.SNAPSHOT_ID)
                .limit(CONFLICT_CHANGE_SUMMARY_LIMIT)
                .fetch()
                .stream()
                .map(r -> r.get(DUCKLAKE_SNAPSHOT_CHANGES.SNAPSHOT_ID)
                        + ":" + r.get(DUCKLAKE_SNAPSHOT_CHANGES.CHANGES_MADE))
                .collect(Collectors.toList());

        if (changes.isEmpty()) {
            return "snapshot advanced without snapshot_changes rows";
        }
        return String.join("; ", changes);
    }

    private long readLatestSnapshotId(DSLContext ctx)
    {
        Long maxId = ctx.select(DSL.max(DUCKLAKE_SNAPSHOT.SNAPSHOT_ID))
                .from(DUCKLAKE_SNAPSHOT)
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
        Record row = tx.dsl().select(
                        DUCKLAKE_VIEW.VIEW_ID,
                        DUCKLAKE_VIEW.VIEW_UUID,
                        DUCKLAKE_VIEW.SCHEMA_ID,
                        DUCKLAKE_VIEW.VIEW_NAME,
                        DUCKLAKE_VIEW.DIALECT,
                        DUCKLAKE_VIEW.SQL,
                        DUCKLAKE_VIEW.COLUMN_ALIASES)
                .from(DUCKLAKE_VIEW)
                .where(DUCKLAKE_VIEW.SCHEMA_ID.eq(schemaId))
                .and(DUCKLAKE_VIEW.VIEW_NAME.eq(viewName))
                .and(activeAt(DUCKLAKE_VIEW, tx.getCurrentSnapshotId()))
                .fetchOne();
        if (row == null) {
            throw new RuntimeException("View not found: schema_id=" + schemaId + ", view_name=" + viewName);
        }
        return new ActiveViewRow(
                orZero(row.get(DUCKLAKE_VIEW.VIEW_ID)),
                row.get(DUCKLAKE_VIEW.VIEW_UUID),
                orZero(row.get(DUCKLAKE_VIEW.SCHEMA_ID)),
                row.get(DUCKLAKE_VIEW.VIEW_NAME),
                row.get(DUCKLAKE_VIEW.DIALECT),
                row.get(DUCKLAKE_VIEW.SQL),
                Optional.ofNullable(row.get(DUCKLAKE_VIEW.COLUMN_ALIASES)));
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
        tx.dsl().insertInto(DUCKLAKE_VIEW)
                .set(DUCKLAKE_VIEW.VIEW_ID, viewId)
                .set(DUCKLAKE_VIEW.VIEW_UUID, viewUuid)
                .set(DUCKLAKE_VIEW.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                .set(DUCKLAKE_VIEW.SCHEMA_ID, schemaId)
                .set(DUCKLAKE_VIEW.VIEW_NAME, viewName)
                .set(DUCKLAKE_VIEW.DIALECT, dialect)
                .set(DUCKLAKE_VIEW.SQL, viewSql)
                .set(DUCKLAKE_VIEW.COLUMN_ALIASES, viewMetadata)
                .execute();
    }

    private void endSnapshotActiveView(DucklakeWriteTransaction tx, long viewId)
    {
        int updatedRows = tx.dsl().update(DUCKLAKE_VIEW)
                .set(DUCKLAKE_VIEW.END_SNAPSHOT, tx.getNewSnapshotId())
                .where(DUCKLAKE_VIEW.VIEW_ID.eq(viewId))
                .and(DUCKLAKE_VIEW.END_SNAPSHOT.isNull())
                .execute();
        if (updatedRows == 0) {
            throw new RuntimeException("View not found: " + viewId);
        }
    }

    private static boolean hasActiveView(DucklakeWriteTransaction tx, long schemaId, String viewName)
    {
        return tx.dsl().fetchExists(
                DSL.selectOne()
                        .from(DUCKLAKE_VIEW)
                        .where(DUCKLAKE_VIEW.SCHEMA_ID.eq(schemaId))
                        .and(DUCKLAKE_VIEW.VIEW_NAME.eq(viewName))
                        .and(activeAt(DUCKLAKE_VIEW, tx.getCurrentSnapshotId())));
    }

    private static boolean hasActiveTable(DucklakeWriteTransaction tx, long schemaId, String tableName)
    {
        return tx.dsl().fetchExists(
                DSL.selectOne()
                        .from(DUCKLAKE_TABLE)
                        .where(DUCKLAKE_TABLE.SCHEMA_ID.eq(schemaId))
                        .and(DUCKLAKE_TABLE.TABLE_NAME.eq(tableName))
                        .and(activeAt(DUCKLAKE_TABLE, tx.getCurrentSnapshotId())));
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
        executeWriteTransaction("create schema " + schemaName, tx -> {
            long schemaId = tx.allocateCatalogId();
            tx.addChange(changeCreatedSchema(schemaName));

            tx.dsl().insertInto(DUCKLAKE_SCHEMA)
                    .set(DUCKLAKE_SCHEMA.SCHEMA_ID, schemaId)
                    .set(DUCKLAKE_SCHEMA.SCHEMA_UUID, UUID.fromString(newCatalogUuid()))
                    .set(DUCKLAKE_SCHEMA.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                    .set(DUCKLAKE_SCHEMA.SCHEMA_NAME, schemaName)
                    .set(DUCKLAKE_SCHEMA.PATH, schemaName + "/")
                    .set(DUCKLAKE_SCHEMA.PATH_IS_RELATIVE, true)
                    .execute();

            tx.incrementSchemaVersion();
        });
    }

    @Override
    public void dropSchema(String schemaName)
    {
        executeWriteTransaction("drop schema " + schemaName, tx -> {
            long schemaId = tx.resolveSchemaId(schemaName);

            if (tx.hasTablesInSchema(schemaId)) {
                throw new RuntimeException("Cannot drop schema " + schemaName + ": schema is not empty");
            }

            tx.addChange("dropped_schema:" + schemaId);

            tx.dsl().update(DUCKLAKE_SCHEMA)
                    .set(DUCKLAKE_SCHEMA.END_SNAPSHOT, tx.getNewSnapshotId())
                    .where(DUCKLAKE_SCHEMA.SCHEMA_ID.eq(schemaId))
                    .and(DUCKLAKE_SCHEMA.END_SNAPSHOT.isNull())
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
        executeWriteTransaction("create table " + schemaName + "." + tableName, tx -> {
            long schemaId = tx.resolveSchemaId(schemaName);
            long tableId = tx.allocateCatalogId();
            DSLContext ctx = tx.dsl();

            // 1. Insert table row
            ctx.insertInto(DUCKLAKE_TABLE)
                    .set(DUCKLAKE_TABLE.TABLE_ID, tableId)
                    .set(DUCKLAKE_TABLE.TABLE_UUID, UUID.fromString(newCatalogUuid()))
                    .set(DUCKLAKE_TABLE.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                    .set(DUCKLAKE_TABLE.SCHEMA_ID, schemaId)
                    .set(DUCKLAKE_TABLE.TABLE_NAME, tableName)
                    .set(DUCKLAKE_TABLE.PATH, tableName + "/")
                    .set(DUCKLAKE_TABLE.PATH_IS_RELATIVE, true)
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

                ctx.insertInto(DUCKLAKE_PARTITION_INFO)
                        .set(DUCKLAKE_PARTITION_INFO.PARTITION_ID, partitionId)
                        .set(DUCKLAKE_PARTITION_INFO.TABLE_ID, tableId)
                        .set(DUCKLAKE_PARTITION_INFO.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                        .execute();

                long keyIndex = 0;
                for (PartitionFieldSpec field : partitionSpec.get()) {
                    Long columnId = topLevelColumnIds.get(field.columnName());
                    if (columnId == null) {
                        throw new RuntimeException("Partition column not found: " + field.columnName());
                    }
                    ctx.insertInto(DUCKLAKE_PARTITION_COLUMN)
                            .set(DUCKLAKE_PARTITION_COLUMN.PARTITION_ID, partitionId)
                            .set(DUCKLAKE_PARTITION_COLUMN.TABLE_ID, tableId)
                            .set(DUCKLAKE_PARTITION_COLUMN.PARTITION_KEY_INDEX, keyIndex++)
                            .set(DUCKLAKE_PARTITION_COLUMN.COLUMN_ID, columnId)
                            .set(DUCKLAKE_PARTITION_COLUMN.TRANSFORM, field.transform().name().toLowerCase(ENGLISH))
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
        tx.dsl().insertInto(DUCKLAKE_COLUMN)
                .set(DUCKLAKE_COLUMN.COLUMN_ID, columnId)
                .set(DUCKLAKE_COLUMN.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                .set(DUCKLAKE_COLUMN.TABLE_ID, tableId)
                .set(DUCKLAKE_COLUMN.COLUMN_ORDER, columnOrder)
                .set(DUCKLAKE_COLUMN.COLUMN_NAME, column.name())
                .set(DUCKLAKE_COLUMN.COLUMN_TYPE, column.ducklakeType())
                .set(DUCKLAKE_COLUMN.DEFAULT_VALUE, "NULL")
                .set(DUCKLAKE_COLUMN.NULLS_ALLOWED, column.nullable())
                .set(DUCKLAKE_COLUMN.PARENT_COLUMN, parentColumnId.isPresent() ? parentColumnId.getAsLong() : null)
                .set(DUCKLAKE_COLUMN.DEFAULT_VALUE_TYPE, "literal")
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
        executeWriteTransaction("drop table " + schemaName + "." + tableName, tx -> {
            long schemaId = tx.resolveSchemaId(schemaName);
            long tableId = tx.resolveTableId(schemaId, tableName);
            DSLContext ctx = tx.dsl();
            long newSnapshotId = tx.getNewSnapshotId();

            // End-snapshot the table row
            ctx.update(DUCKLAKE_TABLE)
                    .set(DUCKLAKE_TABLE.END_SNAPSHOT, newSnapshotId)
                    .where(DUCKLAKE_TABLE.TABLE_ID.eq(tableId))
                    .and(DUCKLAKE_TABLE.END_SNAPSHOT.isNull())
                    .execute();

            // End-snapshot all active columns
            ctx.update(DUCKLAKE_COLUMN)
                    .set(DUCKLAKE_COLUMN.END_SNAPSHOT, newSnapshotId)
                    .where(DUCKLAKE_COLUMN.TABLE_ID.eq(tableId))
                    .and(DUCKLAKE_COLUMN.END_SNAPSHOT.isNull())
                    .execute();

            // End-snapshot all active data files
            ctx.update(DUCKLAKE_DATA_FILE)
                    .set(DUCKLAKE_DATA_FILE.END_SNAPSHOT, newSnapshotId)
                    .where(DUCKLAKE_DATA_FILE.TABLE_ID.eq(tableId))
                    .and(DUCKLAKE_DATA_FILE.END_SNAPSHOT.isNull())
                    .execute();

            // End-snapshot all active delete files (matched via data_file subquery)
            ctx.update(DUCKLAKE_DELETE_FILE)
                    .set(DUCKLAKE_DELETE_FILE.END_SNAPSHOT, newSnapshotId)
                    .where(DUCKLAKE_DELETE_FILE.DATA_FILE_ID.in(
                            DSL.select(DUCKLAKE_DATA_FILE.DATA_FILE_ID)
                                    .from(DUCKLAKE_DATA_FILE)
                                    .where(DUCKLAKE_DATA_FILE.TABLE_ID.eq(tableId))))
                    .and(DUCKLAKE_DELETE_FILE.END_SNAPSHOT.isNull())
                    .execute();

            // End-snapshot partition info
            ctx.update(DUCKLAKE_PARTITION_INFO)
                    .set(DUCKLAKE_PARTITION_INFO.END_SNAPSHOT, newSnapshotId)
                    .where(DUCKLAKE_PARTITION_INFO.TABLE_ID.eq(tableId))
                    .and(DUCKLAKE_PARTITION_INFO.END_SNAPSHOT.isNull())
                    .execute();

            tx.incrementSchemaVersion(tableId);
            tx.addChange("dropped_table:" + tableId);
        });
    }

    @Override
    public void addColumn(long tableId, TableColumnSpec column)
    {
        executeWriteTransaction("add column to table " + tableId, tx -> {
            // Find the current max column_order for top-level columns
            Long maxOrder = tx.dsl().select(DSL.max(DUCKLAKE_COLUMN.COLUMN_ORDER))
                    .from(DUCKLAKE_COLUMN)
                    .where(DUCKLAKE_COLUMN.TABLE_ID.eq(tableId))
                    .and(DUCKLAKE_COLUMN.PARENT_COLUMN.isNull())
                    .and(activeAt(DUCKLAKE_COLUMN, tx.getCurrentSnapshotId()))
                    .fetchOne(0, Long.class);

            insertColumnTree(tx, tableId, column, orZero(maxOrder) + 1, OptionalLong.empty());
            tx.incrementSchemaVersion(tableId);
            tx.addChange("altered_table:" + tableId);
        });
    }

    @Override
    public void dropColumn(long tableId, long columnId)
    {
        executeWriteTransaction("drop column from table " + tableId, tx -> {
            // End-snapshot the column and all its children (for nested types)
            tx.dsl().update(DUCKLAKE_COLUMN)
                    .set(DUCKLAKE_COLUMN.END_SNAPSHOT, tx.getNewSnapshotId())
                    .where(DUCKLAKE_COLUMN.TABLE_ID.eq(tableId))
                    .and(DUCKLAKE_COLUMN.COLUMN_ID.eq(columnId)
                            .or(DUCKLAKE_COLUMN.PARENT_COLUMN.eq(columnId)))
                    .and(DUCKLAKE_COLUMN.END_SNAPSHOT.isNull())
                    .execute();

            tx.incrementSchemaVersion(tableId);
            tx.addChange("altered_table:" + tableId);
        });
    }

    @Override
    public void renameColumn(long tableId, long columnId, String newName)
    {
        executeWriteTransaction("rename column in table " + tableId, tx -> {
            DSLContext ctx = tx.dsl();

            // Read the current column metadata
            Record existing = ctx.select(DUCKLAKE_COLUMN.COLUMN_ORDER, DUCKLAKE_COLUMN.COLUMN_TYPE, DUCKLAKE_COLUMN.NULLS_ALLOWED)
                    .from(DUCKLAKE_COLUMN)
                    .where(DUCKLAKE_COLUMN.TABLE_ID.eq(tableId))
                    .and(DUCKLAKE_COLUMN.COLUMN_ID.eq(columnId))
                    .and(activeAt(DUCKLAKE_COLUMN, tx.getCurrentSnapshotId()))
                    .fetchOne();
            if (existing == null) {
                throw new RuntimeException("Column not found: " + columnId);
            }
            long columnOrder = orZero(existing.get(DUCKLAKE_COLUMN.COLUMN_ORDER));
            String columnType = existing.get(DUCKLAKE_COLUMN.COLUMN_TYPE);
            boolean nullsAllowed = Boolean.TRUE.equals(existing.get(DUCKLAKE_COLUMN.NULLS_ALLOWED));

            // End-snapshot the current version
            ctx.update(DUCKLAKE_COLUMN)
                    .set(DUCKLAKE_COLUMN.END_SNAPSHOT, tx.getNewSnapshotId())
                    .where(DUCKLAKE_COLUMN.TABLE_ID.eq(tableId))
                    .and(DUCKLAKE_COLUMN.COLUMN_ID.eq(columnId))
                    .and(DUCKLAKE_COLUMN.END_SNAPSHOT.isNull())
                    .and(DUCKLAKE_COLUMN.PARENT_COLUMN.isNull())
                    .execute();

            // Insert new version with same column_id but new name. Default-value columns
            // preserve the "no default" sentinel (`'NULL'` string literal) and leave
            // `default_value_dialect` SQL NULL; same policy as insertColumnTree.
            ctx.insertInto(DUCKLAKE_COLUMN)
                    .set(DUCKLAKE_COLUMN.COLUMN_ID, columnId)
                    .set(DUCKLAKE_COLUMN.BEGIN_SNAPSHOT, tx.getNewSnapshotId())
                    .set(DUCKLAKE_COLUMN.TABLE_ID, tableId)
                    .set(DUCKLAKE_COLUMN.COLUMN_ORDER, columnOrder)
                    .set(DUCKLAKE_COLUMN.COLUMN_NAME, newName)
                    .set(DUCKLAKE_COLUMN.COLUMN_TYPE, columnType)
                    .set(DUCKLAKE_COLUMN.DEFAULT_VALUE, "NULL")
                    .set(DUCKLAKE_COLUMN.NULLS_ALLOWED, nullsAllowed)
                    .set(DUCKLAKE_COLUMN.DEFAULT_VALUE_TYPE, "literal")
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
            throws SQLException
    {
        // Read current table stats (may not exist yet — DuckDB creates them on first insert)
        long currentNextRowId = 0;
        long currentRecordCount = 0;
        long currentFileSizeBytes = 0;
        boolean tableStatsExist;
        try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                "SELECT record_count, next_row_id, file_size_bytes FROM ducklake_table_stats WHERE table_id = ?")) {
            stmt.setLong(1, tableId);
            try (ResultSet rs = stmt.executeQuery()) {
                tableStatsExist = rs.next();
                if (tableStatsExist) {
                    currentRecordCount = rs.getLong("record_count");
                    currentNextRowId = rs.getLong("next_row_id");
                    currentFileSizeBytes = rs.getLong("file_size_bytes");
                }
            }
        }

        long runningRowId = currentNextRowId;
        long totalRecords = 0;
        long totalFileSize = 0;
        int partitionValueBatchRows = 0;
        int fileColumnStatsBatchRows = 0;
        try (PreparedStatement dataFileStmt = tx.getConnection().prepareStatement(
                "INSERT INTO ducklake_data_file (data_file_id, table_id, begin_snapshot, end_snapshot, " +
                        "file_order, path, path_is_relative, file_format, record_count, file_size_bytes, " +
                        "footer_size, row_id_start, partition_id) " +
                        "VALUES (?, ?, ?, NULL, ?, ?, true, 'parquet', ?, ?, ?, ?, ?)");
                PreparedStatement partitionValueStmt = tx.getConnection().prepareStatement(
                        "INSERT INTO ducklake_file_partition_value (table_id, data_file_id, partition_key_index, partition_value) " +
                                "VALUES (?, ?, ?, ?)");
                PreparedStatement fileColumnStatsStmt = tx.getConnection().prepareStatement(
                        "INSERT INTO ducklake_file_column_stats (data_file_id, table_id, column_id, " +
                                "column_size_bytes, value_count, null_count, min_value, max_value, contains_nan) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            for (DucklakeWriteFragment fragment : fragments) {
                long dataFileId = tx.allocateFileId();

                dataFileStmt.setLong(1, dataFileId);
                dataFileStmt.setLong(2, tableId);
                dataFileStmt.setLong(3, tx.getNewSnapshotId());
                dataFileStmt.setNull(4, java.sql.Types.BIGINT); // file_order: NULL matches DuckDB convention
                dataFileStmt.setString(5, fragment.path());
                dataFileStmt.setLong(6, fragment.recordCount());
                dataFileStmt.setLong(7, fragment.fileSizeBytes());
                dataFileStmt.setLong(8, fragment.footerSize());
                dataFileStmt.setLong(9, runningRowId);
                if (fragment.partitionId().isPresent()) {
                    dataFileStmt.setLong(10, fragment.partitionId().getAsLong());
                }
                else {
                    dataFileStmt.setNull(10, java.sql.Types.BIGINT);
                }
                dataFileStmt.executeUpdate();

                for (Map.Entry<Integer, String> partitionValue : fragment.partitionValues().entrySet()) {
                    partitionValueStmt.setLong(1, tableId);
                    partitionValueStmt.setLong(2, dataFileId);
                    partitionValueStmt.setInt(3, partitionValue.getKey());
                    setNullableString(partitionValueStmt, 4, partitionValue.getValue());
                    partitionValueStmt.addBatch();
                    partitionValueBatchRows++;
                }

                for (DucklakeFileColumnStats columnStats : fragment.columnStats()) {
                    fileColumnStatsStmt.setLong(1, dataFileId);
                    fileColumnStatsStmt.setLong(2, tableId);
                    fileColumnStatsStmt.setLong(3, columnStats.columnId());
                    fileColumnStatsStmt.setLong(4, columnStats.columnSizeBytes());
                    fileColumnStatsStmt.setLong(5, columnStats.valueCount());
                    fileColumnStatsStmt.setLong(6, columnStats.nullCount());
                    setNullableString(fileColumnStatsStmt, 7, columnStats.minValue().orElse(null));
                    setNullableString(fileColumnStatsStmt, 8, columnStats.maxValue().orElse(null));
                    if (columnStats.containsNan()) {
                        fileColumnStatsStmt.setBoolean(9, true);
                    }
                    else {
                        fileColumnStatsStmt.setNull(9, java.sql.Types.BOOLEAN);
                    }
                    fileColumnStatsStmt.addBatch();
                    fileColumnStatsBatchRows++;
                }

                runningRowId += fragment.recordCount();
                totalRecords += fragment.recordCount();
                totalFileSize += fragment.fileSizeBytes();
            }

            if (partitionValueBatchRows > 0) {
                partitionValueStmt.executeBatch();
            }
            if (fileColumnStatsBatchRows > 0) {
                fileColumnStatsStmt.executeBatch();
            }
        }

        // Insert or update ducklake_table_stats
        if (tableStatsExist) {
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "UPDATE ducklake_table_stats SET record_count = ?, next_row_id = ?, file_size_bytes = ? WHERE table_id = ?")) {
                stmt.setLong(1, currentRecordCount + totalRecords);
                stmt.setLong(2, currentNextRowId + totalRecords);
                stmt.setLong(3, currentFileSizeBytes + totalFileSize);
                stmt.setLong(4, tableId);
                stmt.executeUpdate();
            }
        }
        else {
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "INSERT INTO ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes) VALUES (?, ?, ?, ?)")) {
                stmt.setLong(1, tableId);
                stmt.setLong(2, totalRecords);
                stmt.setLong(3, totalRecords);
                stmt.setLong(4, totalFileSize);
                stmt.executeUpdate();
            }
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
        try (PreparedStatement updateTableColumnStatsStmt = tx.getConnection().prepareStatement(
                "UPDATE ducklake_table_column_stats SET " +
                        "contains_null = (contains_null OR ?), " +
                        "contains_nan = (contains_nan OR ?), " +
                        "min_value = CASE WHEN min_value IS NULL THEN ? WHEN ? IS NULL THEN min_value WHEN ? < min_value THEN ? ELSE min_value END, " +
                        "max_value = CASE WHEN max_value IS NULL THEN ? WHEN ? IS NULL THEN max_value WHEN ? > max_value THEN ? ELSE max_value END " +
                        "WHERE table_id = ? AND column_id = ?");
                PreparedStatement insertTableColumnStatsStmt = tx.getConnection().prepareStatement(
                        "INSERT INTO ducklake_table_column_stats (table_id, column_id, contains_null, contains_nan, min_value, max_value) " +
                                "VALUES (?, ?, ?, ?, ?, ?)")) {
            int updateBatchRows = 0;
            int insertBatchRows = 0;
            for (Map.Entry<Long, AggregatedColumnStats> entry : columnAggregates.entrySet()) {
                long columnId = entry.getKey();
                AggregatedColumnStats agg = entry.getValue();

                if (existingColumnStats.contains(columnId)) {
                    updateTableColumnStatsStmt.setBoolean(1, agg.containsNull);
                    updateTableColumnStatsStmt.setBoolean(2, agg.containsNan);
                    setNullableString(updateTableColumnStatsStmt, 3, agg.minValue);
                    setNullableString(updateTableColumnStatsStmt, 4, agg.minValue);
                    setNullableString(updateTableColumnStatsStmt, 5, agg.minValue);
                    setNullableString(updateTableColumnStatsStmt, 6, agg.minValue);
                    setNullableString(updateTableColumnStatsStmt, 7, agg.maxValue);
                    setNullableString(updateTableColumnStatsStmt, 8, agg.maxValue);
                    setNullableString(updateTableColumnStatsStmt, 9, agg.maxValue);
                    setNullableString(updateTableColumnStatsStmt, 10, agg.maxValue);
                    updateTableColumnStatsStmt.setLong(11, tableId);
                    updateTableColumnStatsStmt.setLong(12, columnId);
                    updateTableColumnStatsStmt.addBatch();
                    updateBatchRows++;
                }
                else {
                    insertTableColumnStatsStmt.setLong(1, tableId);
                    insertTableColumnStatsStmt.setLong(2, columnId);
                    insertTableColumnStatsStmt.setBoolean(3, agg.containsNull);
                    if (agg.containsNan) {
                        insertTableColumnStatsStmt.setBoolean(4, true);
                    }
                    else {
                        insertTableColumnStatsStmt.setNull(4, java.sql.Types.BOOLEAN);
                    }
                    setNullableString(insertTableColumnStatsStmt, 5, agg.minValue);
                    setNullableString(insertTableColumnStatsStmt, 6, agg.maxValue);
                    insertTableColumnStatsStmt.addBatch();
                    insertBatchRows++;
                }
            }

            if (updateBatchRows > 0) {
                updateTableColumnStatsStmt.executeBatch();
            }
            if (insertBatchRows > 0) {
                insertTableColumnStatsStmt.executeBatch();
            }
        }
    }

    private static Set<Long> loadExistingColumnStatsColumnIds(DucklakeWriteTransaction tx, long tableId, Set<Long> candidateColumnIds)
            throws SQLException
    {
        if (candidateColumnIds.isEmpty()) {
            return Set.of();
        }

        List<Long> orderedColumnIds = new ArrayList<>(candidateColumnIds);
        String placeholders = orderedColumnIds.stream()
                .map(ignored -> "?")
                .collect(Collectors.joining(", "));
        String sql = "SELECT column_id FROM ducklake_table_column_stats WHERE table_id = ? AND column_id IN (" + placeholders + ")";

        Set<Long> existingColumnIds = new HashSet<>();
        try (PreparedStatement stmt = tx.getConnection().prepareStatement(sql)) {
            int parameterIndex = 1;
            stmt.setLong(parameterIndex++, tableId);
            for (long columnId : orderedColumnIds) {
                stmt.setLong(parameterIndex++, columnId);
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    existingColumnIds.add(rs.getLong("column_id"));
                }
            }
        }
        return existingColumnIds;
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
            throws SQLException
    {
        long totalDeleteCount = 0;

        for (DucklakeDeleteFragment fragment : deleteFragments) {
            long deleteFileId = tx.allocateFileId();

            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "INSERT INTO ducklake_delete_file (delete_file_id, table_id, begin_snapshot, end_snapshot, " +
                            "data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, " +
                            "footer_size, encryption_key, partial_max) " +
                            "VALUES (?, ?, ?, NULL, ?, ?, true, 'parquet', ?, ?, ?, NULL, NULL)")) {
                stmt.setLong(1, deleteFileId);
                stmt.setLong(2, tableId);
                stmt.setLong(3, tx.getNewSnapshotId());
                stmt.setLong(4, fragment.dataFileId());
                stmt.setString(5, fragment.path());
                stmt.setLong(6, fragment.deleteCount());
                stmt.setLong(7, fragment.fileSizeBytes());
                stmt.setLong(8, fragment.footerSize());
                stmt.executeUpdate();
            }

            totalDeleteCount += fragment.deleteCount();
        }

        // Update table stats: decrement record count
        try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                "UPDATE ducklake_table_stats SET record_count = GREATEST(0, record_count - ?) WHERE table_id = ?")) {
            stmt.setLong(1, totalDeleteCount);
            stmt.setLong(2, tableId);
            stmt.executeUpdate();
        }
    }

    private void setUuid(PreparedStatement stmt, int index, String uuid)
            throws SQLException
    {
        if (isPostgresql) {
            stmt.setObject(index, uuid, java.sql.Types.OTHER);
        }
        else {
            stmt.setString(index, uuid);
        }
    }

    private static void setNullableString(PreparedStatement stmt, int index, String value)
            throws SQLException
    {
        if (value != null) {
            stmt.setString(index, value);
        }
        else {
            stmt.setNull(index, java.sql.Types.VARCHAR);
        }
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

    // Helper methods for handling nullable columns

    private Optional<Long> getLongOptional(ResultSet rs, String columnName)
            throws SQLException
    {
        long value = rs.getLong(columnName);
        return rs.wasNull() ? Optional.empty() : Optional.of(value);
    }

    private Optional<String> getStringOptional(ResultSet rs, String columnName)
            throws SQLException
    {
        String value = rs.getString(columnName);
        return Optional.ofNullable(value);
    }

    private Optional<Boolean> getBooleanOptional(ResultSet rs, String columnName)
            throws SQLException
    {
        boolean value = rs.getBoolean(columnName);
        return rs.wasNull() ? Optional.empty() : Optional.of(value);
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
