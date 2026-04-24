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

    private final DataSource dataSource;
    private final HikariDataSource hikariDataSource;
    private final boolean isPostgresql;

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

        log.log(System.Logger.Level.INFO, "Initialized Ducklake JDBC catalog: {0}", config.getCatalogDatabaseUrl());
    }

    @Override
    public long getCurrentSnapshotId()
    {
        String sql = "SELECT snapshot_id FROM ducklake_snapshot WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getLong("snapshot_id");
            }
            throw new IllegalStateException("No snapshots found in ducklake_snapshot table");
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get current snapshot", e);
        }
    }

    @Override
    public Optional<DucklakeSnapshot> getSnapshot(long snapshotId)
    {
        String sql = "SELECT snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id " +
                     "FROM ducklake_snapshot WHERE snapshot_id = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(readSnapshot(rs));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get snapshot: " + snapshotId, e);
        }
    }

    @Override
    public Optional<DucklakeSnapshot> getSnapshotAtOrBefore(Instant timestamp)
    {
        String sql = "SELECT snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id " +
                     "FROM ducklake_snapshot " +
                     "ORDER BY snapshot_id DESC";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    DucklakeSnapshot snapshot = readSnapshot(rs);
                    if (!snapshot.snapshotTime().isAfter(timestamp)) {
                        return Optional.of(snapshot);
                    }
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get snapshot at or before timestamp: " + timestamp, e);
        }
    }

    @Override
    public List<DucklakeSnapshot> listSnapshots()
    {
        String sql = "SELECT snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id " +
                     "FROM ducklake_snapshot " +
                     "ORDER BY snapshot_id DESC";

        List<DucklakeSnapshot> snapshots = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                snapshots.add(readSnapshot(rs));
            }
            return snapshots;
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list snapshots", e);
        }
    }

    @Override
    public List<DucklakeSnapshotChange> listSnapshotChanges()
    {
        String sql = "SELECT snapshot_id, changes_made, author, commit_message, commit_extra_info " +
                     "FROM ducklake_snapshot_changes " +
                     "ORDER BY snapshot_id DESC";

        List<DucklakeSnapshotChange> changes = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                changes.add(new DucklakeSnapshotChange(
                        rs.getLong("snapshot_id"),
                        getStringOptional(rs, "changes_made"),
                        getStringOptional(rs, "author"),
                        getStringOptional(rs, "commit_message"),
                        getStringOptional(rs, "commit_extra_info")));
            }
            return changes;
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list snapshot changes", e);
        }
    }

    @Override
    public List<DucklakeSchema> listSchemas(long snapshotId)
    {
        String sql = "SELECT schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative " +
                     "FROM ducklake_schema " +
                     "WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        List<DucklakeSchema> schemas = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, snapshotId);
            stmt.setLong(2, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    schemas.add(new DucklakeSchema(
                            rs.getLong("schema_id"),
                            UUID.fromString(rs.getString("schema_uuid")),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getString("schema_name"),
                            getStringOptional(rs, "path"),
                            getBooleanOptional(rs, "path_is_relative")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list schemas at snapshot: " + snapshotId, e);
        }

        return schemas;
    }

    @Override
    public Optional<DucklakeSchema> getSchema(String schemaName, long snapshotId)
    {
        String sql = "SELECT schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative " +
                     "FROM ducklake_schema " +
                     "WHERE schema_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, schemaName);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new DucklakeSchema(
                            rs.getLong("schema_id"),
                            UUID.fromString(rs.getString("schema_uuid")),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getString("schema_name"),
                            getStringOptional(rs, "path"),
                            getBooleanOptional(rs, "path_is_relative")));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get schema: " + schemaName + " at snapshot: " + snapshotId, e);
        }
    }

    @Override
    public List<DucklakeTable> listTables(long schemaId, long snapshotId)
    {
        String sql = "SELECT table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative " +
                     "FROM ducklake_table " +
                     "WHERE schema_id = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        List<DucklakeTable> tables = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, schemaId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tables.add(new DucklakeTable(
                            rs.getLong("table_id"),
                            UUID.fromString(rs.getString("table_uuid")),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getLong("schema_id"),
                            rs.getString("table_name"),
                            getStringOptional(rs, "path"),
                            getBooleanOptional(rs, "path_is_relative")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list tables for schema: " + schemaId + " at snapshot: " + snapshotId, e);
        }

        return tables;
    }

    @Override
    public Optional<DucklakeTable> getTable(String schemaName, String tableName, long snapshotId)
    {
        // First get the schema
        Optional<DucklakeSchema> schema = getSchema(schemaName, snapshotId);
        if (schema.isEmpty()) {
            return Optional.empty();
        }

        String sql = "SELECT table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative " +
                     "FROM ducklake_table " +
                     "WHERE schema_id = ? AND table_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, schema.get().schemaId());
            stmt.setString(2, tableName);
            stmt.setLong(3, snapshotId);
            stmt.setLong(4, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new DucklakeTable(
                            rs.getLong("table_id"),
                            UUID.fromString(rs.getString("table_uuid")),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getLong("schema_id"),
                            rs.getString("table_name"),
                            getStringOptional(rs, "path"),
                            getBooleanOptional(rs, "path_is_relative")));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get table: " + tableName + " at snapshot: " + snapshotId, e);
        }
    }

    @Override
    public Optional<DucklakeTable> getTableById(long tableId, long snapshotId)
    {
        String sql = "SELECT table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative " +
                     "FROM ducklake_table " +
                     "WHERE table_id = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new DucklakeTable(
                            rs.getLong("table_id"),
                            UUID.fromString(rs.getString("table_uuid")),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getLong("schema_id"),
                            rs.getString("table_name"),
                            getStringOptional(rs, "path"),
                            getBooleanOptional(rs, "path_is_relative")));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get table by ID: " + tableId + " at snapshot: " + snapshotId, e);
        }
    }

    @Override
    public List<DucklakeColumn> getTableColumns(long tableId, long snapshotId)
    {
        String sql = "SELECT column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type, nulls_allowed, parent_column " +
                     "FROM ducklake_column " +
                     "WHERE table_id = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL) " +
                     "ORDER BY column_order, column_id";

        List<DucklakeColumn> allColumns = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    allColumns.add(new DucklakeColumn(
                            rs.getLong("column_id"),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getLong("table_id"),
                            rs.getLong("column_order"),
                            rs.getString("column_name"),
                            rs.getString("column_type"),
                            rs.getBoolean("nulls_allowed"),
                            getLongOptional(rs, "parent_column")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get columns for table: " + tableId + " at snapshot: " + snapshotId, e);
        }

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
        String sql = "SELECT column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type, nulls_allowed, parent_column " +
                     "FROM ducklake_column " +
                     "WHERE table_id = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL) " +
                     "ORDER BY column_order, column_id";

        List<DucklakeColumn> allColumns = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    allColumns.add(new DucklakeColumn(
                            rs.getLong("column_id"),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getLong("table_id"),
                            rs.getLong("column_order"),
                            rs.getString("column_name"),
                            rs.getString("column_type"),
                            rs.getBoolean("nulls_allowed"),
                            getLongOptional(rs, "parent_column")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get all columns for table: " + tableId + " at snapshot: " + snapshotId, e);
        }

        return allColumns;
    }

    @Override
    public List<DucklakeDataFile> getDataFiles(long tableId, long snapshotId)
    {
        String sql = "SELECT data.data_file_id, data.table_id, data.begin_snapshot, data.end_snapshot, data.file_order, " +
                     "       data.path, data.path_is_relative, data.file_format, data.record_count, data.file_size_bytes, " +
                     "       data.footer_size, data.row_id_start, data.partition_id, " +
                     "       del.path AS delete_file_path, del.path_is_relative AS delete_path_is_relative " +
                     "FROM ducklake_data_file AS data " +
                     "LEFT JOIN ducklake_delete_file AS del ON data.data_file_id = del.data_file_id " +
                     "  AND ? >= del.begin_snapshot AND (? < del.end_snapshot OR del.end_snapshot IS NULL) " +
                     "WHERE data.table_id = ? AND ? >= data.begin_snapshot AND (? < data.end_snapshot OR data.end_snapshot IS NULL) " +
                     "ORDER BY data.file_order";

        List<DucklakeDataFile> files = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, snapshotId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, tableId);
            stmt.setLong(4, snapshotId);
            stmt.setLong(5, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    files.add(new DucklakeDataFile(
                            rs.getLong("data_file_id"),
                            rs.getLong("table_id"),
                            rs.getLong("begin_snapshot"),
                            getLongOptional(rs, "end_snapshot"),
                            rs.getLong("file_order"),
                            rs.getString("path"),
                            rs.getBoolean("path_is_relative"),
                            rs.getString("file_format"),
                            rs.getLong("record_count"),
                            rs.getLong("file_size_bytes"),
                            rs.getLong("footer_size"),
                            rs.getLong("row_id_start"),
                            getLongOptional(rs, "partition_id"),
                            getStringOptional(rs, "delete_file_path"),
                            getBooleanOptional(rs, "delete_path_is_relative")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get data files for table: " + tableId + " at snapshot: " + snapshotId, e);
        }

        return files;
    }

    @Override
    public List<Long> findDataFileIdsInRange(long tableId, long snapshotId, ColumnRangePredicate predicate)
    {
        long columnId = predicate.columnId();
        String minValue = predicate.minValue();
        String maxValue = predicate.maxValue();

        Optional<String> columnType = getColumnType(tableId, columnId, snapshotId);
        if (columnType.isEmpty()) {
            return List.of();
        }

        String sql = "SELECT stats.data_file_id, stats.min_value, stats.max_value " +
                     "FROM ducklake_file_column_stats AS stats " +
                     "JOIN ducklake_data_file AS data ON stats.data_file_id = data.data_file_id " +
                     "WHERE stats.table_id = ? AND stats.column_id = ? " +
                     "  AND data.table_id = ? " +
                     "  AND ? >= data.begin_snapshot AND (? < data.end_snapshot OR data.end_snapshot IS NULL)";

        List<Long> fileIds = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, columnId);
            stmt.setLong(3, tableId);
            stmt.setLong(4, snapshotId);
            stmt.setLong(5, snapshotId);

            Comparable<?> lowerBound = parseStatValue(columnType.get(), minValue);
            Comparable<?> upperBound = parseStatValue(columnType.get(), maxValue);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Comparable<?> minStat = parseStatValue(columnType.get(), rs.getString("min_value"));
                    Comparable<?> maxStat = parseStatValue(columnType.get(), rs.getString("max_value"));

                    if (isWithinBounds(lowerBound, upperBound, minStat, maxStat)) {
                        fileIds.add(rs.getLong("data_file_id"));
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get file IDs for predicate on table: " + tableId + ", column: " + columnId, e);
        }

        return fileIds;
    }

    @Override
    public Optional<DucklakeTableStats> getTableStats(long tableId)
    {
        String sql = "SELECT table_id, record_count, file_size_bytes FROM ducklake_table_stats WHERE table_id = ?";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new DucklakeTableStats(
                            rs.getLong("table_id"),
                            rs.getLong("record_count"),
                            rs.getLong("file_size_bytes")));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get table stats for table: " + tableId, e);
        }
    }

    @Override
    public List<DucklakeColumnStats> getColumnStats(long tableId, long snapshotId)
    {
        // Resolve column types internally for typed min/max comparison
        Map<Long, String> columnTypes = getTableColumns(tableId, snapshotId).stream()
                .collect(Collectors.toMap(DucklakeColumn::columnId, DucklakeColumn::columnType));

        // Fetch per-file stats and aggregate in Java with typed min/max comparison
        String sql = "SELECT stats.column_id, stats.value_count, stats.null_count, " +
                     "       stats.column_size_bytes, stats.min_value, stats.max_value " +
                     "FROM ducklake_file_column_stats AS stats " +
                     "JOIN ducklake_data_file AS data ON stats.data_file_id = data.data_file_id " +
                     "WHERE stats.table_id = ? AND data.table_id = ? " +
                     "  AND ? >= data.begin_snapshot AND (? < data.end_snapshot OR data.end_snapshot IS NULL)";

        // Accumulate per-column aggregates
        Map<Long, long[]> countAccumulators = new HashMap<>(); // [valueCount, nullCount, sizeBytes]
        Map<Long, String> minAccumulators = new HashMap<>();
        Map<Long, String> maxAccumulators = new HashMap<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, tableId);
            stmt.setLong(3, snapshotId);
            stmt.setLong(4, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    long columnId = rs.getLong("column_id");
                    long valueCount = rs.getLong("value_count");
                    long nullCount = rs.getLong("null_count");
                    long sizeBytes = rs.getLong("column_size_bytes");
                    String minValue = rs.getString("min_value");
                    String maxValue = rs.getString("max_value");

                    countAccumulators.computeIfAbsent(columnId, _ -> new long[3]);
                    long[] counts = countAccumulators.get(columnId);
                    counts[0] += valueCount;
                    counts[1] += nullCount;
                    counts[2] += sizeBytes;

                    String columnType = columnTypes.getOrDefault(columnId, "");
                    if (minValue != null) {
                        minAccumulators.merge(columnId, minValue, (a, b) -> typedMin(a, b, columnType));
                    }
                    if (maxValue != null) {
                        maxAccumulators.merge(columnId, maxValue, (a, b) -> typedMax(a, b, columnType));
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get column stats for table: " + tableId + " at snapshot: " + snapshotId, e);
        }

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
        String sql = "SELECT pi.partition_id, pi.table_id, " +
                     "       pc.partition_key_index, pc.column_id, pc.transform " +
                     "FROM ducklake_partition_info pi " +
                     "JOIN ducklake_partition_column pc ON pi.partition_id = pc.partition_id AND pi.table_id = pc.table_id " +
                     "WHERE pi.table_id = ? " +
                     "  AND ? >= pi.begin_snapshot AND (? < pi.end_snapshot OR pi.end_snapshot IS NULL) " +
                     "ORDER BY pi.partition_id, pc.partition_key_index";

        Map<Long, List<DucklakePartitionField>> fieldsByPartition = new LinkedHashMap<>();
        Map<Long, Long> tableIdByPartition = new HashMap<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    long partitionId = rs.getLong("partition_id");
                    tableIdByPartition.put(partitionId, rs.getLong("table_id"));
                    fieldsByPartition.computeIfAbsent(partitionId, _ -> new ArrayList<>())
                            .add(new DucklakePartitionField(
                                    rs.getInt("partition_key_index"),
                                    rs.getLong("column_id"),
                                    DucklakePartitionTransform.fromString(rs.getString("transform"))));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get partition specs for table: " + tableId + " at snapshot: " + snapshotId, e);
        }

        List<DucklakePartitionSpec> specs = new ArrayList<>();
        for (Map.Entry<Long, List<DucklakePartitionField>> entry : fieldsByPartition.entrySet()) {
            specs.add(new DucklakePartitionSpec(entry.getKey(), tableIdByPartition.get(entry.getKey()), entry.getValue()));
        }
        return specs;
    }

    @Override
    public Map<Long, List<DucklakeFilePartitionValue>> getFilePartitionValues(long tableId, long snapshotId)
    {
        String sql = "SELECT fpv.data_file_id, fpv.partition_key_index, fpv.partition_value " +
                     "FROM ducklake_file_partition_value fpv " +
                     "JOIN ducklake_data_file df ON fpv.data_file_id = df.data_file_id AND fpv.table_id = df.table_id " +
                     "WHERE fpv.table_id = ? " +
                     "  AND ? >= df.begin_snapshot AND (? < df.end_snapshot OR df.end_snapshot IS NULL)";

        Map<Long, List<DucklakeFilePartitionValue>> result = new HashMap<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    long dataFileId = rs.getLong("data_file_id");
                    result.computeIfAbsent(dataFileId, _ -> new ArrayList<>())
                            .add(new DucklakeFilePartitionValue(
                                    dataFileId,
                                    rs.getInt("partition_key_index"),
                                    rs.getString("partition_value")));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get file partition values for table: " + tableId + " at snapshot: " + snapshotId, e);
        }

        return result;
    }

    @Override
    public List<DucklakeInlinedDataInfo> getInlinedDataInfos(long tableId, long snapshotId)
    {
        // A table can have multiple inlined data tables (one per schema version).
        String sql = "SELECT table_id, table_name, schema_version " +
                "FROM ducklake_inlined_data_tables " +
                "WHERE table_id = ? " +
                "  AND schema_version <= (SELECT schema_version FROM ducklake_snapshot WHERE snapshot_id = ?) " +
                "ORDER BY schema_version";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                List<DucklakeInlinedDataInfo> infos = new ArrayList<>();
                while (rs.next()) {
                    long resolvedTableId = rs.getLong("table_id");
                    long schemaVersion = rs.getLong("schema_version");
                    String inlinedTableName = String.format("ducklake_inlined_data_%d_%d", resolvedTableId, schemaVersion);
                    try (PreparedStatement verifyStmt = conn.prepareStatement("SELECT 1 FROM " + inlinedTableName + " WHERE 1 = 0")) {
                        verifyStmt.executeQuery();
                    }
                    catch (SQLException e) {
                        // Catalog metadata can point to a dropped/non-materialized inlined table.
                        // Treat this as "no inlined data" so scan planning does not emit a dead split.
                        log.log(System.Logger.Level.DEBUG, "Inlined data table {0} not available for table {1}: {2}", inlinedTableName, tableId, e.getMessage());
                        continue;
                    }

                    infos.add(new DucklakeInlinedDataInfo(
                            resolvedTableId,
                            rs.getString("table_name"),
                            schemaVersion));
                }
                return infos;
            }
        }
        catch (SQLException e) {
            // ducklake_inlined_data_tables may not exist in catalogs that never used inlining
            log.log(System.Logger.Level.DEBUG, "Could not query inlined data tables (table may not exist): {0}", e.getMessage());
            return List.of();
        }
    }

    @Override
    public boolean hasInlinedRows(long tableId, long schemaVersion, long snapshotId)
    {
        String inlinedTableName = String.format("ducklake_inlined_data_%d_%d", tableId, schemaVersion);
        String sql = "SELECT 1 FROM " + quoteIdentifier(inlinedTableName) +
                " WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL) LIMIT 1";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, snapshotId);
            stmt.setLong(2, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
        catch (SQLException e) {
            log.log(System.Logger.Level.DEBUG, "Could not probe inlined data rows from {0} (table may not exist): {1}", inlinedTableName, e.getMessage());
            return false;
        }
    }

    @Override
    public List<List<Object>> readInlinedData(long tableId, long schemaVersion, long snapshotId, List<DucklakeColumn> columns)
    {
        if (columns.isEmpty()) {
            return List.of();
        }

        String inlinedTableName = String.format("ducklake_inlined_data_%d_%d", tableId, schemaVersion);

        List<List<Object>> rows = new ArrayList<>();

        try (Connection conn = dataSource.getConnection()) {
            OptionalLong sourceSchemaSnapshot = getSnapshotIdForSchemaVersion(conn, tableId, schemaVersion, snapshotId);
            if (sourceSchemaSnapshot.isEmpty()) {
                return List.of();
            }

            Map<Long, DucklakeColumn> sourceColumnsById = getTableColumns(tableId, sourceSchemaSnapshot.getAsLong()).stream()
                    .collect(Collectors.toMap(DucklakeColumn::columnId, column -> column));

            List<String> projectedExpressions = new ArrayList<>(columns.size());
            for (int index = 0; index < columns.size(); index++) {
                DucklakeColumn requestedColumn = columns.get(index);
                String alias = "c" + index;
                DucklakeColumn sourceColumn = sourceColumnsById.get(requestedColumn.columnId());
                if (sourceColumn == null) {
                    projectedExpressions.add("NULL AS " + quoteIdentifier(alias));
                    continue;
                }
                projectedExpressions.add(quoteIdentifier(sourceColumn.columnName()) + " AS " + quoteIdentifier(alias));
            }

            String sql = String.format(
                    "SELECT %s FROM %s WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL) ORDER BY row_id",
                    String.join(", ", projectedExpressions),
                    quoteIdentifier(inlinedTableName));
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, snapshotId);
                stmt.setLong(2, snapshotId);

                try (ResultSet rs = stmt.executeQuery()) {
                    int columnCount = columns.size();
                    while (rs.next()) {
                        List<Object> row = new ArrayList<>(columnCount);
                        for (int i = 1; i <= columnCount; i++) {
                            row.add(rs.getObject(i));
                        }
                        rows.add(row);
                    }
                }
            }
        }
        catch (SQLException e) {
            // The inlined data table may not exist if the table was created but never had data inserted,
            // or if the inlined data was flushed to Parquet files. Return empty in these cases.
            log.log(System.Logger.Level.DEBUG, "Could not read inlined data from {0} (table may not exist): {1}", inlinedTableName, e.getMessage());
            return List.of();
        }

        return rows;
    }

    private OptionalLong getSnapshotIdForSchemaVersion(Connection conn, long tableId, long schemaVersion, long snapshotId)
            throws SQLException
    {
        // Prefer table-scoped schema version rows when available.
        // Some catalogs include ducklake_schema_versions.table_id (DuckDB behavior),
        // which gives an unambiguous snapshot where this table's schema version was introduced.
        String tableScopedSql = "SELECT begin_snapshot FROM ducklake_schema_versions " +
                "WHERE table_id = ? AND schema_version = ? AND begin_snapshot <= ? " +
                "ORDER BY begin_snapshot DESC LIMIT 1";
        try (PreparedStatement stmt = conn.prepareStatement(tableScopedSql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, schemaVersion);
            stmt.setLong(3, snapshotId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return OptionalLong.of(rs.getLong("begin_snapshot"));
                }
            }
        }
        catch (SQLException e) {
            // Fallback for catalogs without table_id in ducklake_schema_versions or older metadata.
            log.log(System.Logger.Level.DEBUG, "Could not resolve schema version via ducklake_schema_versions for table {0}: {1}", tableId, e.getMessage());
        }

        // Backward-compatible fallback: resolve by snapshot.schema_version only.
        String fallbackSql = "SELECT snapshot_id FROM ducklake_snapshot " +
                "WHERE schema_version = ? AND snapshot_id <= ? ORDER BY snapshot_id DESC LIMIT 1";
        try (PreparedStatement stmt = conn.prepareStatement(fallbackSql)) {
            stmt.setLong(1, schemaVersion);
            stmt.setLong(2, snapshotId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return OptionalLong.of(rs.getLong("snapshot_id"));
                }
                return OptionalLong.empty();
            }
        }
    }

    private static String quoteIdentifier(String identifier)
    {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public Optional<String> getDataPath()
    {
        String sql = "SELECT value FROM ducklake_metadata WHERE key = 'data_path'";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return Optional.of(rs.getString("value"));
            }
            return Optional.empty();
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get data path from ducklake_metadata", e);
        }
    }

    // ==================== View operations ====================

    @Override
    public List<DucklakeView> listViews(long schemaId, long snapshotId)
    {
        String sql = "SELECT view_id, view_uuid, begin_snapshot, end_snapshot, schema_id, view_name, dialect, sql, column_aliases " +
                     "FROM ducklake_view " +
                     "WHERE schema_id = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        List<DucklakeView> views = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, schemaId);
            stmt.setLong(2, snapshotId);
            stmt.setLong(3, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    views.add(readView(rs));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list views for schema: " + schemaId + " at snapshot: " + snapshotId, e);
        }

        return views;
    }

    @Override
    public Optional<DucklakeView> getView(String schemaName, String viewName, long snapshotId)
    {
        Optional<DucklakeSchema> schema = getSchema(schemaName, snapshotId);
        if (schema.isEmpty()) {
            return Optional.empty();
        }

        String sql = "SELECT view_id, view_uuid, begin_snapshot, end_snapshot, schema_id, view_name, dialect, sql, column_aliases " +
                     "FROM ducklake_view " +
                     "WHERE schema_id = ? AND view_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, schema.get().schemaId());
            stmt.setString(2, viewName);
            stmt.setLong(3, snapshotId);
            stmt.setLong(4, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(readView(rs));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get view: " + schemaName + "." + viewName + " at snapshot: " + snapshotId, e);
        }
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
            long baseSnapshotId = -1;
            try {
                // 1. Read current snapshot state
                long currentSnapshotId;
                long schemaVersion;
                long nextCatalogId;
                long nextFileId;
                try (PreparedStatement stmt = conn.prepareStatement(
                        "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id " +
                                "FROM ducklake_snapshot WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot)")) {
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (!rs.next()) {
                            throw new IllegalStateException("No snapshots found");
                        }
                        currentSnapshotId = rs.getLong("snapshot_id");
                        baseSnapshotId = currentSnapshotId;
                        schemaVersion = rs.getLong("schema_version");
                        nextCatalogId = rs.getLong("next_catalog_id");
                        nextFileId = rs.getLong("next_file_id");
                    }
                }

                // 2. Execute the caller's mutations
                DucklakeWriteTransaction tx = new DucklakeWriteTransaction(
                        conn, currentSnapshotId, schemaVersion, nextCatalogId, nextFileId);
                action.execute(tx);

                // 3. Strict optimistic conflict check: if snapshot lineage advanced, abort.
                ensureSnapshotLineageUnchanged(conn, tx.getCurrentSnapshotId(), operationDescription);

                // 4. Create new snapshot row (with final allocated IDs)
                insertSnapshotRow(conn, tx, operationDescription);

                // 5. Insert schema_versions row if schema version changed
                if (tx.getSchemaVersion() != schemaVersion) {
                    try (PreparedStatement stmt = conn.prepareStatement(
                            "INSERT INTO ducklake_schema_versions (begin_snapshot, schema_version, table_id) VALUES (?, ?, ?)")) {
                        stmt.setLong(1, tx.getNewSnapshotId());
                        stmt.setLong(2, tx.getSchemaVersion());
                        if (tx.getSchemaVersionTableId() >= 0) {
                            stmt.setLong(3, tx.getSchemaVersionTableId());
                        }
                        else {
                            stmt.setNull(3, java.sql.Types.BIGINT);
                        }
                        stmt.executeUpdate();
                    }
                }

                // 6. Insert snapshot changes (comma-separated per spec, one row per snapshot)
                if (!tx.getChanges().isEmpty()) {
                    String changesMade = String.join(",", tx.getChanges());
                    try (PreparedStatement stmt = conn.prepareStatement(
                            "INSERT INTO ducklake_snapshot_changes (snapshot_id, changes_made) VALUES (?, ?)")) {
                        stmt.setLong(1, tx.getNewSnapshotId());
                        stmt.setString(2, changesMade);
                        stmt.executeUpdate();
                    }
                }

                conn.commit();
            }
            catch (Exception e) {
                conn.rollback();
                if (hasTransactionConflict(e)) {
                    throw (RuntimeException) e;
                }
                if (isMetadataPrimaryKeyConflict(e)) {
                    long currentSnapshot = readLatestSnapshotId(conn);
                    throw transactionConflictException(conn, baseSnapshotId, currentSnapshot, operationDescription, e);
                }
                throw new RuntimeException("Failed to " + operationDescription, e);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to " + operationDescription, e);
        }
    }

    private void ensureSnapshotLineageUnchanged(Connection conn, long expectedSnapshotId, String operationDescription)
            throws SQLException
    {
        long currentSnapshotId = readLatestSnapshotId(conn);
        if (currentSnapshotId != expectedSnapshotId) {
            throw transactionConflictException(conn, expectedSnapshotId, currentSnapshotId, operationDescription, null);
        }
    }

    private void insertSnapshotRow(Connection conn, DucklakeWriteTransaction tx, String operationDescription)
            throws SQLException
    {
        try (PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) " +
                        "VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?)")) {
            stmt.setLong(1, tx.getNewSnapshotId());
            stmt.setLong(2, tx.getSchemaVersion());
            stmt.setLong(3, tx.getFinalNextCatalogId());
            stmt.setLong(4, tx.getFinalNextFileId());
            stmt.executeUpdate();
        }
        catch (SQLException e) {
            if (isDuplicateKeyViolation(e)) {
                long currentSnapshotId = readLatestSnapshotId(conn);
                throw transactionConflictException(conn, tx.getCurrentSnapshotId(), currentSnapshotId, operationDescription, e);
            }
            throw e;
        }
    }

    private TransactionConflictException transactionConflictException(
            Connection conn,
            long expectedSnapshotId,
            long currentSnapshotId,
            String operationDescription,
            Throwable cause)
            throws SQLException
    {
        String interveningChanges = getInterveningChangesSummary(conn, expectedSnapshotId, currentSnapshotId);
        String message = "Concurrent DuckLake commit while attempting to " + operationDescription +
                ": expected base snapshot " + expectedSnapshotId +
                ", but current snapshot is " + currentSnapshotId +
                ". Intervening changes: " + interveningChanges;
        return new TransactionConflictException(message, cause);
    }

    private String getInterveningChangesSummary(Connection conn, long fromSnapshotExclusive, long toSnapshotInclusive)
            throws SQLException
    {
        if (toSnapshotInclusive <= fromSnapshotExclusive) {
            return "none";
        }

        List<String> changes = new ArrayList<>();
        String sql = "SELECT snapshot_id, changes_made FROM ducklake_snapshot_changes " +
                "WHERE snapshot_id > ? AND snapshot_id <= ? " +
                "ORDER BY snapshot_id";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, fromSnapshotExclusive);
            stmt.setLong(2, toSnapshotInclusive);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    if (changes.size() >= CONFLICT_CHANGE_SUMMARY_LIMIT) {
                        break;
                    }
                    changes.add(rs.getLong("snapshot_id") + ":" + rs.getString("changes_made"));
                }
            }
        }

        if (changes.isEmpty()) {
            return "snapshot advanced without snapshot_changes rows";
        }
        return String.join("; ", changes);
    }

    private long readLatestSnapshotId(Connection conn)
            throws SQLException
    {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT snapshot_id FROM ducklake_snapshot " +
                        "WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot)");
                ResultSet rs = stmt.executeQuery()) {
            if (!rs.next()) {
                throw new IllegalStateException("No snapshots found");
            }
            return rs.getLong("snapshot_id");
        }
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
            tx.addChange("created_view:" + viewName);

            insertViewRow(tx, viewId, UUID.randomUUID().toString(), schemaId, viewName, dialect, viewSql, viewMetadata);
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
                    tx.addChange("renamed_view:" + sourceView.viewId());
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
        });
    }

    private ActiveViewRow resolveActiveViewRow(DucklakeWriteTransaction tx, long schemaId, String viewName)
            throws SQLException
    {
        try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                "SELECT view_id, view_uuid, schema_id, view_name, dialect, sql, column_aliases " +
                        "FROM ducklake_view " +
                        "WHERE schema_id = ? AND view_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)")) {
            stmt.setLong(1, schemaId);
            stmt.setString(2, viewName);
            stmt.setLong(3, tx.getCurrentSnapshotId());
            stmt.setLong(4, tx.getCurrentSnapshotId());
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("View not found: schema_id=" + schemaId + ", view_name=" + viewName);
                }

                return new ActiveViewRow(
                        rs.getLong("view_id"),
                        rs.getString("view_uuid"),
                        rs.getLong("schema_id"),
                        rs.getString("view_name"),
                        rs.getString("dialect"),
                        rs.getString("sql"),
                        getStringOptional(rs, "column_aliases"));
            }
        }
    }

    private void insertViewRow(
            DucklakeWriteTransaction tx,
            long viewId,
            String viewUuid,
            long schemaId,
            String viewName,
            String dialect,
            String viewSql,
            String viewMetadata)
            throws SQLException
    {
        try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                "INSERT INTO ducklake_view (view_id, view_uuid, begin_snapshot, end_snapshot, schema_id, view_name, dialect, sql, column_aliases) " +
                        "VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?)")) {
            stmt.setLong(1, viewId);
            setUuid(stmt, 2, viewUuid);
            stmt.setLong(3, tx.getNewSnapshotId());
            stmt.setLong(4, schemaId);
            stmt.setString(5, viewName);
            stmt.setString(6, dialect);
            stmt.setString(7, viewSql);
            setNullableString(stmt, 8, viewMetadata);
            stmt.executeUpdate();
        }
    }

    private void endSnapshotActiveView(DucklakeWriteTransaction tx, long viewId)
            throws SQLException
    {
        try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                "UPDATE ducklake_view SET end_snapshot = ? WHERE view_id = ? AND end_snapshot IS NULL")) {
            stmt.setLong(1, tx.getNewSnapshotId());
            stmt.setLong(2, viewId);
            int updatedRows = stmt.executeUpdate();
            if (updatedRows == 0) {
                throw new RuntimeException("View not found: " + viewId);
            }
        }
    }

    private static boolean hasActiveView(DucklakeWriteTransaction tx, long schemaId, String viewName)
            throws SQLException
    {
        try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                "SELECT 1 FROM ducklake_view " +
                        "WHERE schema_id = ? AND view_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL) LIMIT 1")) {
            stmt.setLong(1, schemaId);
            stmt.setString(2, viewName);
            stmt.setLong(3, tx.getCurrentSnapshotId());
            stmt.setLong(4, tx.getCurrentSnapshotId());
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static boolean hasActiveTable(DucklakeWriteTransaction tx, long schemaId, String tableName)
            throws SQLException
    {
        try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                "SELECT 1 FROM ducklake_table " +
                        "WHERE schema_id = ? AND table_name = ? AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL) LIMIT 1")) {
            stmt.setLong(1, schemaId);
            stmt.setString(2, tableName);
            stmt.setLong(3, tx.getCurrentSnapshotId());
            stmt.setLong(4, tx.getCurrentSnapshotId());
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    private record ActiveViewRow(
            long viewId,
            String viewUuid,
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
            tx.addChange("created_schema:" + schemaName);

            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "INSERT INTO ducklake_schema (schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative) " +
                            "VALUES (?, ?, ?, NULL, ?, ?, true)")) {
                stmt.setLong(1, schemaId);
                setUuid(stmt, 2, UUID.randomUUID().toString());
                stmt.setLong(3, tx.getNewSnapshotId());
                stmt.setString(4, schemaName);
                stmt.setString(5, schemaName + "/");
                stmt.executeUpdate();
            }
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

            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "UPDATE ducklake_schema SET end_snapshot = ? WHERE schema_id = ? AND end_snapshot IS NULL")) {
                stmt.setLong(1, tx.getNewSnapshotId());
                stmt.setLong(2, schemaId);
                stmt.executeUpdate();
            }
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

            // 1. Insert table row
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "INSERT INTO ducklake_table (table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative) " +
                            "VALUES (?, ?, ?, NULL, ?, ?, ?, true)")) {
                stmt.setLong(1, tableId);
                setUuid(stmt, 2, UUID.randomUUID().toString());
                stmt.setLong(3, tx.getNewSnapshotId());
                stmt.setLong(4, schemaId);
                stmt.setString(5, tableName);
                stmt.setString(6, tableName + "/");
                stmt.executeUpdate();
            }

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

                try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                        "INSERT INTO ducklake_partition_info (partition_id, table_id, begin_snapshot, end_snapshot) " +
                                "VALUES (?, ?, ?, NULL)")) {
                    stmt.setLong(1, partitionId);
                    stmt.setLong(2, tableId);
                    stmt.setLong(3, tx.getNewSnapshotId());
                    stmt.executeUpdate();
                }

                int keyIndex = 0;
                for (PartitionFieldSpec field : partitionSpec.get()) {
                    Long columnId = topLevelColumnIds.get(field.columnName());
                    if (columnId == null) {
                        throw new RuntimeException("Partition column not found: " + field.columnName());
                    }
                    try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                            "INSERT INTO ducklake_partition_column (partition_id, table_id, partition_key_index, column_id, transform) " +
                                    "VALUES (?, ?, ?, ?, ?)")) {
                        stmt.setLong(1, partitionId);
                        stmt.setLong(2, tableId);
                        stmt.setLong(3, keyIndex++);
                        stmt.setLong(4, columnId);
                        stmt.setString(5, field.transform().name().toLowerCase(java.util.Locale.ENGLISH));
                        stmt.executeUpdate();
                    }
                }
            }

            tx.incrementSchemaVersion(tableId);
            tx.addChange("created_table:" + tableName);
        });
    }

    /**
     * Recursively inserts a column and its children into ducklake_column.
     * Returns the column_id of the inserted column.
     */
    private long insertColumnTree(DucklakeWriteTransaction tx, long tableId,
            TableColumnSpec column, long columnOrder, OptionalLong parentColumnId)
            throws SQLException
    {
        long columnId = tx.allocateCatalogId();

        try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                "INSERT INTO ducklake_column (column_id, begin_snapshot, end_snapshot, table_id, column_order, " +
                        "column_name, column_type, initial_default, default_value, nulls_allowed, parent_column, " +
                        "default_value_type, default_value_dialect) " +
                        "VALUES (?, ?, NULL, ?, ?, ?, ?, NULL, 'NULL', ?, ?, 'literal', 'duckdb')")) {
            stmt.setLong(1, columnId);
            stmt.setLong(2, tx.getNewSnapshotId());
            stmt.setLong(3, tableId);
            stmt.setLong(4, columnOrder);
            stmt.setString(5, column.name());
            stmt.setString(6, column.ducklakeType());
            stmt.setBoolean(7, column.nullable());
            if (parentColumnId.isPresent()) {
                stmt.setLong(8, parentColumnId.getAsLong());
            }
            else {
                stmt.setNull(8, java.sql.Types.BIGINT);
            }
            stmt.executeUpdate();
        }

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

            // End-snapshot the table row
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "UPDATE ducklake_table SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL")) {
                stmt.setLong(1, tx.getNewSnapshotId());
                stmt.setLong(2, tableId);
                stmt.executeUpdate();
            }

            // End-snapshot all active columns
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "UPDATE ducklake_column SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL")) {
                stmt.setLong(1, tx.getNewSnapshotId());
                stmt.setLong(2, tableId);
                stmt.executeUpdate();
            }

            // End-snapshot all active data files
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "UPDATE ducklake_data_file SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL")) {
                stmt.setLong(1, tx.getNewSnapshotId());
                stmt.setLong(2, tableId);
                stmt.executeUpdate();
            }

            // End-snapshot all active delete files (via data_file join)
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "UPDATE ducklake_delete_file SET end_snapshot = ? " +
                            "WHERE data_file_id IN (SELECT data_file_id FROM ducklake_data_file WHERE table_id = ?) " +
                            "AND end_snapshot IS NULL")) {
                stmt.setLong(1, tx.getNewSnapshotId());
                stmt.setLong(2, tableId);
                stmt.executeUpdate();
            }

            // End-snapshot partition info
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "UPDATE ducklake_partition_info SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL")) {
                stmt.setLong(1, tx.getNewSnapshotId());
                stmt.setLong(2, tableId);
                stmt.executeUpdate();
            }

            tx.incrementSchemaVersion(tableId);
            tx.addChange("dropped_table:" + tableId);
        });
    }

    @Override
    public void addColumn(long tableId, TableColumnSpec column)
    {
        executeWriteTransaction("add column to table " + tableId, tx -> {
            // Find the current max column_order for top-level columns
            long maxOrder = 0;
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "SELECT COALESCE(MAX(column_order), 0) FROM ducklake_column " +
                            "WHERE table_id = ? AND parent_column IS NULL " +
                            "AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)")) {
                stmt.setLong(1, tableId);
                stmt.setLong(2, tx.getCurrentSnapshotId());
                stmt.setLong(3, tx.getCurrentSnapshotId());
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        maxOrder = rs.getLong(1);
                    }
                }
            }

            insertColumnTree(tx, tableId, column, maxOrder + 1, OptionalLong.empty());
            tx.incrementSchemaVersion(tableId);
            tx.addChange("altered_table:" + tableId);
        });
    }

    @Override
    public void dropColumn(long tableId, long columnId)
    {
        executeWriteTransaction("drop column from table " + tableId, tx -> {
            // End-snapshot the column and all its children (for nested types)
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "UPDATE ducklake_column SET end_snapshot = ? " +
                            "WHERE table_id = ? AND (column_id = ? OR parent_column = ?) AND end_snapshot IS NULL")) {
                stmt.setLong(1, tx.getNewSnapshotId());
                stmt.setLong(2, tableId);
                stmt.setLong(3, columnId);
                stmt.setLong(4, columnId);
                stmt.executeUpdate();
            }

            tx.incrementSchemaVersion(tableId);
            tx.addChange("altered_table:" + tableId);
        });
    }

    @Override
    public void renameColumn(long tableId, long columnId, String newName)
    {
        executeWriteTransaction("rename column in table " + tableId, tx -> {
            // Read the current column metadata
            long columnOrder;
            String columnType;
            boolean nullsAllowed;
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "SELECT column_order, column_type, nulls_allowed FROM ducklake_column " +
                            "WHERE table_id = ? AND column_id = ? " +
                            "AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)")) {
                stmt.setLong(1, tableId);
                stmt.setLong(2, columnId);
                stmt.setLong(3, tx.getCurrentSnapshotId());
                stmt.setLong(4, tx.getCurrentSnapshotId());
                try (ResultSet rs = stmt.executeQuery()) {
                    if (!rs.next()) {
                        throw new RuntimeException("Column not found: " + columnId);
                    }
                    columnOrder = rs.getLong("column_order");
                    columnType = rs.getString("column_type");
                    nullsAllowed = rs.getBoolean("nulls_allowed");
                }
            }

            // End-snapshot the current version
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "UPDATE ducklake_column SET end_snapshot = ? " +
                            "WHERE table_id = ? AND column_id = ? AND end_snapshot IS NULL AND parent_column IS NULL")) {
                stmt.setLong(1, tx.getNewSnapshotId());
                stmt.setLong(2, tableId);
                stmt.setLong(3, columnId);
                stmt.executeUpdate();
            }

            // Insert new version with same column_id but new name
            try (PreparedStatement stmt = tx.getConnection().prepareStatement(
                    "INSERT INTO ducklake_column (column_id, begin_snapshot, end_snapshot, table_id, column_order, " +
                            "column_name, column_type, initial_default, default_value, nulls_allowed, parent_column, " +
                            "default_value_type, default_value_dialect) " +
                            "VALUES (?, ?, NULL, ?, ?, ?, ?, NULL, 'NULL', ?, NULL, 'literal', 'duckdb')")) {
                stmt.setLong(1, columnId);
                stmt.setLong(2, tx.getNewSnapshotId());
                stmt.setLong(3, tableId);
                stmt.setLong(4, columnOrder);
                stmt.setString(5, newName);
                stmt.setString(6, columnType);
                stmt.setBoolean(7, nullsAllowed);
                stmt.executeUpdate();
            }

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

    private DucklakeView readView(ResultSet rs)
            throws SQLException
    {
        long endSnapshotRaw = rs.getLong("end_snapshot");
        OptionalLong endSnapshot = rs.wasNull() ? OptionalLong.empty() : OptionalLong.of(endSnapshotRaw);

        return new DucklakeView(
                rs.getLong("view_id"),
                rs.getString("view_uuid"),
                rs.getLong("schema_id"),
                rs.getString("view_name"),
                rs.getString("sql"),
                rs.getString("dialect"),
                getStringOptional(rs, "column_aliases"),
                rs.getLong("begin_snapshot"),
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

    private DucklakeSnapshot readSnapshot(ResultSet rs)
            throws SQLException
    {
        return new DucklakeSnapshot(
                rs.getLong("snapshot_id"),
                parseSnapshotTime(rs.getString("snapshot_time")),
                rs.getLong("schema_version"),
                rs.getLong("next_catalog_id"),
                rs.getLong("next_file_id"));
    }

    private static Instant parseSnapshotTime(String snapshotTime)
    {
        if (snapshotTime == null) {
            throw new IllegalStateException("DuckLake snapshot_time is null");
        }

        String normalized = snapshotTime.trim().replace(' ', 'T');
        if (normalized.matches(".*[+-][0-9]{2}$")) {
            normalized = normalized + ":00";
        }
        if (normalized.matches(".*[+-][0-9]{4}$")) {
            normalized = normalized.substring(0, normalized.length() - 5)
                    + normalized.substring(normalized.length() - 5, normalized.length() - 2)
                    + ":"
                    + normalized.substring(normalized.length() - 2);
        }

        // SQLite CURRENT_TIMESTAMP produces no timezone offset (e.g. "2026-04-03T17:45:02");
        // DuckDB-generated snapshots include timezone. Handle both.
        if (!normalized.contains("+") && !normalized.contains("Z") && !normalized.matches(".*-[0-9]{2}:[0-9]{2}$")) {
            return java.time.LocalDateTime.parse(normalized).toInstant(java.time.ZoneOffset.UTC);
        }
        return java.time.OffsetDateTime.parse(normalized).toInstant();
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

    private Optional<String> getColumnType(long tableId, long columnId, long snapshotId)
    {
        String sql = "SELECT column_type " +
                     "FROM ducklake_column " +
                     "WHERE table_id = ? AND column_id = ? " +
                     "  AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)";

        try (Connection conn = dataSource.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, tableId);
            stmt.setLong(2, columnId);
            stmt.setLong(3, snapshotId);
            stmt.setLong(4, snapshotId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(rs.getString("column_type"));
                }
                return Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get column type for table: " + tableId + ", column: " + columnId, e);
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
