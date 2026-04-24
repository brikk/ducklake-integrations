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
package dev.brikk.ducklake.trino.plugin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import dev.brikk.ducklake.catalog.DucklakeCatalog;
import dev.brikk.ducklake.catalog.DucklakeColumn;
import dev.brikk.ducklake.catalog.DucklakeColumnStats;
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment;
import dev.brikk.ducklake.catalog.DucklakeWriteFragment;
import dev.brikk.ducklake.catalog.TransactionConflictException;
import dev.brikk.ducklake.catalog.DucklakeDataFile;
import dev.brikk.ducklake.catalog.DucklakeInlinedDataInfo;
import dev.brikk.ducklake.catalog.DucklakePartitionField;
import dev.brikk.ducklake.catalog.DucklakePartitionSpec;
import dev.brikk.ducklake.catalog.DucklakeSchema;
import dev.brikk.ducklake.catalog.DucklakeTable;
import dev.brikk.ducklake.catalog.DucklakeTableStats;
import dev.brikk.ducklake.catalog.DucklakeView;
import dev.brikk.ducklake.catalog.PartitionFieldSpec;
import dev.brikk.ducklake.catalog.TableColumnSpec;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.ViewNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.util.Objects.requireNonNull;

/**
 * Metadata implementation for Ducklake connector.
 * Provides access to Ducklake tables and views via SQL catalog.
 */
public class DucklakeMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(DucklakeMetadata.class);
    private static final String METADATA_TABLE_SEPARATOR = "$";
    private static final JsonCodec<ConnectorViewDefinition> VIEW_CODEC = new JsonCodecFactory().jsonCodec(ConnectorViewDefinition.class);
    // `trino/brikk` — base dialect is Trino SQL, `/brikk` marks that this row also carries
    // our plugin-specific JSON sidecar in `ducklake_view.column_aliases` (serialized
    // `ConnectorViewDefinition`). Other writers that understand plain Trino SQL should skip
    // rows with this suffix unless they want to cross-parse our metadata.
    private static final String TRINO_VIEW_DIALECT = "trino/brikk";

    private final DucklakeCatalog catalog;
    private final DucklakeTypeConverter typeConverter;
    private final DucklakeSnapshotResolver snapshotResolver;
    private final JsonCodec<DucklakeWriteFragment> fragmentCodec;
    private final JsonCodec<DucklakeDeleteFragment> deleteFragmentCodec;
    private final DucklakePathResolver pathResolver;
    private final DucklakeTemporalPartitionEncoding temporalPartitionEncoding;

    public DucklakeMetadata(DucklakeCatalog catalog, DucklakeTypeConverter typeConverter)
    {
        this(catalog, typeConverter, new DucklakeSnapshotResolver(catalog, OptionalLong.empty(), Optional.empty()), null, null, null, DucklakeTemporalPartitionEncoding.CALENDAR);
    }

    public DucklakeMetadata(DucklakeCatalog catalog, DucklakeTypeConverter typeConverter, DucklakeSnapshotResolver snapshotResolver)
    {
        this(catalog, typeConverter, snapshotResolver, null, null, null, DucklakeTemporalPartitionEncoding.CALENDAR);
    }

    public DucklakeMetadata(
            DucklakeCatalog catalog,
            DucklakeTypeConverter typeConverter,
            DucklakeSnapshotResolver snapshotResolver,
            JsonCodec<DucklakeWriteFragment> fragmentCodec,
            JsonCodec<DucklakeDeleteFragment> deleteFragmentCodec,
            DucklakePathResolver pathResolver,
            DucklakeTemporalPartitionEncoding temporalPartitionEncoding)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.typeConverter = requireNonNull(typeConverter, "typeConverter is null");
        this.snapshotResolver = requireNonNull(snapshotResolver, "snapshotResolver is null");
        this.fragmentCodec = fragmentCodec;
        this.deleteFragmentCodec = deleteFragmentCodec;
        this.pathResolver = pathResolver;
        this.temporalPartitionEncoding = requireNonNull(temporalPartitionEncoding, "temporalPartitionEncoding is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        long snapshotId = snapshotResolver.resolveSnapshotId(session);
        return catalog.listSchemas(snapshotId).stream()
                .map(DucklakeSchema::schemaName)
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(startVersion, "startVersion is null");
        requireNonNull(endVersion, "endVersion is null");

        if (startVersion.isPresent() && endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "DuckLake does not support version ranges; provide only one table version bound");
        }

        Optional<ConnectorTableVersion> queryVersion = endVersion.isPresent() ? endVersion : startVersion;
        OptionalLong querySnapshotId = OptionalLong.empty();
        Optional<Instant> querySnapshotTimestamp = Optional.empty();
        if (queryVersion.isPresent()) {
            ConnectorTableVersion version = queryVersion.get();
            switch (version.getPointerType()) {
                case TARGET_ID -> querySnapshotId = OptionalLong.of(getSnapshotIdFromVersion(version));
                case TEMPORAL -> querySnapshotTimestamp = Optional.of(getSnapshotTimestampFromVersion(session, version));
            }
        }

        Optional<MetadataTableName> metadataTable = parseMetadataTableName(tableName);
        SchemaTableName baseTableName = metadataTable
                .map(parsed -> new SchemaTableName(tableName.getSchemaName(), parsed.baseTableName()))
                .orElse(tableName);

        long snapshotId = snapshotResolver.resolveSnapshotId(session, querySnapshotId, querySnapshotTimestamp);

        Optional<DucklakeTable> table = catalog.getTable(baseTableName.getSchemaName(), baseTableName.getTableName(), snapshotId);
        if (table.isEmpty()) {
            return null;
        }

        if (metadataTable.isPresent()) {
            MetadataTableName parsed = metadataTable.get();
            return new DucklakeMetadataTableHandle(
                    tableName.getSchemaName(),
                    tableName.getTableName(),
                    parsed.baseTableName(),
                    table.get().tableId(),
                    snapshotId,
                    parsed.metadataTableType());
        }

        return new DucklakeTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.get().tableId(),
                snapshotId);
    }

    private static long getSnapshotIdFromVersion(ConnectorTableVersion version)
    {
        Type versionType = version.getVersionType();
        if (versionType == SMALLINT || versionType == TINYINT || versionType == INTEGER || versionType == BIGINT) {
            return ((Number) version.getVersion()).longValue();
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported type for table version: " + versionType.getDisplayName());
    }

    private static Instant getSnapshotTimestampFromVersion(ConnectorSession session, ConnectorTableVersion version)
    {
        Type versionType = version.getVersionType();
        if (versionType.equals(DATE)) {
            return LocalDate.ofEpochDay((Long) version.getVersion())
                    .atStartOfDay()
                    .atZone(session.getTimeZoneKey().getZoneId())
                    .toInstant();
        }
        if (versionType instanceof TimestampType timestampVersionType) {
            long epochMicrosUtc = timestampVersionType.isShort()
                    ? (long) version.getVersion()
                    : ((LongTimestamp) version.getVersion()).getEpochMicros();
            long epochSecondUtc = floorDiv(epochMicrosUtc, MICROSECONDS_PER_SECOND);
            int nanosOfSecond = (int) floorMod(epochMicrosUtc, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
            return LocalDateTime.ofEpochSecond(epochSecondUtc, nanosOfSecond, ZoneOffset.UTC)
                    .atZone(session.getTimeZoneKey().getZoneId())
                    .toInstant();
        }
        if (versionType instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            long epochMillis = timestampWithTimeZoneType.isShort()
                    ? unpackMillisUtc((long) version.getVersion())
                    : ((LongTimestampWithTimeZone) version.getVersion()).getEpochMillis();
            return Instant.ofEpochMilli(epochMillis);
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported type for temporal table version: " + versionType.getDisplayName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (tableHandle instanceof DucklakeMetadataTableHandle metadataTableHandle) {
            return new ConnectorTableMetadata(
                    metadataTableHandle.getSchemaTableName(),
                    getMetadataColumns(metadataTableHandle.metadataTableType()));
        }

        DucklakeTableHandle ducklakeTableHandle = (DucklakeTableHandle) tableHandle;

        List<DucklakeColumn> columns = catalog.getTableColumns(
                ducklakeTableHandle.tableId(),
                ducklakeTableHandle.snapshotId());

        List<ColumnMetadata> columnMetadata = columns.stream()
                .map(column -> ColumnMetadata.builder()
                        .setName(column.columnName())
                        .setType(typeConverter.toTrinoType(column.columnType()))
                        .setNullable(column.nullsAllowed())
                        .build())
                .collect(toImmutableList());

        return new ConnectorTableMetadata(
                ducklakeTableHandle.getSchemaTableName(),
                columnMetadata);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        long snapshotId = snapshotResolver.resolveSnapshotId(session);

        if (schemaName.isPresent()) {
            Optional<DucklakeSchema> schema = catalog.getSchema(schemaName.get(), snapshotId);
            if (schema.isEmpty()) {
                return ImmutableList.of();
            }

            return catalog.listTables(schema.get().schemaId(), snapshotId).stream()
                    .map(table -> new SchemaTableName(schemaName.get(), table.tableName()))
                    .collect(toImmutableList());
        }

        // List all tables across all schemas
        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        for (DucklakeSchema schema : catalog.listSchemas(snapshotId)) {
            for (DucklakeTable table : catalog.listTables(schema.schemaId(), snapshotId)) {
                tables.add(new SchemaTableName(schema.schemaName(), table.tableName()));
            }
        }
        return tables.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (tableHandle instanceof DucklakeMetadataTableHandle metadataTableHandle) {
            return toColumnHandles(getMetadataColumns(metadataTableHandle.metadataTableType()));
        }

        DucklakeTableHandle ducklakeTableHandle = (DucklakeTableHandle) tableHandle;

        List<DucklakeColumn> columns = catalog.getTableColumns(
                ducklakeTableHandle.tableId(),
                ducklakeTableHandle.snapshotId());

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (DucklakeColumn column : columns) {
            columnHandles.put(
                    column.columnName(),
                    new DucklakeColumnHandle(
                            column.columnId(),
                            column.columnName(),
                            typeConverter.toTrinoType(column.columnType()),
                            column.nullsAllowed()));
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        DucklakeColumnHandle ducklakeColumnHandle = (DucklakeColumnHandle) columnHandle;

        return ColumnMetadata.builder()
                .setName(ducklakeColumnHandle.columnName())
                .setType(ducklakeColumnHandle.columnType())
                .setNullable(ducklakeColumnHandle.nullable())
                .build();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (tableHandle instanceof DucklakeMetadataTableHandle) {
            return TableStatistics.empty();
        }

        DucklakeTableHandle table = (DucklakeTableHandle) tableHandle;
        List<DucklakeDataFile> dataFiles = catalog.getDataFiles(table.tableId(), table.snapshotId());
        boolean hasDeleteFiles = dataFiles.stream().anyMatch(dataFile -> dataFile.deleteFilePath().isPresent());
        if (hasDeleteFiles) {
            // Conservative mode: delete-file snapshots can make row-level table stats stale across engines.
            // Prefer unknown over wrong.
            return TableStatistics.empty();
        }

        boolean hasLiveInlinedRows = hasLiveInlinedRows(table);

        Optional<DucklakeTableStats> tableStats = catalog.getTableStats(table.tableId());
        long recordCount;
        if (tableStats.isPresent()) {
            recordCount = tableStats.get().recordCount();
        }
        else {
            OptionalLong fallbackRecordCount = getFallbackRecordCount(table);
            if (fallbackRecordCount.isEmpty()) {
                return TableStatistics.empty();
            }
            recordCount = fallbackRecordCount.getAsLong();
        }

        TableStatistics.Builder stats = TableStatistics.builder()
                .setRowCount(Estimate.of(recordCount));

        if (recordCount == 0) {
            return stats.build();
        }

        Map<String, ColumnHandle> columnHandles = getColumnHandles(session, tableHandle);

        if (hasLiveInlinedRows) {
            // Conservative mode: when mixed inlined rows are present, file-level column statistics
            // cover only the Parquet portion and can become misleading. Keep only row-count stats.
            return stats.build();
        }

        Set<Long> seenDataFileIds = new HashSet<>();
        long activeDataFileRowCount = 0;
        for (DucklakeDataFile dataFile : dataFiles) {
            if (seenDataFileIds.add(dataFile.dataFileId())) {
                activeDataFileRowCount += dataFile.recordCount();
            }
        }

        List<DucklakeColumnStats> columnStatsList = catalog.getColumnStats(table.tableId(), table.snapshotId());

        // Index column stats by column ID
        Map<Long, DucklakeColumnStats> statsById = columnStatsList.stream()
                .collect(toImmutableMap(DucklakeColumnStats::columnId, s -> s));

        for (ColumnHandle handle : columnHandles.values()) {
            DucklakeColumnHandle column = (DucklakeColumnHandle) handle;
            DucklakeColumnStats colStats = statsById.get(column.columnId());
            if (colStats == null) {
                continue;
            }

            ColumnStatistics.Builder colBuilder = ColumnStatistics.builder();

            long totalCount = colStats.totalValueCount() + colStats.totalNullCount();
            if (activeDataFileRowCount > 0 && totalCount != activeDataFileRowCount) {
                // If file-level stats for this column do not cover all active data-file rows
                // (e.g., column added via schema evolution), expose unknown instead of wrong.
                continue;
            }
            if (totalCount > 0) {
                colBuilder.setNullsFraction(Estimate.of((double) colStats.totalNullCount() / totalCount));
            }

            if (colStats.totalSizeBytes() > 0) {
                colBuilder.setDataSize(Estimate.of(colStats.totalSizeBytes()));
            }

            toDoubleRange(column.columnType(), colStats).ifPresent(colBuilder::setRange);

            stats.setColumnStatistics(column, colBuilder.build());
        }

        return stats.build();
    }

    private boolean hasLiveInlinedRows(DucklakeTableHandle table)
    {
        return catalog.getInlinedDataInfos(table.tableId(), table.snapshotId()).stream()
                .anyMatch(info -> catalog.hasInlinedRows(info.tableId(), info.schemaVersion(), table.snapshotId()));
    }

    private OptionalLong getFallbackRecordCount(DucklakeTableHandle table)
    {
        // Align with Iceberg/Delta behavior: if we can prove there is no data at this snapshot,
        // return row count 0 instead of unknown.
        if (!catalog.getDataFiles(table.tableId(), table.snapshotId()).isEmpty()) {
            // Data files exist but no table stats were found. Keep row count unknown.
            return OptionalLong.empty();
        }

        List<DucklakeInlinedDataInfo> inlinedInfos = catalog.getInlinedDataInfos(table.tableId(), table.snapshotId());
        if (inlinedInfos.isEmpty()) {
            return OptionalLong.of(0);
        }

        List<DucklakeColumn> tableColumns = catalog.getTableColumns(table.tableId(), table.snapshotId());
        if (tableColumns.isEmpty()) {
            return OptionalLong.of(0);
        }

        long inlinedRowCount = inlinedInfos.stream()
                .mapToLong(info -> catalog.readInlinedData(
                        info.tableId(),
                        info.schemaVersion(),
                        table.snapshotId(),
                        ImmutableList.of(tableColumns.getFirst())).size())
                .sum();
        return OptionalLong.of(inlinedRowCount);
    }

    private static Optional<DoubleRange> toDoubleRange(Type type, DucklakeColumnStats stats)
    {
        if (stats.minValue().isEmpty() || stats.maxValue().isEmpty()) {
            return Optional.empty();
        }

        try {
            String minStr = stats.minValue().get();
            String maxStr = stats.maxValue().get();

            if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
                return Optional.of(new DoubleRange(Double.parseDouble(minStr), Double.parseDouble(maxStr)));
            }
            if (type.equals(DOUBLE) || type.equals(REAL)) {
                return Optional.of(new DoubleRange(Double.parseDouble(minStr), Double.parseDouble(maxStr)));
            }
            if (type.equals(DATE)) {
                long minDays = java.time.LocalDate.parse(minStr).toEpochDay();
                long maxDays = java.time.LocalDate.parse(maxStr).toEpochDay();
                return Optional.of(new DoubleRange(minDays, maxDays));
            }
        }
        catch (RuntimeException _) {
            // If parsing fails, skip range
        }
        return Optional.empty();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint constraint)
    {
        if (handle instanceof DucklakeMetadataTableHandle) {
            return Optional.empty();
        }

        DucklakeTableHandle table = (DucklakeTableHandle) handle;

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll()) {
            return Optional.empty();
        }

        TupleDomain<DucklakeColumnHandle> newPredicate = extractDucklakePredicate(summary);

        // Classify predicates as enforced (partition-prunable) or unenforced (best-effort)
        List<DucklakePartitionSpec> partitionSpecs = catalog.getPartitionSpecs(
                table.tableId(), table.snapshotId());

        ImmutableMap.Builder<DucklakeColumnHandle, Domain> enforced = ImmutableMap.builder();
        ImmutableMap.Builder<DucklakeColumnHandle, Domain> unenforced = ImmutableMap.builder();

        if (!newPredicate.isNone()) {
            for (Map.Entry<DucklakeColumnHandle, Domain> entry : newPredicate.getDomains().orElse(Map.of()).entrySet()) {
                switch (classifyColumnConstraint(partitionSpecs, entry.getKey())) {
                    case FULLY_ENFORCED -> enforced.put(entry.getKey(), entry.getValue());
                    case PARTIALLY_ENFORCED -> {
                        // Keep in both predicates: connector can use partition transforms for pruning,
                        // but engine must still evaluate original predicate for correctness.
                        enforced.put(entry.getKey(), entry.getValue());
                        unenforced.put(entry.getKey(), entry.getValue());
                    }
                    case NOT_ENFORCED -> unenforced.put(entry.getKey(), entry.getValue());
                }
            }
        }

        TupleDomain<DucklakeColumnHandle> newEnforced = newPredicate.isNone()
                ? TupleDomain.none()
                : toTupleDomain(enforced.buildOrThrow());
        TupleDomain<DucklakeColumnHandle> newUnenforced = newPredicate.isNone()
                ? TupleDomain.all()
                : toTupleDomain(unenforced.buildOrThrow());

        TupleDomain<DucklakeColumnHandle> combinedEnforced = table.enforcedPredicate().intersect(newEnforced);
        TupleDomain<DucklakeColumnHandle> combinedUnenforced = table.unenforcedPredicate().intersect(newUnenforced);

        if (combinedEnforced.equals(table.enforcedPredicate())
                && combinedUnenforced.equals(table.unenforcedPredicate())) {
            return Optional.empty();
        }

        DucklakeTableHandle newHandle = new DucklakeTableHandle(
                table.schemaName(),
                table.tableName(),
                table.tableId(),
                table.snapshotId(),
                combinedUnenforced,
                combinedEnforced);

        // Fully enforced predicates are omitted from remaining filter.
        // Partially enforced predicates (e.g. temporal transforms) remain so engine verifies exact semantics.
        TupleDomain<ColumnHandle> remainingFilter = newUnenforced.transformKeys(ColumnHandle.class::cast);

        return Optional.of(new ConstraintApplicationResult<>(
                newHandle,
                remainingFilter,
                constraint.getExpression(),
                false));
    }

    private static ConstraintEnforcement classifyColumnConstraint(
            List<DucklakePartitionSpec> specs,
            DucklakeColumnHandle column)
    {
        if (specs.isEmpty()) {
            return ConstraintEnforcement.NOT_ENFORCED;
        }

        boolean fullyEnforced = true;
        // A predicate can only be enforced (fully or partially) if it is enforceable in EVERY active spec
        // (spec evolution means different files may have different partition schemes)
        for (DucklakePartitionSpec spec : specs) {
            Optional<DucklakePartitionField> field = spec.fields().stream()
                    .filter(partitionField -> partitionField.columnId() == column.columnId())
                    .findFirst();
            if (field.isEmpty()) {
                return ConstraintEnforcement.NOT_ENFORCED;
            }

            ConstraintEnforcement fieldEnforcement = classifyTransformEnforcement(field.get(), column);
            if (fieldEnforcement == ConstraintEnforcement.NOT_ENFORCED) {
                return ConstraintEnforcement.NOT_ENFORCED;
            }
            if (fieldEnforcement == ConstraintEnforcement.PARTIALLY_ENFORCED) {
                fullyEnforced = false;
            }
        }
        return fullyEnforced ? ConstraintEnforcement.FULLY_ENFORCED : ConstraintEnforcement.PARTIALLY_ENFORCED;
    }

    private static ConstraintEnforcement classifyTransformEnforcement(DucklakePartitionField field, DucklakeColumnHandle column)
    {
        if (field.transform().isIdentity()) {
            return ConstraintEnforcement.FULLY_ENFORCED;
        }
        if (field.transform().isTemporal()) {
            // Temporal transforms support safe partition pruning but do not fully enforce
            // original predicates (e.g. day equality with month transform).
            Type type = column.columnType();
            if (type.equals(DATE) || type.equals(TIMESTAMP_MILLIS) || type.equals(TIMESTAMP_MICROS) || type.equals(TIMESTAMP_TZ_MILLIS) || type.equals(TIMESTAMP_TZ_MICROS)) {
                return ConstraintEnforcement.PARTIALLY_ENFORCED;
            }
        }
        return ConstraintEnforcement.NOT_ENFORCED;
    }

    private enum ConstraintEnforcement
    {
        FULLY_ENFORCED,
        PARTIALLY_ENFORCED,
        NOT_ENFORCED
    }

    private static TupleDomain<DucklakeColumnHandle> extractDucklakePredicate(TupleDomain<ColumnHandle> summary)
    {
        if (summary.isNone()) {
            return TupleDomain.none();
        }

        Optional<Map<ColumnHandle, Domain>> domains = summary.getDomains();
        if (domains.isEmpty()) {
            return TupleDomain.all();
        }

        ImmutableMap.Builder<DucklakeColumnHandle, Domain> ducklakeDomains = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : domains.get().entrySet()) {
            if (entry.getKey() instanceof DucklakeColumnHandle columnHandle) {
                // Only push down primitive types (arrays/complex types can't be pruned)
                if (!columnHandle.columnType().getTypeParameters().isEmpty()) {
                    continue;
                }
                ducklakeDomains.put(columnHandle, entry.getValue());
            }
        }

        Map<DucklakeColumnHandle, Domain> result = ducklakeDomains.buildOrThrow();
        if (result.isEmpty()) {
            return TupleDomain.all();
        }
        return TupleDomain.withColumnDomains(result);
    }

    private static TupleDomain<DucklakeColumnHandle> toTupleDomain(Map<DucklakeColumnHandle, Domain> domains)
    {
        if (domains.isEmpty()) {
            return TupleDomain.all();
        }
        return TupleDomain.withColumnDomains(domains);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        long snapshotId = catalog.getCurrentSnapshotId();
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tables = prefix.getTable()
                .map(table -> List.of(prefix.toSchemaTableName()))
                .orElseGet(() -> listTables(session, prefix.getSchema()));

        for (SchemaTableName tableName : tables) {
            Optional<MetadataTableName> metadataTable = parseMetadataTableName(tableName);
            if (metadataTable.isPresent()) {
                SchemaTableName baseTable = new SchemaTableName(tableName.getSchemaName(), metadataTable.get().baseTableName());
                if (catalog.getTable(baseTable.getSchemaName(), baseTable.getTableName(), snapshotId).isPresent()) {
                    columns.put(tableName, getMetadataColumns(metadataTable.get().metadataTableType()));
                }
                continue;
            }

            Optional<DucklakeTable> table = catalog.getTable(tableName.getSchemaName(), tableName.getTableName(), snapshotId);
            if (table.isPresent()) {
                List<DucklakeColumn> tableColumns = catalog.getTableColumns(table.get().tableId(), snapshotId);
                columns.put(
                        tableName,
                        tableColumns.stream()
                                .map(column -> ColumnMetadata.builder()
                                        .setName(column.columnName())
                                        .setType(typeConverter.toTrinoType(column.columnType()))
                                        .setNullable(column.nullsAllowed())
                                        .build())
                                .collect(toImmutableList()));
            }
        }

        return columns.buildOrThrow();
    }

    private static Optional<MetadataTableName> parseMetadataTableName(SchemaTableName tableName)
    {
        String rawTableName = tableName.getTableName();
        int separator = rawTableName.lastIndexOf(METADATA_TABLE_SEPARATOR);
        if (separator <= 0 || separator == rawTableName.length() - 1) {
            return Optional.empty();
        }

        String baseName = rawTableName.substring(0, separator);
        String suffix = rawTableName.substring(separator + 1);
        return DucklakeMetadataTableType.fromSuffix(suffix)
                .map(type -> new MetadataTableName(baseName, type));
    }

    private static Map<String, ColumnHandle> toColumnHandles(List<ColumnMetadata> metadataColumns)
    {
        ImmutableMap.Builder<String, ColumnHandle> handles = ImmutableMap.builder();
        for (int index = 0; index < metadataColumns.size(); index++) {
            ColumnMetadata column = metadataColumns.get(index);
            handles.put(
                    column.getName(),
                    new DucklakeColumnHandle(
                            -(index + 1L),
                            column.getName(),
                            column.getType(),
                            column.isNullable()));
        }
        return handles.buildOrThrow();
    }

    private static List<ColumnMetadata> getMetadataColumns(DucklakeMetadataTableType metadataTableType)
    {
        return switch (metadataTableType) {
            case FILES -> ImmutableList.of(
                    column("data_file_id", BIGINT, false),
                    column("path", VARCHAR, false),
                    column("file_format", VARCHAR, false),
                    column("record_count", BIGINT, false),
                    column("file_size_bytes", BIGINT, false),
                    column("row_id_start", BIGINT, false),
                    column("partition_id", BIGINT, true),
                    column("delete_file_path", VARCHAR, true));
            case SNAPSHOTS -> snapshotColumns();
            case CURRENT_SNAPSHOT -> snapshotColumns();
            case SNAPSHOT_CHANGES -> ImmutableList.of(
                    column("snapshot_id", BIGINT, false),
                    column("changes_made", VARCHAR, true),
                    column("author", VARCHAR, true),
                    column("commit_message", VARCHAR, true),
                    column("commit_extra_info", VARCHAR, true));
        };
    }

    private static List<ColumnMetadata> snapshotColumns()
    {
        return ImmutableList.of(
                column("snapshot_id", BIGINT, false),
                column("snapshot_time", TIMESTAMP_TZ_MILLIS, false),
                column("schema_version", BIGINT, false),
                column("next_catalog_id", BIGINT, false),
                column("next_file_id", BIGINT, false));
    }

    private static ColumnMetadata column(String name, Type type, boolean nullable)
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(nullable)
                .build();
    }

    private record MetadataTableName(String baseTableName, DucklakeMetadataTableType metadataTableType) {}

    // ==================== Schema DDL ====================

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        translateCatalogExceptions(() -> catalog.createSchema(schemaName));
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        translateCatalogExceptions(() -> catalog.dropSchema(schemaName));
    }

    // ==================== Table DDL ====================

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        if (saveMode == SaveMode.REPLACE) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }

        SchemaTableName tableName = tableMetadata.getTable();

        // Convert columns to DuckLake column specs
        List<TableColumnSpec> columnSpecs = tableMetadata.getColumns().stream()
                .map(column -> toColumnSpec(column.getName(), column.getType(), column.isNullable()))
                .collect(toImmutableList());

        // Parse partition spec from table properties
        List<PartitionFieldSpec> partitionFields = DucklakeTableProperties.getPartitionFields(tableMetadata.getProperties());
        Optional<List<PartitionFieldSpec>> partitionSpec = partitionFields.isEmpty()
                ? Optional.empty()
                : Optional.of(partitionFields);

        translateCatalogExceptions(() -> catalog.createTable(tableName.getSchemaName(), tableName.getTableName(), columnSpecs, partitionSpec));
    }

    private TableColumnSpec toColumnSpec(String name, Type trinoType, boolean nullable)
    {
        String ducklakeType = typeConverter.toDucklakeType(trinoType);

        if (trinoType instanceof ArrayType arrayType) {
            List<TableColumnSpec> children = ImmutableList.of(
                    toColumnSpec("element", arrayType.getElementType(), true));
            return new TableColumnSpec(name, ducklakeType, nullable, children);
        }
        if (trinoType instanceof RowType rowType) {
            List<TableColumnSpec> children = rowType.getFields().stream()
                    .map(field -> toColumnSpec(
                            field.getName().orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Anonymous row fields not supported")),
                            field.getType(),
                            true))
                    .collect(toImmutableList());
            return new TableColumnSpec(name, ducklakeType, nullable, children);
        }
        if (trinoType instanceof MapType mapType) {
            List<TableColumnSpec> children = ImmutableList.of(
                    toColumnSpec("key", mapType.getKeyType(), false),
                    toColumnSpec("value", mapType.getValueType(), true));
            return new TableColumnSpec(name, ducklakeType, nullable, children);
        }

        return TableColumnSpec.leaf(name, ducklakeType, nullable);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DucklakeTableHandle handle = (DucklakeTableHandle) tableHandle;
        translateCatalogExceptions(() -> catalog.dropTable(handle.schemaName(), handle.tableName()));
    }

    // ==================== ALTER TABLE ====================

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column, ColumnPosition position)
    {
        DucklakeTableHandle handle = (DucklakeTableHandle) tableHandle;
        TableColumnSpec columnSpec = toColumnSpec(column.getName(), column.getType(), column.isNullable());
        translateCatalogExceptions(() -> catalog.addColumn(handle.tableId(), columnSpec));
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        DucklakeTableHandle handle = (DucklakeTableHandle) tableHandle;
        DucklakeColumnHandle ducklakeColumn = (DucklakeColumnHandle) column;
        translateCatalogExceptions(() -> catalog.dropColumn(handle.tableId(), ducklakeColumn.columnId()));
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        DucklakeTableHandle handle = (DucklakeTableHandle) tableHandle;
        DucklakeColumnHandle ducklakeColumn = (DucklakeColumnHandle) source;
        translateCatalogExceptions(() -> catalog.renameColumn(handle.tableId(), ducklakeColumn.columnId(), target));
    }

    // ==================== INSERT ====================

    @Override
    public ConnectorInsertTableHandle beginInsert(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            RetryMode retryMode)
    {
        DucklakeTableHandle handle = (DucklakeTableHandle) tableHandle;

        List<DucklakeColumnHandle> ducklakeColumns = columns.stream()
                .map(DucklakeColumnHandle.class::cast)
                .collect(toImmutableList());

        List<DucklakeColumn> allCatalogColumns = catalog.getAllColumnsWithParentage(handle.tableId(), handle.snapshotId());

        List<DucklakePartitionSpec> partitionSpecs = catalog.getPartitionSpecs(handle.tableId(), handle.snapshotId());
        Optional<DucklakePartitionSpec> activePartitionSpec = partitionSpecs.isEmpty()
                ? Optional.empty()
                : Optional.of(partitionSpecs.getLast());

        String tableDataPath = resolveTableDataPath(handle.schemaName(), handle.tableName(), handle.snapshotId());

        return new DucklakeWritableTableHandle(
                handle.schemaName(),
                handle.tableName(),
                handle.tableId(),
                ducklakeColumns,
                allCatalogColumns,
                tableDataPath,
                activePartitionSpec,
                temporalPartitionEncoding);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        DucklakeWritableTableHandle handle = (DucklakeWritableTableHandle) insertHandle;
        List<DucklakeWriteFragment> writeFragments = deserializeFragments(fragments);

        if (!writeFragments.isEmpty()) {
            translateCatalogExceptions(() -> catalog.commitInsert(handle.tableId(), writeFragments));
        }

        return Optional.empty();
    }

    // ==================== CTAS ====================

    @Override
    public ConnectorOutputTableHandle beginCreateTable(
            ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            Optional<ConnectorTableLayout> layout,
            RetryMode retryMode,
            boolean replace)
    {
        if (replace) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }

        SchemaTableName tableName = tableMetadata.getTable();

        // Create the table structure (DDL snapshot)
        List<TableColumnSpec> columnSpecs = tableMetadata.getColumns().stream()
                .map(column -> toColumnSpec(column.getName(), column.getType(), column.isNullable()))
                .collect(toImmutableList());

        List<PartitionFieldSpec> partitionFields = DucklakeTableProperties.getPartitionFields(tableMetadata.getProperties());
        Optional<List<PartitionFieldSpec>> partitionSpec = partitionFields.isEmpty()
                ? Optional.empty()
                : Optional.of(partitionFields);

        translateCatalogExceptions(() -> catalog.createTable(tableName.getSchemaName(), tableName.getTableName(), columnSpecs, partitionSpec));

        // Resolve the newly created table to get its ID and build a writable handle
        long snapshotId = catalog.getCurrentSnapshotId();
        Optional<DucklakeTable> table = catalog.getTable(tableName.getSchemaName(), tableName.getTableName(), snapshotId);
        if (table.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Table was not created: " + tableName);
        }

        List<DucklakeColumn> catalogColumns = catalog.getTableColumns(table.get().tableId(), snapshotId);
        List<DucklakeColumnHandle> columnHandles = catalogColumns.stream()
                .filter(col -> col.parentColumn().isEmpty())
                .map(col -> new DucklakeColumnHandle(
                        col.columnId(),
                        col.columnName(),
                        typeConverter.toTrinoType(col.columnType()),
                        col.nullsAllowed()))
                .collect(toImmutableList());

        List<DucklakeColumn> allCatalogColumns = catalog.getAllColumnsWithParentage(table.get().tableId(), snapshotId);

        List<DucklakePartitionSpec> partitionSpecs = catalog.getPartitionSpecs(table.get().tableId(), snapshotId);
        Optional<DucklakePartitionSpec> activePartitionSpec = partitionSpecs.isEmpty()
                ? Optional.empty()
                : Optional.of(partitionSpecs.getLast());

        String tableDataPath = resolveTableDataPath(tableName.getSchemaName(), tableName.getTableName(), snapshotId);

        return new DucklakeWritableTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.get().tableId(),
                columnHandles,
                allCatalogColumns,
                tableDataPath,
                activePartitionSpec,
                temporalPartitionEncoding);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        DucklakeWritableTableHandle handle = (DucklakeWritableTableHandle) tableHandle;
        List<DucklakeWriteFragment> writeFragments = deserializeFragments(fragments);

        if (!writeFragments.isEmpty()) {
            translateCatalogExceptions(() -> catalog.commitInsert(handle.tableId(), writeFragments));
        }

        return Optional.empty();
    }

    private List<DucklakeWriteFragment> deserializeFragments(Collection<Slice> fragments)
    {
        return fragments.stream()
                .map(fragment -> fragmentCodec.fromJson(fragment.getBytes()))
                .collect(toImmutableList());
    }

    private String resolveTableDataPath(String schemaName, String tableName, long snapshotId)
    {
        Optional<DucklakeSchema> schema = catalog.getSchema(schemaName, snapshotId);
        Optional<DucklakeTable> table = catalog.getTable(schemaName, tableName, snapshotId);

        if (schema.isEmpty() || table.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot resolve data path for " + schemaName + "." + tableName);
        }

        return pathResolver.resolveTableDataPath(schema.get(), table.get());
    }

    // ==================== DELETE / MERGE ====================

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return DucklakeColumnHandle.rowIdColumnHandle();
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Map<Integer, Collection<ColumnHandle>> updateCaseColumns,
            RetryMode retryMode)
    {
        DucklakeTableHandle handle = (DucklakeTableHandle) tableHandle;

        // Build insert handle for UPDATE support (delete+insert pattern)
        List<DucklakeColumnHandle> ducklakeColumns = catalog.getTableColumns(handle.tableId(), handle.snapshotId()).stream()
                .filter(col -> col.parentColumn().isEmpty())
                .map(col -> new DucklakeColumnHandle(
                        col.columnId(),
                        col.columnName(),
                        typeConverter.toTrinoType(col.columnType()),
                        col.nullsAllowed()))
                .collect(toImmutableList());

        List<DucklakeColumn> allCatalogColumns = catalog.getAllColumnsWithParentage(handle.tableId(), handle.snapshotId());

        List<DucklakePartitionSpec> partitionSpecs = catalog.getPartitionSpecs(handle.tableId(), handle.snapshotId());
        Optional<DucklakePartitionSpec> activePartitionSpec = partitionSpecs.isEmpty()
                ? Optional.empty()
                : Optional.of(partitionSpecs.getLast());

        String tableDataPath = resolveTableDataPath(handle.schemaName(), handle.tableName(), handle.snapshotId());

        DucklakeWritableTableHandle insertHandle = new DucklakeWritableTableHandle(
                handle.schemaName(),
                handle.tableName(),
                handle.tableId(),
                ducklakeColumns,
                allCatalogColumns,
                tableDataPath,
                activePartitionSpec,
                temporalPartitionEncoding);

        // Build data file ranges for row ID → data file resolution
        List<DucklakeDataFile> dataFiles = catalog.getDataFiles(handle.tableId(), handle.snapshotId());
        List<DucklakeMergeTableHandle.DataFileRange> dataFileRanges = dataFiles.stream()
                .map(df -> new DucklakeMergeTableHandle.DataFileRange(df.dataFileId(), df.rowIdStart(), df.recordCount()))
                .collect(toImmutableList());

        return new DucklakeMergeTableHandle(handle, insertHandle, dataFileRanges);
    }

    @Override
    public void finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle mergeTableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        DucklakeMergeTableHandle mergeHandle = (DucklakeMergeTableHandle) mergeTableHandle;
        DucklakeTableHandle tableHandle = mergeHandle.tableHandle();

        List<DucklakeDeleteFragment> deleteFragments = new ArrayList<>();
        List<DucklakeWriteFragment> insertFragments = new ArrayList<>();

        for (Slice fragment : fragments) {
            byte[] bytes = fragment.getBytes();
            // Try to parse as delete fragment first, fall back to write fragment
            try {
                DucklakeDeleteFragment deleteFragment = deleteFragmentCodec.fromJson(bytes);
                // Verify it's actually a delete fragment (has dataFileId > 0 and path starts with "ducklake-delete-")
                if (deleteFragment.path().startsWith("ducklake-delete-")) {
                    deleteFragments.add(deleteFragment);
                    continue;
                }
            }
            catch (RuntimeException _) {
                // Not a delete fragment
            }
            try {
                insertFragments.add(fragmentCodec.fromJson(bytes));
            }
            catch (RuntimeException e) {
                throw new RuntimeException("Failed to deserialize merge fragment", e);
            }
        }

        // Commit atomically in a single snapshot — critical for UPDATE (delete+insert must be atomic)
        if (!deleteFragments.isEmpty() && !insertFragments.isEmpty()) {
            translateCatalogExceptions(() -> catalog.commitMerge(tableHandle.tableId(), deleteFragments, insertFragments));
        }
        else if (!deleteFragments.isEmpty()) {
            translateCatalogExceptions(() -> catalog.commitDelete(tableHandle.tableId(), deleteFragments));
        }
        else if (!insertFragments.isEmpty()) {
            translateCatalogExceptions(() -> catalog.commitInsert(tableHandle.tableId(), insertFragments));
        }
    }

    // ==================== View operations ====================

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        long snapshotId = snapshotResolver.resolveSnapshotId(session);

        if (schemaName.isPresent()) {
            Optional<DucklakeSchema> schema = catalog.getSchema(schemaName.get(), snapshotId);
            if (schema.isEmpty()) {
                return ImmutableList.of();
            }
            return catalog.listViews(schema.get().schemaId(), snapshotId).stream()
                    .filter(view -> isViewAccessible(view))
                    .map(view -> new SchemaTableName(schemaName.get(), view.viewName()))
                    .collect(toImmutableList());
        }

        ImmutableList.Builder<SchemaTableName> views = ImmutableList.builder();
        for (DucklakeSchema schema : catalog.listSchemas(snapshotId)) {
            for (DucklakeView view : catalog.listViews(schema.schemaId(), snapshotId)) {
                if (isViewAccessible(view)) {
                    views.add(new SchemaTableName(schema.schemaName(), view.viewName()));
                }
            }
        }
        return views.build();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        long snapshotId = snapshotResolver.resolveSnapshotId(session);

        Optional<DucklakeView> ducklakeView = catalog.getView(viewName.getSchemaName(), viewName.getTableName(), snapshotId);
        if (ducklakeView.isEmpty()) {
            return Optional.empty();
        }

        DucklakeView view = ducklakeView.get();
        if (!isViewAccessible(view)) {
            return Optional.empty();
        }

        return decodeTrinoView(view, viewName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        long snapshotId = snapshotResolver.resolveSnapshotId(session);
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();

        List<DucklakeSchema> schemas;
        if (schemaName.isPresent()) {
            Optional<DucklakeSchema> schema = catalog.getSchema(schemaName.get(), snapshotId);
            if (schema.isEmpty()) {
                return ImmutableMap.of();
            }
            schemas = ImmutableList.of(schema.get());
        }
        else {
            schemas = catalog.listSchemas(snapshotId);
        }

        for (DucklakeSchema schema : schemas) {
            for (DucklakeView view : catalog.listViews(schema.schemaId(), snapshotId)) {
                if (isViewAccessible(view)) {
                    SchemaTableName name = new SchemaTableName(schema.schemaName(), view.viewName());
                    decodeTrinoView(view, name).ifPresent(def -> views.put(name, def));
                }
            }
        }

        return views.buildOrThrow();
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        if (replace) {
            long snapshotId = snapshotResolver.resolveSnapshotId(session);
            Optional<DucklakeView> existing = catalog.getView(viewName.getSchemaName(), viewName.getTableName(), snapshotId);
            if (existing.isPresent()) {
                translateCatalogExceptions(() -> catalog.dropView(viewName.getSchemaName(), viewName.getTableName()));
            }
        }

        // Store the original SQL in the sql field (spec-compliant).
        // Store the full ConnectorViewDefinition as JSON in column_aliases
        // so we can round-trip column names and types.
        String originalSql = definition.getOriginalSql();
        String viewMetadataJson = VIEW_CODEC.toJson(definition);

        translateCatalogExceptions(() -> catalog.createView(viewName.getSchemaName(), viewName.getTableName(), originalSql, TRINO_VIEW_DIALECT, viewMetadataJson));
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        if (source.equals(target)) {
            return;
        }

        long snapshotId = snapshotResolver.resolveSnapshotId(session);
        Optional<DucklakeView> sourceView = catalog.getView(source.getSchemaName(), source.getTableName(), snapshotId);
        if (sourceView.isEmpty() || !isViewAccessible(sourceView.get())) {
            throw new ViewNotFoundException(source);
        }

        if (catalog.getTable(target.getSchemaName(), target.getTableName(), snapshotId).isPresent() || catalog.getView(target.getSchemaName(), target.getTableName(), snapshotId).isPresent()) {
            throw new TrinoException(ALREADY_EXISTS, "Relation already exists: " + target);
        }

        translateCatalogExceptions(() -> catalog.renameView(source.getSchemaName(), source.getTableName(), target.getSchemaName(), target.getTableName()));
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        ConnectorViewDefinition definition = getRequiredViewDefinition(session, viewName);
        ConnectorViewDefinition updated = new ConnectorViewDefinition(
                definition.getOriginalSql(),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns(),
                comment,
                definition.getOwner(),
                definition.isRunAsInvoker(),
                definition.getPath());

        translateCatalogExceptions(() -> catalog.replaceViewMetadata(
                viewName.getSchemaName(),
                viewName.getTableName(),
                updated.getOriginalSql(),
                TRINO_VIEW_DIALECT,
                VIEW_CODEC.toJson(updated)));
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        ConnectorViewDefinition definition = getRequiredViewDefinition(session, viewName);

        boolean updated = false;
        ImmutableList.Builder<ConnectorViewDefinition.ViewColumn> columnsBuilder = ImmutableList.builderWithExpectedSize(definition.getColumns().size());
        for (ConnectorViewDefinition.ViewColumn column : definition.getColumns()) {
            if (column.getName().equals(columnName)) {
                columnsBuilder.add(new ConnectorViewDefinition.ViewColumn(column.getName(), column.getType(), comment));
                updated = true;
            }
            else {
                columnsBuilder.add(column);
            }
        }
        if (!updated) {
            throw new ColumnNotFoundException(viewName, columnName);
        }
        List<ConnectorViewDefinition.ViewColumn> columns = columnsBuilder.build();

        ConnectorViewDefinition updatedDefinition = new ConnectorViewDefinition(
                definition.getOriginalSql(),
                definition.getCatalog(),
                definition.getSchema(),
                columns,
                definition.getComment(),
                definition.getOwner(),
                definition.isRunAsInvoker(),
                definition.getPath());

        translateCatalogExceptions(() -> catalog.replaceViewMetadata(
                viewName.getSchemaName(),
                viewName.getTableName(),
                updatedDefinition.getOriginalSql(),
                TRINO_VIEW_DIALECT,
                VIEW_CODEC.toJson(updatedDefinition)));
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        long snapshotId = snapshotResolver.resolveSnapshotId(session);
        Optional<DucklakeView> existing = catalog.getView(viewName.getSchemaName(), viewName.getTableName(), snapshotId);
        if (existing.isEmpty()) {
            throw new ViewNotFoundException(viewName);
        }

        translateCatalogExceptions(() -> catalog.dropView(viewName.getSchemaName(), viewName.getTableName()));
    }

    private ConnectorViewDefinition getRequiredViewDefinition(ConnectorSession session, SchemaTableName viewName)
    {
        long snapshotId = snapshotResolver.resolveSnapshotId(session);
        Optional<DucklakeView> view = catalog.getView(viewName.getSchemaName(), viewName.getTableName(), snapshotId);
        if (view.isEmpty() || !isViewAccessible(view.get())) {
            throw new ViewNotFoundException(viewName);
        }

        return decodeTrinoView(view.get(), viewName)
                .orElseThrow(() -> new ViewNotFoundException(viewName, "View metadata is unavailable"));
    }

    /**
     * Only Trino-dialect views are currently accessible.
     * Non-Trino dialects (e.g., duckdb) require a SQL transpiler to safely expose.
     */
    private boolean isViewAccessible(DucklakeView view)
    {
        String dialect = view.dialect().toLowerCase();
        if (TRINO_VIEW_DIALECT.equals(dialect)) {
            return true;
        }
        // Future: check viewSqlDialects set + transpiler availability
        log.debug("Skipping view %s with non-Trino dialect: %s (transpiler not configured)", view.viewName(), dialect);
        return false;
    }

    /**
     * Decode a Trino-dialect view from the catalog.
     * Column metadata is stored as JSON in the column_aliases field.
     * Falls back to original SQL if metadata is missing or corrupt.
     */
    private Optional<ConnectorViewDefinition> decodeTrinoView(DucklakeView view, SchemaTableName viewName)
    {
        // Trino views store the full ConnectorViewDefinition as JSON in column_aliases
        if (view.viewMetadata().isPresent() && !view.viewMetadata().get().isBlank()) {
            try {
                return Optional.of(VIEW_CODEC.fromJson(view.viewMetadata().get()));
            }
            catch (RuntimeException e) {
                log.warn(e, "Failed to decode Trino view metadata for %s", viewName);
            }
        }

        log.warn("View %s has no Trino metadata in column_aliases, cannot resolve column types", viewName);
        return Optional.empty();
    }

    /**
     * Translate catalog-layer TransactionConflictException to Trino's
     * TrinoException(TRANSACTION_CONFLICT) so the engine reports it correctly.
     */
    private static void translateCatalogExceptions(Runnable action)
    {
        try {
            action.run();
        }
        catch (TransactionConflictException e) {
            throw new TrinoException(TRANSACTION_CONFLICT, e.getMessage(), e);
        }
    }
}
