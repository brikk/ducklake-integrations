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
import dev.brikk.ducklake.catalog.DucklakeColumn;
import dev.brikk.ducklake.catalog.DucklakeNameMap;
import dev.brikk.ducklake.catalog.DucklakeNameMapEntry;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Recursively matches a parquet file's schema against a DuckLake table's field
 * tree by case-insensitive name, type-checks each leaf, and emits a
 * {@link DucklakeNameMap} that records the source-name → target-field-id
 * mapping for the file. Mirrors upstream's {@code DuckLakeFileProcessor::MapColumns}
 * and {@code MapColumn} in {@code ducklake_add_data_files.cpp}.
 *
 * <p>Modes:
 * <ul>
 *   <li>{@code allowMissing} — table columns absent from the file are allowed
 *       (NULL at read time) instead of throwing.
 *   <li>{@code ignoreExtraColumns} — parquet columns absent from the table
 *       are silently skipped instead of throwing.
 *   <li>{@code hivePartitionValues} — pre-parsed {@code key=value} pairs from
 *       the file path. When the table has a column matching a key, the column
 *       is treated as a hive-partition column: it gets a {@code is_partition=true}
 *       name-map entry and a partition value record, and need not exist in the
 *       parquet schema.
 * </ul>
 */
final class DucklakeAddFilesNameMapper
{
    private final DucklakeTypeConverter typeConverter;
    private final boolean allowMissing;
    private final boolean ignoreExtraColumns;
    private final Map<String, String> hivePartitionValues;
    private final String fileName;
    private final String tableName;

    private final List<DucklakeNameMapEntry> resultEntries = new ArrayList<>();
    // Maps each top-level parquet column to a (Trino target type, DuckLake field id).
    // Used to reorder columns when feeding the existing DucklakeStatsExtractor — its
    // contract is "column N of the columns list maps to row-group column N of the file".
    private final List<TopLevelMatch> topLevelMatches = new ArrayList<>();
    private final Map<Integer, Object> partitionValues = new LinkedHashMap<>();

    DucklakeAddFilesNameMapper(
            DucklakeTypeConverter typeConverter,
            boolean allowMissing,
            boolean ignoreExtraColumns,
            Map<String, String> hivePartitionValues,
            String fileName,
            String tableName)
    {
        this.typeConverter = typeConverter;
        this.allowMissing = allowMissing;
        this.ignoreExtraColumns = ignoreExtraColumns;
        this.hivePartitionValues = hivePartitionValues;
        this.fileName = fileName;
        this.tableName = tableName;
    }

    /**
     * Result bundle returned from {@link #map}.
     */
    record Result(
            DucklakeNameMap nameMap,
            List<TopLevelMatch> topLevelMatches,
            Map<Integer, String> partitionValues) {}

    /**
     * One top-level parquet column matched to a DuckLake field. {@code parquetIndex}
     * is the column's position in the parquet root schema (in file order), used to
     * align the existing stats extractor's positional column lookup.
     */
    record TopLevelMatch(
            int parquetIndex,
            long fieldId,
            String columnName,
            Type targetType) {}

    Result map(MessageType parquetSchema, List<DucklakeColumn> allColumns, List<DucklakeColumn> topLevelColumns)
    {
        // Build parent → name → child lookup
        Map<Long, Map<String, DucklakeColumn>> childrenByParent = new HashMap<>();
        for (DucklakeColumn column : allColumns) {
            column.parentColumn().ifPresent(parentId ->
                    childrenByParent
                            .computeIfAbsent(parentId, ignored -> new HashMap<>())
                            .put(column.columnName().toLowerCase(Locale.ROOT), column));
        }

        Map<String, DucklakeColumn> topByName = new HashMap<>();
        for (DucklakeColumn column : topLevelColumns) {
            topByName.put(column.columnName().toLowerCase(Locale.ROOT), column);
        }
        Map<String, Long> hivePartitionKeyLower = new HashMap<>();
        for (String key : hivePartitionValues.keySet()) {
            hivePartitionKeyLower.put(key.toLowerCase(Locale.ROOT), null);
        }

        List<org.apache.parquet.schema.Type> parquetFields = parquetSchema.getFields();
        Map<String, Integer> tableColumnsMatched = new HashMap<>();

        for (int i = 0; i < parquetFields.size(); i++) {
            org.apache.parquet.schema.Type parquetField = parquetFields.get(i);
            String parquetName = parquetField.getName();
            String parquetNameLower = parquetName.toLowerCase(Locale.ROOT);

            DucklakeColumn match = topByName.get(parquetNameLower);
            if (match == null) {
                if (ignoreExtraColumns) {
                    continue;
                }
                throw new DucklakeAddFilesException(String.format(
                        "Column \"%s\" exists in file \"%s\" but was not found in table \"%s\". "
                                + "Set ignore_extra_columns => true to add the file anyway",
                        parquetName, fileName, tableName));
            }
            // If the matched column is also named in the hive path, the path value takes
            // precedence (upstream behavior — a file under part=10/foo.parquet wins over
            // any 'part' column inside the parquet itself).
            if (hivePartitionKeyLower.containsKey(parquetNameLower)) {
                continue;
            }
            Type targetType = typeConverter.toTrinoType(match.columnType());
            DucklakeNameMapEntry entry = mapField(parquetField, parquetName, match, targetType, childrenByParent);
            resultEntries.add(entry);
            topLevelMatches.add(new TopLevelMatch(i, match.columnId(), parquetName, targetType));
            tableColumnsMatched.put(parquetNameLower, i);
        }

        // Handle table columns not present in the parquet schema: either hive partition
        // value, allow_missing, or error.
        for (DucklakeColumn column : topLevelColumns) {
            String columnNameLower = column.columnName().toLowerCase(Locale.ROOT);
            if (tableColumnsMatched.containsKey(columnNameLower)) {
                continue;
            }
            if (hivePartitionValues.containsKey(column.columnName())
                    || hivePartitionKeyLower.containsKey(columnNameLower)) {
                // Resolve the actual path-side key matching this column (case-insensitive).
                String pathKey = column.columnName();
                if (!hivePartitionValues.containsKey(pathKey)) {
                    for (String k : hivePartitionValues.keySet()) {
                        if (k.equalsIgnoreCase(column.columnName())) {
                            pathKey = k;
                            break;
                        }
                    }
                }
                String stringValue = hivePartitionValues.get(pathKey);
                Type targetType = typeConverter.toTrinoType(column.columnType());
                if (isNestedType(targetType)) {
                    throw new DucklakeAddFilesException(String.format(
                            "Type \"%s\" is not supported for hive partitioning (column \"%s\")",
                            targetType.getDisplayName(), column.columnName()));
                }
                resultEntries.add(new DucklakeNameMapEntry(
                        column.columnName(), column.columnId(), true, List.of()));
                partitionValues.put(toIntFieldIndex(column.columnId()), stringValue);
                continue;
            }
            if (!allowMissing) {
                throw new DucklakeAddFilesException(String.format(
                        "Column \"%s\" exists in table \"%s\" but was not found in file \"%s\". "
                                + "Set allow_missing => true to allow missing fields and columns",
                        column.columnName(), tableName, fileName));
            }
        }

        // Convert the (Integer fieldIndex → String) partition map to immutable.
        Map<Integer, String> partitionValuesOut = new LinkedHashMap<>();
        for (Map.Entry<Integer, Object> entry : partitionValues.entrySet()) {
            partitionValuesOut.put(entry.getKey(), (String) entry.getValue());
        }

        return new Result(
                new DucklakeNameMap(ImmutableList.copyOf(resultEntries)),
                ImmutableList.copyOf(topLevelMatches),
                partitionValuesOut);
    }

    private DucklakeNameMapEntry mapField(
            org.apache.parquet.schema.Type parquetField,
            String parquetName,
            DucklakeColumn target,
            Type targetType,
            Map<Long, Map<String, DucklakeColumn>> childrenByParent)
    {
        // Leaf primitive: type-check, no children.
        if (parquetField.isPrimitive()) {
            if (targetType instanceof RowType || targetType instanceof ArrayType || targetType instanceof MapType) {
                throw new DucklakeAddFilesException(String.format(
                        "Column \"%s\" in file \"%s\" is primitive but table column has nested type %s",
                        parquetName, fileName, targetType.getDisplayName()));
            }
            Type source = parquetPrimitiveToTrino(parquetField.asPrimitiveType(), parquetName);
            DucklakeAddFilesTypeChecker.checkCompatible(targetType, source, parquetName, fileName, tableName);
            return new DucklakeNameMapEntry(parquetName, target.columnId(), false, List.of());
        }

        // Group/nested: recurse based on target type kind.
        GroupType group = parquetField.asGroupType();
        if (targetType instanceof RowType rowType) {
            return mapStruct(group, parquetName, target, rowType, childrenByParent);
        }
        if (targetType instanceof ArrayType arrayType) {
            return mapList(group, parquetName, target, arrayType, childrenByParent);
        }
        if (targetType instanceof MapType mapType) {
            return mapMap(group, parquetName, target, mapType, childrenByParent);
        }
        throw new DucklakeAddFilesException(String.format(
                "Column \"%s\" in file \"%s\" is a group but table column has primitive type %s",
                parquetName, fileName, targetType.getDisplayName()));
    }

    private DucklakeNameMapEntry mapStruct(
            GroupType group,
            String parquetName,
            DucklakeColumn target,
            RowType rowType,
            Map<Long, Map<String, DucklakeColumn>> childrenByParent)
    {
        Map<String, DucklakeColumn> children = childrenByParent.getOrDefault(target.columnId(), Map.of());
        List<DucklakeNameMapEntry> childEntries = new ArrayList<>();
        for (org.apache.parquet.schema.Type field : group.getFields()) {
            String fieldName = field.getName();
            DucklakeColumn childTarget = children.get(fieldName.toLowerCase(Locale.ROOT));
            if (childTarget == null) {
                if (ignoreExtraColumns) {
                    continue;
                }
                throw new DucklakeAddFilesException(String.format(
                        "Struct field \"%s.%s\" exists in file \"%s\" but was not found in table \"%s\". "
                                + "Set ignore_extra_columns => true to add the file anyway",
                        parquetName, fieldName, fileName, tableName));
            }
            Type childTrinoType = findRowFieldType(rowType, childTarget.columnName());
            if (childTrinoType == null) {
                // Catalog and Trino-side row type out of sync — shouldn't happen for a
                // freshly converted type. Defensive throw.
                throw new DucklakeAddFilesException(String.format(
                        "Internal: struct field \"%s.%s\" not found in target row type for table \"%s\"",
                        parquetName, fieldName, tableName));
            }
            childEntries.add(mapField(field, fieldName, childTarget, childTrinoType, childrenByParent));
        }
        return new DucklakeNameMapEntry(parquetName, target.columnId(), false, childEntries);
    }

    private DucklakeNameMapEntry mapList(
            GroupType group,
            String parquetName,
            DucklakeColumn target,
            ArrayType arrayType,
            Map<Long, Map<String, DucklakeColumn>> childrenByParent)
    {
        // Parquet LIST: one repeated child group ("list") containing one element field.
        if (group.getFieldCount() != 1) {
            throw new DucklakeAddFilesException(String.format(
                    "Column \"%s\" in file \"%s\" has unexpected LIST shape (expected one child group, found %d)",
                    parquetName, fileName, group.getFieldCount()));
        }
        org.apache.parquet.schema.Type middle = group.getType(0);
        org.apache.parquet.schema.Type elementParquetType = middle;
        if (!middle.isPrimitive() && middle.asGroupType().getFieldCount() == 1) {
            elementParquetType = middle.asGroupType().getType(0);
        }
        // Catalog has a single synthetic child for list element.
        Map<String, DucklakeColumn> children = childrenByParent.getOrDefault(target.columnId(), Map.of());
        if (children.size() != 1) {
            throw new DucklakeAddFilesException(String.format(
                    "Column \"%s\" in file \"%s\" maps to ARRAY but table side has %d list-element columns",
                    parquetName, fileName, children.size()));
        }
        DucklakeColumn elementTarget = children.values().iterator().next();
        DucklakeNameMapEntry childEntry = mapField(elementParquetType, "element", elementTarget,
                arrayType.getElementType(), childrenByParent);
        return new DucklakeNameMapEntry(parquetName, target.columnId(), false, List.of(childEntry));
    }

    private DucklakeNameMapEntry mapMap(
            GroupType group,
            String parquetName,
            DucklakeColumn target,
            MapType mapType,
            Map<Long, Map<String, DucklakeColumn>> childrenByParent)
    {
        // Parquet MAP: one repeated child group ("key_value") with two fields key, value.
        if (group.getFieldCount() != 1) {
            throw new DucklakeAddFilesException(String.format(
                    "Column \"%s\" in file \"%s\" has unexpected MAP shape (expected one child group, found %d)",
                    parquetName, fileName, group.getFieldCount()));
        }
        GroupType kvGroup = group.getType(0).asGroupType();
        if (kvGroup.getFieldCount() != 2) {
            throw new DucklakeAddFilesException(String.format(
                    "Column \"%s\" in file \"%s\" has unexpected MAP shape (key_value group must have 2 fields)",
                    parquetName, fileName));
        }
        Map<String, DucklakeColumn> children = childrenByParent.getOrDefault(target.columnId(), Map.of());
        DucklakeColumn keyTarget = children.get("key");
        DucklakeColumn valueTarget = children.get("value");
        if (keyTarget == null || valueTarget == null) {
            throw new DucklakeAddFilesException(String.format(
                    "Column \"%s\" maps to MAP but table side is missing key/value child columns",
                    parquetName));
        }
        DucklakeNameMapEntry keyEntry = mapField(kvGroup.getType(0), "key", keyTarget,
                mapType.getKeyType(), childrenByParent);
        DucklakeNameMapEntry valueEntry = mapField(kvGroup.getType(1), "value", valueTarget,
                mapType.getValueType(), childrenByParent);
        return new DucklakeNameMapEntry(parquetName, target.columnId(), false, List.of(keyEntry, valueEntry));
    }

    private static Type findRowFieldType(RowType rowType, String fieldName)
    {
        for (RowType.Field field : rowType.getFields()) {
            if (field.getName().isPresent() && field.getName().get().equalsIgnoreCase(fieldName)) {
                return field.getType();
            }
        }
        return null;
    }

    private static boolean isNestedType(Type type)
    {
        return type instanceof RowType || type instanceof ArrayType || type instanceof MapType;
    }

    private static int toIntFieldIndex(long columnId)
    {
        if (columnId > Integer.MAX_VALUE) {
            throw new DucklakeAddFilesException("DuckLake column_id exceeds Integer range: " + columnId);
        }
        return (int) columnId;
    }

    /**
     * Best-effort parquet primitive → Trino type. Mirrors upstream's
     * {@code DuckLakeParquetTypeChecker::DeriveLogicalType}. Errors raised here become
     * "Failed to map column..." messages at the call site.
     */
    private Type parquetPrimitiveToTrino(PrimitiveType type, String parquetName)
    {
        LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
        if (annotation != null) {
            Optional<Type> result = annotation.accept(new LogicalTypeAnnotationVisitor<Type>()
            {
                @Override
                public Optional<Type> visit(StringLogicalTypeAnnotation a)
                {
                    return Optional.of(VarcharType.VARCHAR);
                }

                @Override
                public Optional<Type> visit(DecimalLogicalTypeAnnotation a)
                {
                    return Optional.of(DecimalType.createDecimalType(a.getPrecision(), a.getScale()));
                }

                @Override
                public Optional<Type> visit(IntLogicalTypeAnnotation a)
                {
                    if (a.isSigned()) {
                        return Optional.of(switch (a.getBitWidth()) {
                            case 8 -> TinyintType.TINYINT;
                            case 16 -> SmallintType.SMALLINT;
                            case 32 -> IntegerType.INTEGER;
                            default -> BigintType.BIGINT;
                        });
                    }
                    // unsigned widens upstream — pin to next-larger signed type for safety
                    return Optional.of(switch (a.getBitWidth()) {
                        case 8 -> SmallintType.SMALLINT;
                        case 16 -> IntegerType.INTEGER;
                        case 32 -> BigintType.BIGINT;
                        default -> DecimalType.createDecimalType(20, 0);
                    });
                }

                @Override
                public Optional<Type> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation a)
                {
                    return Optional.of(DateType.DATE);
                }

                @Override
                public Optional<Type> visit(TimestampLogicalTypeAnnotation a)
                {
                    int precision = timeUnitPrecision(a.getUnit());
                    if (a.isAdjustedToUTC()) {
                        return Optional.of(TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision));
                    }
                    return Optional.of(TimestampType.createTimestampType(precision));
                }

                @Override
                public Optional<Type> visit(TimeLogicalTypeAnnotation a)
                {
                    int precision = timeUnitPrecision(a.getUnit());
                    return Optional.of(io.trino.spi.type.TimeType.createTimeType(precision));
                }

                @Override
                public Optional<Type> visit(UUIDLogicalTypeAnnotation a)
                {
                    return Optional.of(UuidType.UUID);
                }
            });
            if (result.isPresent()) {
                return result.get();
            }
        }
        switch (type.getPrimitiveTypeName()) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case INT32:
                return IntegerType.INTEGER;
            case INT64:
                return BigintType.BIGINT;
            case INT96:
                return TimestampType.createTimestampType(6);
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                return VarbinaryType.VARBINARY;
        }
        throw new DucklakeAddFilesException(String.format(
                "Unrecognized parquet primitive type %s for column \"%s\" in file \"%s\"",
                type.getPrimitiveTypeName(), parquetName, fileName));
    }

    private static int timeUnitPrecision(TimeUnit unit)
    {
        return switch (unit) {
            case MILLIS -> 3;
            case MICROS -> 6;
            case NANOS -> 9;
        };
    }
}
