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
import dev.brikk.ducklake.catalog.DucklakeColumn
import dev.brikk.ducklake.catalog.DucklakeNameMap
import dev.brikk.ducklake.catalog.DucklakeNameMapEntry
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType
import io.trino.spi.type.BooleanType
import io.trino.spi.type.DateType
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType
import io.trino.spi.type.IntegerType
import io.trino.spi.type.MapType
import io.trino.spi.type.RealType
import io.trino.spi.type.RowType
import io.trino.spi.type.SmallintType
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType
import io.trino.spi.type.Type
import io.trino.spi.type.UuidType
import io.trino.spi.type.VarbinaryType
import io.trino.spi.type.VarcharType
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import java.util.HashMap
import java.util.LinkedHashMap
import java.util.Locale
import java.util.Optional

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
internal class DucklakeAddFilesNameMapper(
        private val typeConverter: DucklakeTypeConverter,
        private val allowMissing: Boolean,
        private val ignoreExtraColumns: Boolean,
        private val hivePartitionValues: Map<String, String>,
        private val fileName: String,
        private val tableName: String,
) {
    private val resultEntries: MutableList<DucklakeNameMapEntry> = ArrayList()
    // Maps each top-level parquet column to a (Trino target type, DuckLake field id).
    // Used to reorder columns when feeding the existing DucklakeStatsExtractor — its
    // contract is "column N of the columns list maps to row-group column N of the file".
    private val topLevelMatches: MutableList<TopLevelMatch> = ArrayList()
    private val partitionValues: MutableMap<Int, Any?> = LinkedHashMap()
    // One LeafStatsTarget per *mapped* parquet leaf — i.e., leaves that have a
    // corresponding DuckLake catalog column. Skipped leaves (ignore_extra_columns
    // children, hive-partition-wins overlap) still advance leafCounter so the
    // emitted parquetColumnIndex stays aligned with the file's RowGroup.columns.
    private val leafStatsTargets: MutableList<LeafStatsTarget> = ArrayList()
    private val leafCounter: IntArray = intArrayOf(0)

    /**
     * Result bundle returned from {@link #map}. {@code leafStatsTargets} lists
     * one entry per matched parquet leaf (in file leaf order) for
     * {@link DucklakeStatsExtractor} to consume; skipped or hive-overridden
     * parquet leaves contribute no entry but still advance the underlying
     * parquet column index so {@code parquetColumnIndex} on later targets
     * stays aligned with {@code RowGroup.columns}.
     */
    @JvmRecord
    data class Result(
            val nameMap: DucklakeNameMap,
            val topLevelMatches: List<TopLevelMatch>,
            val partitionValues: Map<Int, String>,
            val leafStatsTargets: List<LeafStatsTarget>,
    )

    /**
     * One top-level parquet column matched to a DuckLake field. {@code parquetIndex}
     * is the column's position in the parquet root schema (in file order), used to
     * align the existing stats extractor's positional column lookup.
     */
    @JvmRecord
    data class TopLevelMatch(
            val parquetIndex: Int,
            val fieldId: Long,
            val columnName: String,
            val targetType: Type,
    )

    fun map(parquetSchema: MessageType, allColumns: List<DucklakeColumn>, topLevelColumns: List<DucklakeColumn>): Result {
        // Build parent → name → child lookup
        val childrenByParent: MutableMap<Long, MutableMap<String, DucklakeColumn>> = HashMap()
        for (column in allColumns) {
            column.parentColumn.ifPresent { parentId ->
                childrenByParent
                        .computeIfAbsent(parentId) { _ -> HashMap() }
                        .put(column.columnName.lowercase(Locale.ROOT), column)
            }
        }

        val topByName: MutableMap<String, DucklakeColumn> = HashMap()
        for (column in topLevelColumns) {
            topByName.put(column.columnName.lowercase(Locale.ROOT), column)
        }
        val hivePartitionKeyLower: MutableMap<String, Long?> = HashMap()
        for (key in hivePartitionValues.keys) {
            hivePartitionKeyLower.put(key.lowercase(Locale.ROOT), null)
        }

        val parquetFields: List<org.apache.parquet.schema.Type> = parquetSchema.getFields()
        val tableColumnsMatched: MutableMap<String, Int> = HashMap()

        for (i in 0 until parquetFields.size) {
            val parquetField = parquetFields.get(i)
            val parquetName = parquetField.getName()
            val parquetNameLower = parquetName.lowercase(Locale.ROOT)

            val match: DucklakeColumn? = topByName.get(parquetNameLower)
            if (match == null) {
                if (ignoreExtraColumns) {
                    // Walk past the skipped column's parquet leaves so subsequent
                    // matched leaves get the right RowGroup.columns offset.
                    leafCounter[0] += countParquetLeaves(parquetField)
                    continue
                }
                throw DucklakeAddFilesException(String.format(
                        "Column \"%s\" exists in file \"%s\" but was not found in table \"%s\". "
                                + "Set ignore_extra_columns => true to add the file anyway",
                        parquetName, fileName, tableName))
            }
            // If the matched column is also named in the hive path, the path value takes
            // precedence (upstream behavior — a file under part=10/foo.parquet wins over
            // any 'part' column inside the parquet itself).
            if (hivePartitionKeyLower.containsKey(parquetNameLower)) {
                // Hive value replaces the parquet column entirely — no leaf stats from
                // the parquet column chunks. Advance the counter so later leaves stay
                // aligned with the row group.
                leafCounter[0] += countParquetLeaves(parquetField)
                continue
            }
            val targetType: Type = typeConverter.toTrinoType(match.columnType)
            val entry = mapField(parquetField, parquetName, match, targetType, childrenByParent)
            resultEntries.add(entry)
            topLevelMatches.add(TopLevelMatch(i, match.columnId, parquetName, targetType))
            tableColumnsMatched.put(parquetNameLower, i)
        }

        // Handle table columns not present in the parquet schema: either hive partition
        // value, allow_missing, or error.
        for (column in topLevelColumns) {
            val columnNameLower = column.columnName.lowercase(Locale.ROOT)
            if (tableColumnsMatched.containsKey(columnNameLower)) {
                continue
            }
            if (hivePartitionValues.containsKey(column.columnName)
                    || hivePartitionKeyLower.containsKey(columnNameLower)) {
                // Resolve the actual path-side key matching this column (case-insensitive).
                var pathKey = column.columnName
                if (!hivePartitionValues.containsKey(pathKey)) {
                    for (k in hivePartitionValues.keys) {
                        if (k.equals(column.columnName, ignoreCase = true)) {
                            pathKey = k
                            break
                        }
                    }
                }
                val stringValue = hivePartitionValues.get(pathKey)
                val targetType: Type = typeConverter.toTrinoType(column.columnType)
                if (isNestedType(targetType)) {
                    throw DucklakeAddFilesException(String.format(
                            "Type \"%s\" is not supported for hive partitioning (column \"%s\")",
                            targetType.getDisplayName(), column.columnName))
                }
                resultEntries.add(DucklakeNameMapEntry(
                        column.columnName, column.columnId, true, listOf<DucklakeNameMapEntry>()))
                partitionValues.put(toIntFieldIndex(column.columnId), stringValue)
                continue
            }
            if (!allowMissing) {
                throw DucklakeAddFilesException(String.format(
                        "Column \"%s\" exists in table \"%s\" but was not found in file \"%s\". "
                                + "Set allow_missing => true to allow missing fields and columns",
                        column.columnName, tableName, fileName))
            }
        }

        // Convert the (Integer fieldIndex → String) partition map to immutable.
        val partitionValuesOut: MutableMap<Int, String> = LinkedHashMap()
        for (entry in partitionValues.entries) {
            partitionValuesOut.put(entry.key, entry.value as String)
        }

        return Result(
                DucklakeNameMap(ImmutableList.copyOf(resultEntries)),
                ImmutableList.copyOf(topLevelMatches),
                partitionValuesOut,
                ImmutableList.copyOf(leafStatsTargets))
    }

    private fun mapField(
            parquetField: org.apache.parquet.schema.Type,
            parquetName: String,
            target: DucklakeColumn,
            targetType: Type,
            childrenByParent: Map<Long, Map<String, DucklakeColumn>>,
    ): DucklakeNameMapEntry {
        // Leaf primitive: type-check, no children.
        if (parquetField.isPrimitive()) {
            if (targetType is RowType || targetType is ArrayType || targetType is MapType) {
                throw DucklakeAddFilesException(String.format(
                        "Column \"%s\" in file \"%s\" is primitive but table column has nested type %s",
                        parquetName, fileName, targetType.getDisplayName()))
            }
            val source: Type = parquetPrimitiveToTrino(parquetField.asPrimitiveType(), parquetName)
            DucklakeAddFilesTypeChecker.checkCompatible(targetType, source, parquetName, fileName, tableName)
            // Record one leaf stats target per matched parquet primitive. Upstream's
            // MapColumnStats keys these by the catalog field_id (target.columnId()),
            // not by parquet path — and decodes min/max bytes using the *target*
            // Trino type. The latter matches today's flat-column convention; values
            // can be wrong if the parquet primitive's byte width disagrees with the
            // catalog widening (e.g., INT32 file vs BIGINT catalog), which is a
            // pre-existing limitation of the add_files stats path independent of
            // nesting.
            leafStatsTargets.add(LeafStatsTarget(target.columnId, targetType, leafCounter[0]))
            leafCounter[0]++
            return DucklakeNameMapEntry(parquetName, target.columnId, false, listOf<DucklakeNameMapEntry>())
        }

        // Group/nested: recurse based on target type kind.
        val group: GroupType = parquetField.asGroupType()
        if (targetType is RowType) {
            return mapStruct(group, parquetName, target, targetType, childrenByParent)
        }
        if (targetType is ArrayType) {
            return mapList(group, parquetName, target, targetType, childrenByParent)
        }
        if (targetType is MapType) {
            return mapMap(group, parquetName, target, targetType, childrenByParent)
        }
        throw DucklakeAddFilesException(String.format(
                "Column \"%s\" in file \"%s\" is a group but table column has primitive type %s",
                parquetName, fileName, targetType.getDisplayName()))
    }

    private fun mapStruct(
            group: GroupType,
            parquetName: String,
            target: DucklakeColumn,
            rowType: RowType,
            childrenByParent: Map<Long, Map<String, DucklakeColumn>>,
    ): DucklakeNameMapEntry {
        val children: Map<String, DucklakeColumn> = childrenByParent.getOrDefault(target.columnId, mapOf())
        val childEntries: MutableList<DucklakeNameMapEntry> = ArrayList()
        for (field in group.getFields()) {
            val fieldName = field.getName()
            val childTarget: DucklakeColumn? = children.get(fieldName.lowercase(Locale.ROOT))
            if (childTarget == null) {
                if (ignoreExtraColumns) {
                    // Advance past the skipped struct child's parquet leaves.
                    leafCounter[0] += countParquetLeaves(field)
                    continue
                }
                throw DucklakeAddFilesException(String.format(
                        "Struct field \"%s.%s\" exists in file \"%s\" but was not found in table \"%s\". "
                                + "Set ignore_extra_columns => true to add the file anyway",
                        parquetName, fieldName, fileName, tableName))
            }
            val childTrinoType: Type? = findRowFieldType(rowType, childTarget.columnName)
            if (childTrinoType == null) {
                // Catalog and Trino-side row type out of sync — shouldn't happen for a
                // freshly converted type. Defensive throw.
                throw DucklakeAddFilesException(String.format(
                        "Internal: struct field \"%s.%s\" not found in target row type for table \"%s\"",
                        parquetName, fieldName, tableName))
            }
            childEntries.add(mapField(field, fieldName, childTarget, childTrinoType, childrenByParent))
        }
        return DucklakeNameMapEntry(parquetName, target.columnId, false, childEntries)
    }

    private fun mapList(
            group: GroupType,
            parquetName: String,
            target: DucklakeColumn,
            arrayType: ArrayType,
            childrenByParent: Map<Long, Map<String, DucklakeColumn>>,
    ): DucklakeNameMapEntry {
        // Parquet LIST: one repeated child group ("list") containing one element field.
        if (group.getFieldCount() != 1) {
            throw DucklakeAddFilesException(String.format(
                    "Column \"%s\" in file \"%s\" has unexpected LIST shape (expected one child group, found %d)",
                    parquetName, fileName, group.getFieldCount()))
        }
        val middle: org.apache.parquet.schema.Type = group.getType(0)
        var elementParquetType: org.apache.parquet.schema.Type = middle
        if (!middle.isPrimitive() && middle.asGroupType().getFieldCount() == 1) {
            elementParquetType = middle.asGroupType().getType(0)
        }
        // Catalog has a single synthetic child for list element.
        val children: Map<String, DucklakeColumn> = childrenByParent.getOrDefault(target.columnId, mapOf())
        if (children.size != 1) {
            throw DucklakeAddFilesException(String.format(
                    "Column \"%s\" in file \"%s\" maps to ARRAY but table side has %d list-element columns",
                    parquetName, fileName, children.size))
        }
        val elementTarget: DucklakeColumn = children.values.iterator().next()
        val childEntry = mapField(elementParquetType, "element", elementTarget,
                arrayType.getElementType(), childrenByParent)
        return DucklakeNameMapEntry(parquetName, target.columnId, false, listOf(childEntry))
    }

    private fun mapMap(
            group: GroupType,
            parquetName: String,
            target: DucklakeColumn,
            mapType: MapType,
            childrenByParent: Map<Long, Map<String, DucklakeColumn>>,
    ): DucklakeNameMapEntry {
        // Parquet MAP: one repeated child group ("key_value") with two fields key, value.
        if (group.getFieldCount() != 1) {
            throw DucklakeAddFilesException(String.format(
                    "Column \"%s\" in file \"%s\" has unexpected MAP shape (expected one child group, found %d)",
                    parquetName, fileName, group.getFieldCount()))
        }
        // Guard the key_value child shape before asGroupType(): a MAP whose single child is a
        // primitive (a malformed/foreign 2-level encoding) would otherwise throw a raw parquet
        // ClassCastException that escapes the caller's DucklakeAddFilesException catch and
        // surfaces as an internal error instead of a clean INVALID_PROCEDURE_ARGUMENT. Mirrors
        // the !isPrimitive() guard mapList() already applies to its middle group.
        if (group.getType(0).isPrimitive()) {
            throw DucklakeAddFilesException(String.format(
                    "Column \"%s\" in file \"%s\" has unexpected MAP shape (key_value child must be a group)",
                    parquetName, fileName))
        }
        val kvGroup: GroupType = group.getType(0).asGroupType()
        if (kvGroup.getFieldCount() != 2) {
            throw DucklakeAddFilesException(String.format(
                    "Column \"%s\" in file \"%s\" has unexpected MAP shape (key_value group must have 2 fields)",
                    parquetName, fileName))
        }
        val children: Map<String, DucklakeColumn> = childrenByParent.getOrDefault(target.columnId, mapOf())
        val keyTarget: DucklakeColumn? = children.get("key")
        val valueTarget: DucklakeColumn? = children.get("value")
        if (keyTarget == null || valueTarget == null) {
            throw DucklakeAddFilesException(String.format(
                    "Column \"%s\" maps to MAP but table side is missing key/value child columns",
                    parquetName))
        }
        val keyEntry = mapField(kvGroup.getType(0), "key", keyTarget,
                mapType.getKeyType(), childrenByParent)
        val valueEntry = mapField(kvGroup.getType(1), "value", valueTarget,
                mapType.getValueType(), childrenByParent)
        return DucklakeNameMapEntry(parquetName, target.columnId, false, listOf(keyEntry, valueEntry))
    }

    /**
     * Best-effort parquet primitive → Trino type. Mirrors upstream's
     * {@code DuckLakeParquetTypeChecker::DeriveLogicalType}. Errors raised here become
     * "Failed to map column..." messages at the call site.
     */
    private fun parquetPrimitiveToTrino(type: PrimitiveType, parquetName: String): Type {
        val annotation: LogicalTypeAnnotation? = type.getLogicalTypeAnnotation()
        if (annotation != null) {
            val result: Optional<Type> = annotation.accept(object : LogicalTypeAnnotationVisitor<Type> {
                override fun visit(a: StringLogicalTypeAnnotation): Optional<Type> {
                    return Optional.of(VarcharType.VARCHAR)
                }

                override fun visit(a: DecimalLogicalTypeAnnotation): Optional<Type> {
                    return Optional.of(DecimalType.createDecimalType(a.getPrecision(), a.getScale()))
                }

                override fun visit(a: IntLogicalTypeAnnotation): Optional<Type> {
                    if (a.isSigned()) {
                        return Optional.of(when (a.getBitWidth()) {
                            8 -> TinyintType.TINYINT
                            16 -> SmallintType.SMALLINT
                            32 -> IntegerType.INTEGER
                            else -> BigintType.BIGINT
                        })
                    }
                    // unsigned widens upstream — pin to next-larger signed type for safety
                    return Optional.of(when (a.getBitWidth()) {
                        8 -> SmallintType.SMALLINT
                        16 -> IntegerType.INTEGER
                        32 -> BigintType.BIGINT
                        else -> DecimalType.createDecimalType(20, 0)
                    })
                }

                override fun visit(a: LogicalTypeAnnotation.DateLogicalTypeAnnotation): Optional<Type> {
                    return Optional.of(DateType.DATE)
                }

                override fun visit(a: TimestampLogicalTypeAnnotation): Optional<Type> {
                    val precision = timeUnitPrecision(a.getUnit())
                    if (a.isAdjustedToUTC()) {
                        return Optional.of(TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision))
                    }
                    return Optional.of(TimestampType.createTimestampType(precision))
                }

                override fun visit(a: TimeLogicalTypeAnnotation): Optional<Type> {
                    val precision = timeUnitPrecision(a.getUnit())
                    return Optional.of(io.trino.spi.type.TimeType.createTimeType(precision))
                }

                override fun visit(a: UUIDLogicalTypeAnnotation): Optional<Type> {
                    return Optional.of(UuidType.UUID)
                }
            })
            if (result.isPresent()) {
                return result.get()
            }
        }
        when (type.getPrimitiveTypeName()) {
            PrimitiveType.PrimitiveTypeName.BOOLEAN -> return BooleanType.BOOLEAN
            PrimitiveType.PrimitiveTypeName.INT32 -> return IntegerType.INTEGER
            PrimitiveType.PrimitiveTypeName.INT64 -> return BigintType.BIGINT
            PrimitiveType.PrimitiveTypeName.INT96 -> return TimestampType.createTimestampType(6)
            PrimitiveType.PrimitiveTypeName.FLOAT -> return RealType.REAL
            PrimitiveType.PrimitiveTypeName.DOUBLE -> return DoubleType.DOUBLE
            PrimitiveType.PrimitiveTypeName.BINARY,
            PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> return VarbinaryType.VARBINARY
            else -> {}
        }
        throw DucklakeAddFilesException(String.format(
                "Unrecognized parquet primitive type %s for column \"%s\" in file \"%s\"",
                type.getPrimitiveTypeName(), parquetName, fileName))
    }

    companion object {
        /**
         * Number of primitive leaves in a parquet schema subtree — i.e., the number
         * of {@code RowGroup.columns} entries this subtree contributes. Used to
         * advance the leaf-index counter past skipped or hive-overridden parquet
         * columns so subsequent stats targets stay aligned with the row group.
         */
        private fun countParquetLeaves(field: org.apache.parquet.schema.Type): Int {
            if (field.isPrimitive()) {
                return 1
            }
            var total = 0
            for (child in field.asGroupType().getFields()) {
                total += countParquetLeaves(child)
            }
            return total
        }

        private fun findRowFieldType(rowType: RowType, fieldName: String): Type? {
            for (field in rowType.getFields()) {
                if (field.getName().isPresent() && field.getName().get().equals(fieldName, ignoreCase = true)) {
                    return field.getType()
                }
            }
            return null
        }

        private fun isNestedType(type: Type): Boolean {
            return type is RowType || type is ArrayType || type is MapType
        }

        private fun toIntFieldIndex(columnId: Long): Int {
            if (columnId > Integer.MAX_VALUE) {
                throw DucklakeAddFilesException("DuckLake column_id exceeds Integer range: " + columnId)
            }
            return columnId.toInt()
        }

        private fun timeUnitPrecision(unit: TimeUnit): Int {
            return when (unit) {
                TimeUnit.MILLIS -> 3
                TimeUnit.MICROS -> 6
                TimeUnit.NANOS -> 9
            }
        }
    }
}
