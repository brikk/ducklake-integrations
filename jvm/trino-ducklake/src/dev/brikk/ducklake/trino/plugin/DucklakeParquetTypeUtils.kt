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
import io.trino.parquet.Field
import io.trino.parquet.GroupField
import io.trino.parquet.ParquetTypeUtils.getArrayElementColumn
import io.trino.parquet.ParquetTypeUtils.getMapKeyValueColumn
import io.trino.parquet.PrimitiveField
import io.trino.spi.type.ArrayType
import io.trino.spi.type.MapType
import io.trino.spi.type.RowType
import io.trino.spi.type.Type
import org.apache.parquet.io.ColumnIO
import org.apache.parquet.io.GroupColumnIO
import org.apache.parquet.io.PrimitiveColumnIO
import org.apache.parquet.schema.Type.Repetition.OPTIONAL
import java.util.Optional

/**
 * Utility class for converting between Ducklake/Trino types and Parquet Field objects.
 * Supports all types: primitives, arrays, maps, structs/rows, and arbitrary nesting.
 */
object DucklakeParquetTypeUtils {
    /**
     * Construct a Parquet Field from a Trino type and Parquet ColumnIO.
     * Recursively handles all nested types (ROW, MAP, ARRAY).
     * Returns Optional.empty() if the columnIO is null (e.g., a struct field missing from an older Parquet file).
     */
    fun constructField(trinoType: Type, columnIO: ColumnIO?): Optional<Field> {
        if (columnIO == null) {
            return Optional.empty()
        }

        val required = columnIO.type.repetition != OPTIONAL
        val repetitionLevel = columnIO.repetitionLevel
        val definitionLevel = columnIO.definitionLevel

        if (trinoType is RowType) {
            val rowType = trinoType
            val groupColumnIO = columnIO as GroupColumnIO
            val rowFields = rowType.fields
            val children = ImmutableList.builder<Optional<Field>>()
            var hasAnyField = false
            for (rowField in rowFields) {
                val fieldName = rowField.name
                        .orElseThrow { IllegalArgumentException("ROW type field must have a name") }
                val childColumnIO = groupColumnIO.getChild(fieldName)
                val childField = constructField(rowField.type, childColumnIO)
                hasAnyField = hasAnyField or childField.isPresent
                children.add(childField)
            }
            if (hasAnyField) {
                return Optional.of(GroupField(trinoType, repetitionLevel, definitionLevel, required, children.build()))
            }
            return Optional.empty()
        }

        if (trinoType is MapType) {
            val mapType = trinoType
            val groupColumnIO = columnIO as GroupColumnIO
            val keyValueColumnIO = getMapKeyValueColumn(groupColumnIO)
            if (keyValueColumnIO.childrenCount != 2) {
                return Optional.empty()
            }
            val keyField = constructField(mapType.keyType, keyValueColumnIO.getChild(0))
            val valueField = constructField(mapType.valueType, keyValueColumnIO.getChild(1))
            return Optional.of(GroupField(trinoType, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)))
        }

        if (trinoType is ArrayType) {
            val arrayType = trinoType
            // TODO(review:after id=correctness-parquet-2level-list-cast): unconditional cast assumes 3-level LIST; legacy 2-level (repeated primitive) crashes
            val groupColumnIO = columnIO as GroupColumnIO
            if (groupColumnIO.childrenCount != 1) {
                return Optional.empty()
            }
            val elementColumnIO = getArrayElementColumn(groupColumnIO.getChild(0))
            val elementField = constructField(arrayType.elementType, elementColumnIO)
            return Optional.of(GroupField(trinoType, repetitionLevel, definitionLevel, required, ImmutableList.of(elementField)))
        }

        // Primitive type
        val primitiveColumnIO = columnIO as PrimitiveColumnIO
        return Optional.of(PrimitiveField(
                trinoType,
                required,
                primitiveColumnIO.columnDescriptor,
                primitiveColumnIO.id))
    }
}
