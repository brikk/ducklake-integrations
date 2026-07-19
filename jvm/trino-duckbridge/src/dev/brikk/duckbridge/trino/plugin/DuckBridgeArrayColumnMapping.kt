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
package dev.brikk.duckbridge.trino.plugin

import io.airlift.slice.Slices
import io.trino.plugin.jdbc.ColumnMapping
import io.trino.plugin.jdbc.ObjectReadFunction
import io.trino.plugin.jdbc.ObjectWriteFunction
import io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.block.ArrayBlockBuilder
import io.trino.spi.block.Block
import io.trino.spi.block.BlockBuilder
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.BooleanType.BOOLEAN
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.Type
import io.trino.spi.type.VarcharType

/**
 * Read-only `ARRAY` column mapping for DuckDB list/array columns (`FLOAT[3]`, `DOUBLE[]`,
 * `VARCHAR[]`, ...). Needed for the lance embedding + tag columns the P5 scan/search PTFs surface —
 * base-jdbc's default `toColumnMapping` rejects `Types.ARRAY`, which is why `lance_scan('...')` over
 * a dataset with an embedding column failed metadata resolution.
 *
 * The DuckDB type name (`<element>[len]` or `<element>[]`) carries the element type; we map the
 * supported scalar element types and read via `java.sql.Array`. Predicate pushdown is disabled for
 * array columns (no meaningful domain over an array), and writes are unsupported (these functions
 * are read paths). The T2 Arrow page source decodes arrays via [DuckBridgeArrowToPageConverter]; this
 * mapping is the JDBC record-set counterpart + the metadata resolver.
 */
object DuckBridgeArrayColumnMapping {
    /** Build an ARRAY [ColumnMapping] from a DuckDB array type name, or null if the element is unsupported. */
    fun fromTypeName(duckDbTypeName: String): ColumnMapping? {
        val elementTypeName = arrayElementTypeName(duckDbTypeName) ?: return null
        val elementType = trinoElementType(elementTypeName) ?: return null
        val arrayType = ArrayType(elementType)
        return ColumnMapping.objectMapping(
            arrayType,
            ObjectReadFunction.of(Block::class.java) { resultSet, columnIndex ->
                val array = resultSet.getArray(columnIndex)
                buildArrayBlock(arrayType, elementType, array?.array as Array<*>?)
            },
            ObjectWriteFunction.of(Block::class.java) { _, _, _ ->
                throw TrinoException(NOT_SUPPORTED, "DuckBridge does not support writing ARRAY columns")
            },
            DISABLE_PUSHDOWN,
        )
    }

    /** `FLOAT[3]` / `DOUBLE[]` → `FLOAT` / `DOUBLE`; null when the name isn't an array shape. */
    private fun arrayElementTypeName(typeName: String): String? {
        val open = typeName.lastIndexOf('[')
        if (open <= 0 || !typeName.endsWith("]")) {
            return null
        }
        return typeName.substring(0, open).trim()
    }

    private fun trinoElementType(elementTypeName: String): Type? =
        when (elementTypeName.uppercase()) {
            "BOOLEAN" -> BOOLEAN
            "TINYINT", "SMALLINT", "INTEGER", "INT" -> INTEGER
            "BIGINT", "HUGEINT" -> BIGINT
            "FLOAT", "REAL" -> REAL
            "DOUBLE" -> DOUBLE
            "VARCHAR", "TEXT" -> VarcharType.VARCHAR
            else -> null
        }

    private fun buildArrayBlock(arrayType: ArrayType, elementType: Type, elements: Array<*>?): Block {
        val builder = arrayType.createBlockBuilder(null, 1) as ArrayBlockBuilder
        if (elements == null) {
            builder.appendNull()
            return arrayType.getObject(builder.build(), 0) as Block
        }
        builder.buildEntry(
            io.trino.spi.block.ArrayValueBuilder<RuntimeException> { elementBuilder: BlockBuilder ->
                for (element in elements) {
                    appendElement(elementType, elementBuilder, element)
                }
            },
        )
        return arrayType.getObject(builder.build(), 0) as Block
    }

    private fun appendElement(elementType: Type, builder: BlockBuilder, element: Any?) {
        if (element == null) {
            builder.appendNull()
            return
        }
        when (elementType) {
            BOOLEAN -> BOOLEAN.writeBoolean(builder, element as Boolean)
            INTEGER -> INTEGER.writeLong(builder, (element as Number).toLong())
            BIGINT -> BIGINT.writeLong(builder, (element as Number).toLong())
            REAL -> REAL.writeLong(builder, java.lang.Float.floatToRawIntBits((element as Number).toFloat()).toLong())
            DOUBLE -> DOUBLE.writeDouble(builder, (element as Number).toDouble())
            is VarcharType -> elementType.writeSlice(builder, Slices.utf8Slice(element.toString()))
            else -> throw TrinoException(NOT_SUPPORTED, "Unsupported array element type: $elementType")
        }
    }
}
