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

import dev.brikk.ducklake.catalog.DucklakeColumn
import io.trino.parquet.writer.ParquetSchemaConverter
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.DateType.DATE
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.MapType
import io.trino.spi.type.RowType
import io.trino.spi.type.Type
import io.trino.spi.type.TypeOperators
import io.trino.spi.type.VarcharType.VARCHAR
import org.apache.parquet.schema.MessageType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.Optional

internal class TestDucklakeParquetSchemaBuilder {
    @Test
    fun testPrimitiveColumnsGetFieldId() {
        val columns: List<DucklakeColumnHandle> = listOf(
                DucklakeColumnHandle(10, "id", INTEGER, false),
                DucklakeColumnHandle(20, "name", VARCHAR, true),
                DucklakeColumnHandle(30, "price", DOUBLE, true))

        val converter = ParquetSchemaConverter(
                columns.map { it.columnType },
                columns.map { it.columnName },
                false, false)

        val result: MessageType = DucklakeParquetSchemaBuilder.buildMessageType(columns, converter.messageType)

        assertThat(result.fieldCount).isEqualTo(3)
        assertThat(result.getType("id").id.intValue()).isEqualTo(10)
        assertThat(result.getType("name").id.intValue()).isEqualTo(20)
        assertThat(result.getType("price").id.intValue()).isEqualTo(30)
    }

    @Test
    fun testDateAndBigintColumnsGetFieldId() {
        val columns: List<DucklakeColumnHandle> = listOf(
                DucklakeColumnHandle(5, "event_date", DATE, false),
                DucklakeColumnHandle(15, "count", BIGINT, true))

        val converter = ParquetSchemaConverter(
                columns.map { it.columnType },
                columns.map { it.columnName },
                false, false)

        val result: MessageType = DucklakeParquetSchemaBuilder.buildMessageType(columns, converter.messageType)

        assertThat(result.getType("event_date").id.intValue()).isEqualTo(5)
        assertThat(result.getType("count").id.intValue()).isEqualTo(15)
    }

    @Test
    fun testRowTypeGetsFieldId() {
        val rowType = RowType.from(listOf(
                RowType.Field(Optional.of("key"), VARCHAR),
                RowType.Field(Optional.of("value"), INTEGER)))

        val columns: List<DucklakeColumnHandle> = listOf(
                DucklakeColumnHandle(100, "metadata", rowType, true))

        // Nested columns: parentColumn=100
        val allColumns: List<DucklakeColumn> = listOf(
                DucklakeColumn(100, 1, null, 1, 1, "metadata", "STRUCT(key VARCHAR, value INTEGER)", true, null),
                DucklakeColumn(101, 1, null, 1, 2, "key", "VARCHAR", true, 100L),
                DucklakeColumn(102, 1, null, 1, 3, "value", "INTEGER", true, 100L))

        val converter = ParquetSchemaConverter(
                columns.map { it.columnType },
                columns.map { it.columnName },
                false, false)

        val result: MessageType = DucklakeParquetSchemaBuilder.buildMessageType(columns, allColumns, converter.messageType)

        // Top-level field has ID 100
        assertThat(result.getType("metadata").id.intValue()).isEqualTo(100)
        // Nested fields have their own IDs
        val group: org.apache.parquet.schema.GroupType = result.getType("metadata").asGroupType()
        assertThat(group.getType("key").id.intValue()).isEqualTo(101)
        assertThat(group.getType("value").id.intValue()).isEqualTo(102)
    }

    @Test
    fun testArrayTypeGetsFieldId() {
        val arrayType: Type = ArrayType(VARCHAR)

        val columns: List<DucklakeColumnHandle> = listOf(
                DucklakeColumnHandle(50, "tags", arrayType, true))

        val converter = ParquetSchemaConverter(
                columns.map { it.columnType },
                columns.map { it.columnName },
                false, false)

        val result: MessageType = DucklakeParquetSchemaBuilder.buildMessageType(columns, converter.messageType)

        // Top-level array field has ID
        assertThat(result.getType("tags").id.intValue()).isEqualTo(50)
    }

    @Test
    fun testMapTypeGetsFieldId() {
        val mapType: Type = MapType(VARCHAR, INTEGER, TYPE_OPERATORS)

        val columns: List<DucklakeColumnHandle> = listOf(
                DucklakeColumnHandle(75, "properties", mapType, true))

        val converter = ParquetSchemaConverter(
                columns.map { it.columnType },
                columns.map { it.columnName },
                false, false)

        val result: MessageType = DucklakeParquetSchemaBuilder.buildMessageType(columns, converter.messageType)

        // Top-level map field has ID
        assertThat(result.getType("properties").id.intValue()).isEqualTo(75)
    }

    @Test
    fun testSchemaPreservesTypeInformation() {
        val columns: List<DucklakeColumnHandle> = listOf(
                DucklakeColumnHandle(1, "id", INTEGER, false),
                DucklakeColumnHandle(2, "name", VARCHAR, true))

        val converter = ParquetSchemaConverter(
                columns.map { it.columnType },
                columns.map { it.columnName },
                false, false)

        val original: MessageType = converter.messageType
        val annotated: MessageType = DucklakeParquetSchemaBuilder.buildMessageType(columns, original)

        // Field count preserved
        assertThat(annotated.fieldCount).isEqualTo(original.fieldCount)
        // Field names preserved
        assertThat(annotated.getType("id").name).isEqualTo("id")
        assertThat(annotated.getType("name").name).isEqualTo("name")
        // Primitive types preserved
        assertThat(annotated.getType("id").asPrimitiveType().primitiveTypeName)
                .isEqualTo(original.getType("id").asPrimitiveType().primitiveTypeName)
    }

    companion object {
        private val TYPE_OPERATORS = TypeOperators()
    }
}
