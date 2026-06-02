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

import io.trino.parquet.writer.ParquetSchemaConverter;
import dev.brikk.ducklake.catalog.DucklakeColumn;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestDucklakeParquetSchemaBuilder
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Test
    void testPrimitiveColumnsGetFieldId()
    {
        List<DucklakeColumnHandle> columns = List.of(
                new DucklakeColumnHandle(10, "id", INTEGER, false),
                new DucklakeColumnHandle(20, "name", VARCHAR, true),
                new DucklakeColumnHandle(30, "price", DOUBLE, true));

        ParquetSchemaConverter converter = new ParquetSchemaConverter(
                columns.stream().map(DucklakeColumnHandle::columnType).toList(),
                columns.stream().map(DucklakeColumnHandle::columnName).toList(),
                false, false);

        MessageType result = DucklakeParquetSchemaBuilder.buildMessageType(columns, converter.getMessageType());

        assertThat(result.getFieldCount()).isEqualTo(3);
        assertThat(result.getType("id").getId().intValue()).isEqualTo(10);
        assertThat(result.getType("name").getId().intValue()).isEqualTo(20);
        assertThat(result.getType("price").getId().intValue()).isEqualTo(30);
    }

    @Test
    void testDateAndBigintColumnsGetFieldId()
    {
        List<DucklakeColumnHandle> columns = List.of(
                new DucklakeColumnHandle(5, "event_date", DATE, false),
                new DucklakeColumnHandle(15, "count", BIGINT, true));

        ParquetSchemaConverter converter = new ParquetSchemaConverter(
                columns.stream().map(DucklakeColumnHandle::columnType).toList(),
                columns.stream().map(DucklakeColumnHandle::columnName).toList(),
                false, false);

        MessageType result = DucklakeParquetSchemaBuilder.buildMessageType(columns, converter.getMessageType());

        assertThat(result.getType("event_date").getId().intValue()).isEqualTo(5);
        assertThat(result.getType("count").getId().intValue()).isEqualTo(15);
    }

    @Test
    void testRowTypeGetsFieldId()
    {
        RowType rowType = RowType.from(List.of(
                new RowType.Field(Optional.of("key"), VARCHAR),
                new RowType.Field(Optional.of("value"), INTEGER)));

        List<DucklakeColumnHandle> columns = List.of(
                new DucklakeColumnHandle(100, "metadata", rowType, true));

        // Nested columns: parentColumn=100
        List<DucklakeColumn> allColumns = List.of(
                new DucklakeColumn(100, 1, Optional.empty(), 1, 1, "metadata", "STRUCT(key VARCHAR, value INTEGER)", true, Optional.empty()),
                new DucklakeColumn(101, 1, Optional.empty(), 1, 2, "key", "VARCHAR", true, Optional.of(100L)),
                new DucklakeColumn(102, 1, Optional.empty(), 1, 3, "value", "INTEGER", true, Optional.of(100L)));

        ParquetSchemaConverter converter = new ParquetSchemaConverter(
                columns.stream().map(DucklakeColumnHandle::columnType).toList(),
                columns.stream().map(DucklakeColumnHandle::columnName).toList(),
                false, false);

        MessageType result = DucklakeParquetSchemaBuilder.buildMessageType(columns, allColumns, converter.getMessageType());

        // Top-level field has ID 100
        assertThat(result.getType("metadata").getId().intValue()).isEqualTo(100);
        // Nested fields have their own IDs
        org.apache.parquet.schema.GroupType group = result.getType("metadata").asGroupType();
        assertThat(group.getType("key").getId().intValue()).isEqualTo(101);
        assertThat(group.getType("value").getId().intValue()).isEqualTo(102);
    }

    @Test
    void testArrayTypeGetsFieldId()
    {
        Type arrayType = new ArrayType(VARCHAR);

        List<DucklakeColumnHandle> columns = List.of(
                new DucklakeColumnHandle(50, "tags", arrayType, true));

        ParquetSchemaConverter converter = new ParquetSchemaConverter(
                columns.stream().map(DucklakeColumnHandle::columnType).toList(),
                columns.stream().map(DucklakeColumnHandle::columnName).toList(),
                false, false);

        MessageType result = DucklakeParquetSchemaBuilder.buildMessageType(columns, converter.getMessageType());

        // Top-level array field has ID
        assertThat(result.getType("tags").getId().intValue()).isEqualTo(50);
    }

    @Test
    void testMapTypeGetsFieldId()
    {
        Type mapType = new MapType(VARCHAR, INTEGER, TYPE_OPERATORS);

        List<DucklakeColumnHandle> columns = List.of(
                new DucklakeColumnHandle(75, "properties", mapType, true));

        ParquetSchemaConverter converter = new ParquetSchemaConverter(
                columns.stream().map(DucklakeColumnHandle::columnType).toList(),
                columns.stream().map(DucklakeColumnHandle::columnName).toList(),
                false, false);

        MessageType result = DucklakeParquetSchemaBuilder.buildMessageType(columns, converter.getMessageType());

        // Top-level map field has ID
        assertThat(result.getType("properties").getId().intValue()).isEqualTo(75);
    }

    @Test
    void testSchemaPreservesTypeInformation()
    {
        List<DucklakeColumnHandle> columns = List.of(
                new DucklakeColumnHandle(1, "id", INTEGER, false),
                new DucklakeColumnHandle(2, "name", VARCHAR, true));

        ParquetSchemaConverter converter = new ParquetSchemaConverter(
                columns.stream().map(DucklakeColumnHandle::columnType).toList(),
                columns.stream().map(DucklakeColumnHandle::columnName).toList(),
                false, false);

        MessageType original = converter.getMessageType();
        MessageType annotated = DucklakeParquetSchemaBuilder.buildMessageType(columns, original);

        // Field count preserved
        assertThat(annotated.getFieldCount()).isEqualTo(original.getFieldCount());
        // Field names preserved
        assertThat(annotated.getType("id").getName()).isEqualTo("id");
        assertThat(annotated.getType("name").getName()).isEqualTo("name");
        // Primitive types preserved
        assertThat(annotated.getType("id").asPrimitiveType().getPrimitiveTypeName())
                .isEqualTo(original.getType("id").asPrimitiveType().getPrimitiveTypeName());
    }
}
