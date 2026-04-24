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

import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDucklakeTypeConverter
{
    private final DucklakeTypeConverter converter = new DucklakeTypeConverter(TESTING_TYPE_MANAGER);

    // ==================== Primitives ====================

    @Test
    public void testPrimitiveTypes()
    {
        assertThat(converter.toTrinoType("boolean")).isEqualTo(BooleanType.BOOLEAN);
        assertThat(converter.toTrinoType("int8")).isEqualTo(TinyintType.TINYINT);
        assertThat(converter.toTrinoType("int16")).isEqualTo(SmallintType.SMALLINT);
        assertThat(converter.toTrinoType("int32")).isEqualTo(IntegerType.INTEGER);
        assertThat(converter.toTrinoType("int64")).isEqualTo(BigintType.BIGINT);
        assertThat(converter.toTrinoType("float32")).isEqualTo(RealType.REAL);
        assertThat(converter.toTrinoType("float64")).isEqualTo(DoubleType.DOUBLE);
        assertThat(converter.toTrinoType("varchar")).isEqualTo(VarcharType.VARCHAR);
        assertThat(converter.toTrinoType("blob")).isEqualTo(VarbinaryType.VARBINARY);
        assertThat(converter.toTrinoType("uuid")).isEqualTo(UuidType.UUID);
        assertThat(converter.toTrinoType("date")).isEqualTo(DateType.DATE);
    }

    @Test
    public void testUnsignedIntegersWiden()
    {
        // Documents the current mapping. No range validation on writes — see TODO-compatibility.md.
        assertThat(converter.toTrinoType("uint8")).isEqualTo(SmallintType.SMALLINT);
        assertThat(converter.toTrinoType("uint16")).isEqualTo(IntegerType.INTEGER);
        assertThat(converter.toTrinoType("uint32")).isEqualTo(BigintType.BIGINT);
        assertThat(converter.toTrinoType("uint64")).isEqualTo(DecimalType.createDecimalType(20, 0));
    }

    @Test
    public void testTimestampPrecisionVariants()
    {
        assertThat(converter.toTrinoType("timestamp")).isEqualTo(TimestampType.TIMESTAMP_MICROS);
        assertThat(converter.toTrinoType("timestamp_s")).isEqualTo(TimestampType.createTimestampType(0));
        assertThat(converter.toTrinoType("timestamp_ms")).isEqualTo(TimestampType.createTimestampType(3));
        assertThat(converter.toTrinoType("timestamp_ns")).isEqualTo(TimestampType.createTimestampType(9));
    }

    @Test
    public void testCaseInsensitiveAndTrimmed()
    {
        assertThat(converter.toTrinoType("  INT32  ")).isEqualTo(IntegerType.INTEGER);
        assertThat(converter.toTrinoType("BOOLEAN")).isEqualTo(BooleanType.BOOLEAN);
    }

    // ==================== Decimals ====================

    @Test
    public void testDecimal()
    {
        assertThat(converter.toTrinoType("decimal(18,4)")).isEqualTo(DecimalType.createDecimalType(18, 4));
        // Tolerant of whitespace around the comma.
        assertThat(converter.toTrinoType("decimal(38, 0)")).isEqualTo(DecimalType.createDecimalType(38, 0));
    }

    // ==================== Geometry — linestring_z rename (DuckLake 1.0) ====================

    @Test
    public void testLinestringZUnderscoreIsVarbinary()
    {
        // DuckLake 1.0 renamed "linestring z" to "linestring_z".
        // Upstream now writes the underscore form; we must accept it.
        assertThat(converter.toTrinoType("linestring_z")).isEqualTo(VarbinaryType.VARBINARY);
    }

    @Test
    public void testLinestringZLegacySpaceFormStillAccepted()
    {
        // Pre-1.0 catalogs still use the space form; we keep it for backwards compat.
        assertThat(converter.toTrinoType("linestring z")).isEqualTo(VarbinaryType.VARBINARY);
    }

    @Test
    public void testOtherGeometryTypesAreVarbinary()
    {
        for (String geom : new String[] {
                "geometry", "point", "linestring", "polygon",
                "multipoint", "multilinestring", "multipolygon", "geometrycollection"
        }) {
            assertThat(converter.toTrinoType(geom))
                    .as("geometry type %s should map to VARBINARY", geom)
                    .isEqualTo(VarbinaryType.VARBINARY);
        }
    }

    // ==================== Nested ====================

    @Test
    public void testListOfInt()
    {
        assertThat(converter.toTrinoType("list<int32>")).isEqualTo(new ArrayType(IntegerType.INTEGER));
    }

    @Test
    public void testStructWithTwoFields()
    {
        RowType rowType = (RowType) converter.toTrinoType("struct<a:int32,b:varchar>");
        assertThat(rowType.getFields()).hasSize(2);
        assertThat(rowType.getFields().get(0).getName()).contains("a");
        assertThat(rowType.getFields().get(0).getType()).isEqualTo(IntegerType.INTEGER);
        assertThat(rowType.getFields().get(1).getName()).contains("b");
        assertThat(rowType.getFields().get(1).getType()).isEqualTo(VarcharType.VARCHAR);
    }

    // ==================== 128-bit integers (int128 / uint128) ====================

    @Test
    public void testInt128MapsToDecimal38()
    {
        // DuckLake's canonical type string is "int128" (maps to DuckDB HUGEINT). Range is
        // roughly ±1.7e38; DECIMAL(38, 0) covers ~±10^38 so all but the narrow edges round-trip.
        assertThat(converter.toTrinoType("int128")).isEqualTo(DecimalType.createDecimalType(38, 0));
    }

    @Test
    public void testUint128MapsToVarchar()
    {
        // DuckLake's canonical type string is "uint128" (maps to DuckDB UHUGEINT). Range is
        // 0..3.4e38 which exceeds any Trino decimal, so we degrade to VARCHAR (data preserved
        // as text; no numeric operations).
        assertThat(converter.toTrinoType("uint128")).isEqualTo(VarcharType.VARCHAR);
    }

    // ==================== Unknown type still errors ====================

    @Test
    public void testUnknownTypeThrows()
    {
        assertThatThrownBy(() -> converter.toTrinoType("nonsense_type"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Unsupported Ducklake type");
    }
}
