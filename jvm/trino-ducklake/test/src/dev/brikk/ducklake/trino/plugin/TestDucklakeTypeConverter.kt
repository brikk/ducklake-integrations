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

import io.trino.spi.TrinoException
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType
import io.trino.spi.type.BooleanType
import io.trino.spi.type.DateType
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType
import io.trino.spi.type.IntegerType
import io.trino.spi.type.RealType
import io.trino.spi.type.RowType
import io.trino.spi.type.SmallintType
import io.trino.spi.type.TimeType
import io.trino.spi.type.TimeWithTimeZoneType
import io.trino.spi.type.TimestampType
import io.trino.spi.type.TimestampWithTimeZoneType
import io.trino.spi.type.TinyintType
import io.trino.spi.type.UuidType
import io.trino.spi.type.VarbinaryType
import io.trino.spi.type.VarcharType
import io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class TestDucklakeTypeConverter {
    private val converter = DucklakeTypeConverter(TESTING_TYPE_MANAGER)

    // ==================== Primitives ====================

    @Test
    fun testPrimitiveTypes() {
        assertThat(converter.toTrinoType("boolean")).isEqualTo(BooleanType.BOOLEAN)
        assertThat(converter.toTrinoType("int8")).isEqualTo(TinyintType.TINYINT)
        assertThat(converter.toTrinoType("int16")).isEqualTo(SmallintType.SMALLINT)
        assertThat(converter.toTrinoType("int32")).isEqualTo(IntegerType.INTEGER)
        assertThat(converter.toTrinoType("int64")).isEqualTo(BigintType.BIGINT)
        assertThat(converter.toTrinoType("float32")).isEqualTo(RealType.REAL)
        assertThat(converter.toTrinoType("float64")).isEqualTo(DoubleType.DOUBLE)
        assertThat(converter.toTrinoType("varchar")).isEqualTo(VarcharType.VARCHAR)
        assertThat(converter.toTrinoType("blob")).isEqualTo(VarbinaryType.VARBINARY)
        assertThat(converter.toTrinoType("uuid")).isEqualTo(UuidType.UUID)
        assertThat(converter.toTrinoType("date")).isEqualTo(DateType.DATE)
    }

    @Test
    fun testUnsignedIntegersWiden() {
        // Documents the read-side widening; write-side range validation lives in
        // DucklakeUnsignedRangeChecker (covered by TestDucklakeUnsignedRangeChecker
        // and TestDucklakeCrossEngineWriteGuards).
        assertThat(converter.toTrinoType("uint8")).isEqualTo(SmallintType.SMALLINT)
        assertThat(converter.toTrinoType("uint16")).isEqualTo(IntegerType.INTEGER)
        assertThat(converter.toTrinoType("uint32")).isEqualTo(BigintType.BIGINT)
        assertThat(converter.toTrinoType("uint64")).isEqualTo(DecimalType.createDecimalType(20, 0))
    }

    @Test
    fun testTimestampPrecisionVariants() {
        assertThat(converter.toTrinoType("timestamp")).isEqualTo(TimestampType.TIMESTAMP_MICROS)
        assertThat(converter.toTrinoType("timestamp_s")).isEqualTo(TimestampType.createTimestampType(0))
        assertThat(converter.toTrinoType("timestamp_ms")).isEqualTo(TimestampType.createTimestampType(3))
        assertThat(converter.toTrinoType("timestamp_ns")).isEqualTo(TimestampType.createTimestampType(9))
    }

    @Test
    fun testCaseInsensitiveAndTrimmed() {
        assertThat(converter.toTrinoType("  INT32  ")).isEqualTo(IntegerType.INTEGER)
        assertThat(converter.toTrinoType("BOOLEAN")).isEqualTo(BooleanType.BOOLEAN)
    }

    // ==================== Decimals ====================

    @Test
    fun testDecimal() {
        assertThat(converter.toTrinoType("decimal(18,4)")).isEqualTo(DecimalType.createDecimalType(18, 4))
        // Tolerant of whitespace around the comma.
        assertThat(converter.toTrinoType("decimal(38, 0)")).isEqualTo(DecimalType.createDecimalType(38, 0))
    }

    // ==================== Geometry — linestring_z rename (DuckLake 1.0) ====================

    @Test
    fun testLinestringZUnderscoreIsVarbinary() {
        // DuckLake 1.0 renamed "linestring z" to "linestring_z".
        // Upstream now writes the underscore form; we must accept it.
        assertThat(converter.toTrinoType("linestring_z")).isEqualTo(VarbinaryType.VARBINARY)
    }

    @Test
    fun testLinestringZLegacySpaceFormStillAccepted() {
        // Pre-1.0 catalogs still use the space form; we keep it for backwards compat.
        assertThat(converter.toTrinoType("linestring z")).isEqualTo(VarbinaryType.VARBINARY)
    }

    @Test
    fun testOtherGeometryTypesAreVarbinary() {
        for (geom in arrayOf(
                "geometry", "point", "linestring", "polygon",
                "multipoint", "multilinestring", "multipolygon", "geometrycollection")) {
            assertThat(converter.toTrinoType(geom))
                    .`as`("geometry type %s should map to VARBINARY", geom)
                    .isEqualTo(VarbinaryType.VARBINARY)
        }
    }

    // ==================== Nested ====================

    @Test
    fun testListOfInt() {
        assertThat(converter.toTrinoType("list<int32>")).isEqualTo(ArrayType(IntegerType.INTEGER))
    }

    @Test
    fun testStructWithTwoFields() {
        val rowType = converter.toTrinoType("struct<a:int32,b:varchar>") as RowType
        assertThat(rowType.fields).hasSize(2)
        assertThat(rowType.fields[0].name).contains("a")
        assertThat(rowType.fields[0].type).isEqualTo(IntegerType.INTEGER)
        assertThat(rowType.fields[1].name).contains("b")
        assertThat(rowType.fields[1].type).isEqualTo(VarcharType.VARCHAR)
    }

    // ==================== 128-bit integers (int128 / uint128) ====================

    @Test
    fun testInt128MapsToDecimal38() {
        // DuckLake's canonical type string is "int128" (maps to DuckDB HUGEINT). Range is
        // roughly ±1.7e38; DECIMAL(38, 0) covers ~±10^38 so all but the narrow edges round-trip.
        assertThat(converter.toTrinoType("int128")).isEqualTo(DecimalType.createDecimalType(38, 0))
    }

    @Test
    fun testUint128MapsToVarchar() {
        // DuckLake's canonical type string is "uint128" (maps to DuckDB UHUGEINT). Range is
        // 0..3.4e38 which exceeds any Trino decimal, so we degrade to VARCHAR (data preserved
        // as text; no numeric operations).
        assertThat(converter.toTrinoType("uint128")).isEqualTo(VarcharType.VARCHAR)
    }

    // ==================== Write-path precision (toDucklakeType) ====================

    @Test
    fun testTimestampWriteMapsSupportedPrecisions() {
        // DuckLake encodes only second/milli/micro/nano timestamps.
        assertThat(converter.toDucklakeType(TimestampType.createTimestampType(0))).isEqualTo("timestamp_s")
        assertThat(converter.toDucklakeType(TimestampType.createTimestampType(3))).isEqualTo("timestamp_ms")
        assertThat(converter.toDucklakeType(TimestampType.createTimestampType(6))).isEqualTo("timestamp")
        assertThat(converter.toDucklakeType(TimestampType.createTimestampType(9))).isEqualTo("timestamp_ns")
    }

    @Test
    fun testTimestampWriteRejectsUnsupportedPrecision() {
        // Precisions DuckLake cannot represent must fail loudly rather than silently collapse to
        // micros (which would round-trip back as TIMESTAMP(6)).
        for (precision in intArrayOf(1, 2, 4, 5, 7, 8)) {
            assertThatThrownBy { converter.toDucklakeType(TimestampType.createTimestampType(precision)) }
                    .`as`("timestamp precision %d should be rejected", precision)
                    .isInstanceOf(TrinoException::class.java)
                    .hasMessageContaining("Unsupported timestamp precision")
        }
    }

    @Test
    fun testTimeWriteAcceptsMicrosRejectsOthers() {
        // DuckLake time/timetz are microsecond-only (precision 6).
        assertThat(converter.toDucklakeType(TimeType.createTimeType(6))).isEqualTo("time")
        assertThat(converter.toDucklakeType(TimeWithTimeZoneType.createTimeWithTimeZoneType(6))).isEqualTo("timetz")
        for (precision in intArrayOf(0, 3, 9)) {
            assertThatThrownBy { converter.toDucklakeType(TimeType.createTimeType(precision)) }
                    .`as`("time precision %d should be rejected", precision)
                    .isInstanceOf(TrinoException::class.java)
                    .hasMessageContaining("Unsupported time precision")
            assertThatThrownBy { converter.toDucklakeType(TimeWithTimeZoneType.createTimeWithTimeZoneType(precision)) }
                    .`as`("time-with-time-zone precision %d should be rejected", precision)
                    .isInstanceOf(TrinoException::class.java)
                    .hasMessageContaining("Unsupported time with time zone precision")
        }
    }

    @Test
    fun testTimestampTzWriteIsMicros() {
        // DuckLake timestamptz is micros-only; every declared precision maps to the single
        // micros storage type (the precision widens to 6 on read). The CTAS short-block
        // mismatch this once caused is handled in beginCreateTable, not by rejecting the type.
        for (precision in intArrayOf(0, 3, 6, 9)) {
            assertThat(converter.toDucklakeType(TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision)))
                    .`as`("tstz precision %d", precision)
                    .isEqualTo("timestamptz")
        }
    }

    // ==================== Unknown type still errors ====================

    @Test
    fun testUnknownTypeThrows() {
        assertThatThrownBy { converter.toTrinoType("nonsense_type") }
                .isInstanceOf(TrinoException::class.java)
                .hasMessageContaining("Unsupported Ducklake type")
    }
}
