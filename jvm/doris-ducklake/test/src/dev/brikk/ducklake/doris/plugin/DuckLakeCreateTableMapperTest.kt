package dev.brikk.ducklake.doris.plugin

import org.apache.doris.connector.api.ConnectorType
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * Pure-logic coverage of [DuckLakeCreateTableMapper] — Doris [ConnectorType] → DuckLake
 * type string for CREATE TABLE. No catalog needed.
 *
 * The strong oracle is the round-trip against [DuckLakeTypeMapping] (the read-path
 * inverse): for every non-degraded scalar, `toDucklakeType(fromDucklakeType(t)) == t`.
 */
internal class DuckLakeCreateTableMapperTest {

    private fun type(name: String, precision: Int = 0, scale: Int = 0) =
        if (precision == 0 && scale == 0) ConnectorType.of(name) else ConnectorType.of(name, precision, scale)

    @Test
    fun mapsScalarDorisTypesToDucklake() {
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(type("BOOLEAN"))).isEqualTo("boolean")
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(type("TINYINT"))).isEqualTo("int8")
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(type("SMALLINT"))).isEqualTo("int16")
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(type("INT"))).isEqualTo("int32")
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(type("BIGINT"))).isEqualTo("int64")
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(type("LARGEINT"))).isEqualTo("int128")
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(type("FLOAT"))).isEqualTo("float32")
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(type("DOUBLE"))).isEqualTo("float64")
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(type("DECIMALV3", 10, 2))).isEqualTo("decimal(10,2)")
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(type("DATEV2"))).isEqualTo("date")
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(type("VARCHAR"))).isEqualTo("varchar")
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(type("STRING"))).isEqualTo("varchar")
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(type("VARBINARY", 100, 0))).isEqualTo("blob")
    }

    @Test
    fun mapsDatetimeByPrecisionToDucklakeResolution() {
        // DuckLakeTypeMapping carries the fractional-second resolution in the PRECISION
        // slot for DATETIMEV2, so build the types directly (the of(name) shortcut would
        // drop a 0 precision).
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(ConnectorType.of("DATETIMEV2", 0, 0))).isEqualTo("timestamp_s")
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(ConnectorType.of("DATETIMEV2", 3, 0))).isEqualTo("timestamp_ms")
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(ConnectorType.of("DATETIMEV2", 6, 0))).isEqualTo("timestamp")
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(ConnectorType.of("DATETIMEV2", 9, 0))).isEqualTo("timestamp_ns")
        assertThat(DuckLakeCreateTableMapper.toDucklakeType(ConnectorType.of("TIMESTAMPTZV2", 6, 0))).isEqualTo("timestamptz")
    }

    @Test
    fun roundTripsTheNonDegradedCoreAgainstTheReadMapping() {
        // Excludes the read path's deliberately-degraded mappings (int128→DECIMALV3,
        // unsigned promotions, uuid/json/time → other types, and timestamp_ns whose
        // read surface clamps to DATETIMEV2(6) — Doris's max datetime scale — and so
        // write-maps back to "timestamp") which by design don't round-trip — those
        // are DuckLakeTypeMapping choices, not this mapper's. timestamptz is
        // likewise excluded: it reads as (degraded) DATETIMEV2 → write-maps to
        // "timestamp" (BE can't read a TimeStampTz slot; see DuckLakeTypeMapping).
        val core = listOf(
            "boolean", "int8", "int16", "int32", "int64",
            "float32", "float64", "date",
            "timestamp", "timestamp_s", "timestamp_ms",
            "varchar", "blob", "decimal(10,2)",
        )
        for (t in core) {
            val back = DuckLakeCreateTableMapper.toDucklakeType(DuckLakeTypeMapping.fromDucklakeType(t))
            assertThat(back).describedAs("round-trip of %s", t).isEqualTo(t)
        }
    }

    @Test
    fun unsupportedTypesThrow() {
        for (name in listOf("HLL", "BITMAP", "QUANTILE_STATE", "JSON", "VARIANT", "IPV4", "IPV6", "ARRAY")) {
            assertThatThrownBy { DuckLakeCreateTableMapper.toDucklakeType(type(name)) }
                .describedAs("unsupported %s", name)
                .isInstanceOf(IllegalArgumentException::class.java)
        }
    }
}
