package dev.brikk.ducklake.doris.plugin

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

internal class DuckLakeTypeMappingTest {

    @Test
    fun primitivesMapDirectly() {
        assertThat(name("boolean")).isEqualTo("BOOLEAN")
        assertThat(name("int8")).isEqualTo("TINYINT")
        assertThat(name("int16")).isEqualTo("SMALLINT")
        assertThat(name("int32")).isEqualTo("INT")
        assertThat(name("int64")).isEqualTo("BIGINT")
        assertThat(name("float32")).isEqualTo("FLOAT")
        assertThat(name("float64")).isEqualTo("DOUBLE")
        assertThat(name("date")).isEqualTo("DATEV2")
        assertThat(name("varchar")).isEqualTo("STRING")
    }

    @Test
    fun unsignedIntsPromoteToNextSignedTypeOrDecimal() {
        assertThat(name("uint8")).isEqualTo("SMALLINT")
        assertThat(name("uint16")).isEqualTo("INT")
        assertThat(name("uint32")).isEqualTo("BIGINT")

        val uint64 = DuckLakeTypeMapping.fromDucklakeType("uint64")
        assertThat(uint64.typeName).isEqualTo("DECIMALV3")
        assertThat(uint64.precision).isEqualTo(20)
        assertThat(uint64.scale).isEqualTo(0)

        val int128 = DuckLakeTypeMapping.fromDucklakeType("int128")
        assertThat(int128.typeName).isEqualTo("DECIMALV3")
        assertThat(int128.precision).isEqualTo(38)
    }

    @Test
    fun timestampPrecisionsMap() {
        // Doris encodes datetime scale into the precision slot of ConnectorType.
        assertThat(precision("timestamp")).isEqualTo(6)
        assertThat(precision("timestamp_s")).isEqualTo(0)
        assertThat(precision("timestamp_ms")).isEqualTo(3)
        // nanos clamp to Doris's max datetime scale (6) — documented-lossy, see the
        // mapping's DEGRADED note and DuckLakeTemporalTypeAuditTest.
        assertThat(precision("timestamp_ns")).isEqualTo(6)
        assertThat(name("timestamptz")).isEqualTo("TIMESTAMPTZV2")
    }

    @Test
    fun decimalRoundsTrip() {
        val t = DuckLakeTypeMapping.fromDucklakeType("decimal(18,4)")
        assertThat(t.typeName).isEqualTo("DECIMALV3")
        assertThat(t.precision).isEqualTo(18)
        assertThat(t.scale).isEqualTo(4)
    }

    @Test
    fun blobAndUuidMapToVarbinary() {
        val blob = DuckLakeTypeMapping.fromDucklakeType("blob")
        assertThat(blob.typeName).isEqualTo("VARBINARY")
        val uuid = DuckLakeTypeMapping.fromDucklakeType("uuid")
        assertThat(uuid.typeName).isEqualTo("VARBINARY")
        assertThat(uuid.precision).isEqualTo(16)
    }

    @Test
    fun listOfPrimitive() {
        val t = DuckLakeTypeMapping.fromDucklakeType("list<int32>")
        assertThat(t.typeName).isEqualTo("ARRAY")
        assertThat(t.children).hasSize(1)
        assertThat(t.children[0].typeName).isEqualTo("INT")
    }

    @Test
    fun mapOfStringToBigint() {
        val t = DuckLakeTypeMapping.fromDucklakeType("map<varchar, int64>")
        assertThat(t.typeName).isEqualTo("MAP")
        assertThat(t.children).hasSize(2)
        assertThat(t.children[0].typeName).isEqualTo("STRING")
        assertThat(t.children[1].typeName).isEqualTo("BIGINT")
    }

    @Test
    fun structOfFields() {
        val t = DuckLakeTypeMapping.fromDucklakeType("struct<id:int32, name:varchar>")
        assertThat(t.typeName).isEqualTo("STRUCT")
        assertThat(t.fieldNames).containsExactly("id", "name")
        assertThat(t.children).extracting<String> { it.typeName }
            .containsExactly("INT", "STRING")
    }

    @Test
    fun nestedListInsideStruct() {
        val t = DuckLakeTypeMapping.fromDucklakeType(
            "struct<tags:list<varchar>, scores:map<varchar, float64>>",
        )
        assertThat(t.typeName).isEqualTo("STRUCT")
        assertThat(t.children[0].typeName).isEqualTo("ARRAY")
        assertThat(t.children[0].children[0].typeName).isEqualTo("STRING")
        assertThat(t.children[1].typeName).isEqualTo("MAP")
    }

    @Test
    fun unsupportedTypeThrows() {
        assertThatThrownBy { DuckLakeTypeMapping.fromDucklakeType("frobnicate") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("frobnicate")
    }

    private fun name(dlType: String): String =
        DuckLakeTypeMapping.fromDucklakeType(dlType).typeName

    private fun precision(dlType: String): Int =
        DuckLakeTypeMapping.fromDucklakeType(dlType).precision
}
