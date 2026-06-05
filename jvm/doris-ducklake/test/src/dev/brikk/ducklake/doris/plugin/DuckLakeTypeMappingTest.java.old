package dev.brikk.ducklake.doris.plugin;

import org.apache.doris.connector.api.ConnectorType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DuckLakeTypeMappingTest {

    @Test
    void primitivesMapDirectly() {
        assertThat(name("boolean")).isEqualTo("BOOLEAN");
        assertThat(name("int8")).isEqualTo("TINYINT");
        assertThat(name("int16")).isEqualTo("SMALLINT");
        assertThat(name("int32")).isEqualTo("INT");
        assertThat(name("int64")).isEqualTo("BIGINT");
        assertThat(name("float32")).isEqualTo("FLOAT");
        assertThat(name("float64")).isEqualTo("DOUBLE");
        assertThat(name("date")).isEqualTo("DATEV2");
        assertThat(name("varchar")).isEqualTo("STRING");
    }

    @Test
    void unsignedIntsPromoteToNextSignedTypeOrDecimal() {
        assertThat(name("uint8")).isEqualTo("SMALLINT");
        assertThat(name("uint16")).isEqualTo("INT");
        assertThat(name("uint32")).isEqualTo("BIGINT");

        ConnectorType uint64 = DuckLakeTypeMapping.fromDucklakeType("uint64");
        assertThat(uint64.getTypeName()).isEqualTo("DECIMALV3");
        assertThat(uint64.getPrecision()).isEqualTo(20);
        assertThat(uint64.getScale()).isEqualTo(0);

        ConnectorType int128 = DuckLakeTypeMapping.fromDucklakeType("int128");
        assertThat(int128.getTypeName()).isEqualTo("DECIMALV3");
        assertThat(int128.getPrecision()).isEqualTo(38);
    }

    @Test
    void timestampPrecisionsMap() {
        // Doris encodes datetime scale into the precision slot of ConnectorType.
        assertThat(precision("timestamp")).isEqualTo(6);
        assertThat(precision("timestamp_s")).isEqualTo(0);
        assertThat(precision("timestamp_ms")).isEqualTo(3);
        assertThat(precision("timestamp_ns")).isEqualTo(9);
        assertThat(name("timestamptz")).isEqualTo("TIMESTAMPTZV2");
    }

    @Test
    void decimalRoundsTrip() {
        ConnectorType t = DuckLakeTypeMapping.fromDucklakeType("decimal(18,4)");
        assertThat(t.getTypeName()).isEqualTo("DECIMALV3");
        assertThat(t.getPrecision()).isEqualTo(18);
        assertThat(t.getScale()).isEqualTo(4);
    }

    @Test
    void blobAndUuidMapToVarbinary() {
        ConnectorType blob = DuckLakeTypeMapping.fromDucklakeType("blob");
        assertThat(blob.getTypeName()).isEqualTo("VARBINARY");
        ConnectorType uuid = DuckLakeTypeMapping.fromDucklakeType("uuid");
        assertThat(uuid.getTypeName()).isEqualTo("VARBINARY");
        assertThat(uuid.getPrecision()).isEqualTo(16);
    }

    @Test
    void listOfPrimitive() {
        ConnectorType t = DuckLakeTypeMapping.fromDucklakeType("list<int32>");
        assertThat(t.getTypeName()).isEqualTo("ARRAY");
        assertThat(t.getChildren()).hasSize(1);
        assertThat(t.getChildren().get(0).getTypeName()).isEqualTo("INT");
    }

    @Test
    void mapOfStringToBigint() {
        ConnectorType t = DuckLakeTypeMapping.fromDucklakeType("map<varchar, int64>");
        assertThat(t.getTypeName()).isEqualTo("MAP");
        assertThat(t.getChildren()).hasSize(2);
        assertThat(t.getChildren().get(0).getTypeName()).isEqualTo("STRING");
        assertThat(t.getChildren().get(1).getTypeName()).isEqualTo("BIGINT");
    }

    @Test
    void structOfFields() {
        ConnectorType t = DuckLakeTypeMapping.fromDucklakeType("struct<id:int32, name:varchar>");
        assertThat(t.getTypeName()).isEqualTo("STRUCT");
        assertThat(t.getFieldNames()).containsExactly("id", "name");
        assertThat(t.getChildren()).extracting(ConnectorType::getTypeName)
                .containsExactly("INT", "STRING");
    }

    @Test
    void nestedListInsideStruct() {
        ConnectorType t = DuckLakeTypeMapping.fromDucklakeType(
                "struct<tags:list<varchar>, scores:map<varchar, float64>>");
        assertThat(t.getTypeName()).isEqualTo("STRUCT");
        assertThat(t.getChildren().get(0).getTypeName()).isEqualTo("ARRAY");
        assertThat(t.getChildren().get(0).getChildren().get(0).getTypeName()).isEqualTo("STRING");
        assertThat(t.getChildren().get(1).getTypeName()).isEqualTo("MAP");
    }

    @Test
    void unsupportedTypeThrows() {
        assertThatThrownBy(() -> DuckLakeTypeMapping.fromDucklakeType("frobnicate"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("frobnicate");
    }

    private static String name(String dlType) {
        return DuckLakeTypeMapping.fromDucklakeType(dlType).getTypeName();
    }

    private static int precision(String dlType) {
        return DuckLakeTypeMapping.fromDucklakeType(dlType).getPrecision();
    }
}
