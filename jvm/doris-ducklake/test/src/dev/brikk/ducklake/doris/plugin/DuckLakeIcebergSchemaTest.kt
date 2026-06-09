package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakeColumn
import org.apache.iceberg.SchemaParser
import org.apache.iceberg.types.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * Pure-logic coverage of [DuckLakeIcebergSchema] — the DuckLake-columns → iceberg
 * `Schema` builder that produces the write sink's `schema_json`. The key assertion
 * round-trips the schema through iceberg's **own** `SchemaParser` (an independent
 * oracle for well-formedness + field-id preservation), not just self-comparison.
 */
internal class DuckLakeIcebergSchemaTest {

    private fun col(
        id: Long,
        name: String,
        type: String,
        nullable: Boolean = true,
        order: Long = id,
        parent: Long? = null,
    ) = DucklakeColumn(
        columnId = id,
        beginSnapshot = 0,
        endSnapshot = null,
        tableId = 1,
        columnOrder = order,
        columnName = name,
        columnType = type,
        nullsAllowed = nullable,
        parentColumn = parent,
    )

    @Test
    fun buildsSchemaWithFieldIdsEqualToColumnIdsAndRoundTrips() {
        val columns = listOf(
            col(1, "id", "int32", nullable = false),
            col(2, "amount", "float64"),
            col(3, "name", "varchar"),
            col(4, "ts", "timestamp"),
            col(5, "d", "date", nullable = false),
            col(6, "big", "int64"),
            col(7, "dec", "decimal(10,2)"),
        )

        val schema = DuckLakeIcebergSchema.of(columns)

        // field_id == column_id, in column order.
        assertThat(schema.columns().map { it.fieldId() }).containsExactly(1, 2, 3, 4, 5, 6, 7)
        assertThat(schema.findField("id").type()).isEqualTo(Types.IntegerType.get())
        assertThat(schema.findField("id").isRequired).isTrue()
        assertThat(schema.findField("amount").type()).isEqualTo(Types.DoubleType.get())
        assertThat(schema.findField("amount").isOptional).isTrue()
        assertThat(schema.findField("name").type()).isEqualTo(Types.StringType.get())
        assertThat(schema.findField("ts").type()).isEqualTo(Types.TimestampType.withoutZone())
        assertThat(schema.findField("d").type()).isEqualTo(Types.DateType.get())
        assertThat(schema.findField("big").type()).isEqualTo(Types.LongType.get())
        assertThat(schema.findField("dec").type()).isEqualTo(Types.DecimalType.of(10, 2))

        // ORACLE: iceberg's own SchemaParser round-trips the JSON losslessly and
        // preserves field-ids — i.e. schema_json is a valid iceberg schema.
        val json = SchemaParser.toJson(schema)
        val reparsed = SchemaParser.fromJson(json)
        assertThat(reparsed.sameSchema(schema)).isTrue()
        assertThat(reparsed.columns().map { it.fieldId() }).containsExactly(1, 2, 3, 4, 5, 6, 7)
    }

    @Test
    fun keepsOnlyTopLevelColumnsInColumnOrder() {
        val columns = listOf(
            col(2, "b", "int32", order = 2),
            col(1, "a", "varchar", order = 1),
            col(99, "child", "int32", parent = 1L), // nested child row → excluded
        )
        val schema = DuckLakeIcebergSchema.of(columns)
        assertThat(schema.columns().map { it.name() }).containsExactly("a", "b")
    }

    @Test
    fun rejectsNestedAndLossyTypes() {
        for (unsupported in listOf("struct<x:int32>", "list<int32>", "map<varchar,int32>", "uint64", "time")) {
            assertThatThrownBy { DuckLakeIcebergSchema.of(listOf(col(1, "c", unsupported))) }
                .describedAs(unsupported)
                .isInstanceOf(IllegalArgumentException::class.java)
        }
    }
}
