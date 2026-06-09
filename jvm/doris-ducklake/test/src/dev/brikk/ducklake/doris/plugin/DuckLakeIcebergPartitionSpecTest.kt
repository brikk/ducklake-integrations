package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakePartitionField
import dev.brikk.ducklake.catalog.DucklakePartitionSpec
import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import org.apache.iceberg.PartitionSpecParser
import org.apache.iceberg.Schema
import org.apache.iceberg.types.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * Pure-logic coverage of [DuckLakeIcebergPartitionSpec] — DuckLake partition spec →
 * `org.apache.iceberg.PartitionSpec`. The oracle is iceberg's own `PartitionSpecParser`
 * round-trip (`fromJson(schema, toJson(spec)) == spec`), the same independent-parser
 * approach the schema builder is tested with. No catalog / BE needed.
 *
 * Field-id == column_id; partition columns are referenced by name through the schema.
 */
internal class DuckLakeIcebergPartitionSpecTest {

    // field_id == column_id, matching DuckLakeIcebergSchema.
    private val schema = Schema(
        listOf(
            Types.NestedField.optional(1, "region", Types.StringType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "id", Types.LongType.get()),
        ),
    )
    private val nameById = mapOf(1L to "region", 2L to "name", 3L to "ts", 4L to "id")

    private fun specOf(vararg fields: DucklakePartitionField) =
        DucklakeIcebergSpecBuilt(DucklakePartitionSpec(partitionId = 42L, tableId = 7L, fields = fields.toList()))

    /** Builds the iceberg spec and exposes both it and its parser round-trip. */
    private inner class DucklakeIcebergSpecBuilt(duckSpec: DucklakePartitionSpec) {
        val spec = DuckLakeIcebergPartitionSpec.of(duckSpec, schema, nameById)
        val roundTripped = PartitionSpecParser.fromJson(schema, PartitionSpecParser.toJson(spec))
    }

    @Test
    fun identityTransformRoundTrips() {
        val built = specOf(DucklakePartitionField(0, 1L, DucklakePartitionTransform.IDENTITY))
        assertThat(built.roundTripped).isEqualTo(built.spec) // parser oracle
        assertThat(built.spec.specId()).isEqualTo(DuckLakeIcebergPartitionSpec.SINK_SPEC_ID)
        val field = built.spec.fields().single()
        assertThat(field.sourceId()).isEqualTo(1) // region (field_id == column_id)
        assertThat(field.transform().toString()).isEqualTo("identity")
    }

    @Test
    fun bucketTransformCarriesArity() {
        val built = specOf(DucklakePartitionField(0, 2L, DucklakePartitionTransform.BUCKET, 4))
        assertThat(built.roundTripped).isEqualTo(built.spec)
        val field = built.spec.fields().single()
        assertThat(field.sourceId()).isEqualTo(2) // name
        assertThat(field.transform().toString()).isEqualTo("bucket[4]")
    }

    @Test
    fun temporalTransformsRoundTrip() {
        for ((transform, rendered) in listOf(
            DucklakePartitionTransform.YEAR to "year",
            DucklakePartitionTransform.MONTH to "month",
            DucklakePartitionTransform.DAY to "day",
            DucklakePartitionTransform.HOUR to "hour",
        )) {
            val built = specOf(DucklakePartitionField(0, 3L, transform)) // ts (timestamp)
            assertThat(built.roundTripped).isEqualTo(built.spec)
            assertThat(built.spec.fields().single().transform().toString()).isEqualTo(rendered)
        }
    }

    @Test
    fun fieldsAreOrderedByPartitionKeyIndexNotListOrder() {
        // Supply the fields out of partition-key order; the BE reads partition_values
        // positionally, so the iceberg spec must be in partitionKeyIndex order.
        val built = specOf(
            DucklakePartitionField(1, 2L, DucklakePartitionTransform.BUCKET, 8), // key index 1
            DucklakePartitionField(0, 1L, DucklakePartitionTransform.IDENTITY), // key index 0
        )
        assertThat(built.roundTripped).isEqualTo(built.spec)
        assertThat(built.spec.fields().map { it.sourceId() }).containsExactly(1, 2) // region then name
    }

    @Test
    fun multipleFieldsRoundTrip() {
        val built = specOf(
            DucklakePartitionField(0, 1L, DucklakePartitionTransform.IDENTITY),
            DucklakePartitionField(1, 3L, DucklakePartitionTransform.DAY),
            DucklakePartitionField(2, 2L, DucklakePartitionTransform.BUCKET, 16),
        )
        assertThat(built.roundTripped).isEqualTo(built.spec)
        assertThat(built.spec.fields().map { it.transform().toString() })
            .containsExactly("identity", "day", "bucket[16]")
    }

    @Test
    fun unknownPartitionColumnThrows() {
        assertThatThrownBy {
            DuckLakeIcebergPartitionSpec.of(
                DucklakePartitionSpec(1L, 1L, listOf(DucklakePartitionField(0, 999L, DucklakePartitionTransform.IDENTITY))),
                schema,
                nameById,
            )
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("999")
    }
}
