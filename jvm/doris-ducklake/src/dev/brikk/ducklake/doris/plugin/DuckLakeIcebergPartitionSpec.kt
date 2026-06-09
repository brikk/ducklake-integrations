package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakePartitionField
import dev.brikk.ducklake.catalog.DucklakePartitionSpec
import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema

/**
 * Builds an `org.apache.iceberg.PartitionSpec` from a DuckLake partition spec, so the
 * write sink ([DuckLakeWritePlanProvider]) can carry `partition_specs_json` +
 * `partition_spec_id` — mirroring Doris's native `IcebergTableSink`, which does
 * `setPartitionSpecsJson(transformValues(table.specs(), PartitionSpecParser::toJson))`
 * and `setPartitionSpecId(table.spec().specId())`.
 *
 * Source columns are referenced by NAME, resolved from `column_id == iceberg field_id`
 * via the table schema. iceberg's builder assigns the partition field ids/names
 * itself; the BE reports `partition_values` **positionally**, so field ORDER is what
 * carries meaning — we add fields in `partitionKeyIndex` order, the same order the
 * commit mapper reads `partition_values` back in ([DuckLakeIcebergCommitMapper]).
 *
 * Transforms map 1:1 onto iceberg: IDENTITY / YEAR / MONTH / DAY / HOUR and
 * BUCKET(arity). iceberg validates each transform against the source column type at
 * build time (e.g. temporal transforms require a date/timestamp), so a malformed spec
 * throws here rather than corrupting a write.
 *
 * **The one live-only unknown:** that the BE's Iceberg `bucket` (murmur3 % N) assigns
 * the *same* bucket as DuckLake's writer / our [DuckLakeBucketTransform]. The hashes
 * are spec-identical on paper; only the compose smoke proves it on a real BE.
 */
internal object DuckLakeIcebergPartitionSpec {

    /**
     * Spec id stamped on the single emitted spec. DuckLake has one active partition
     * spec per snapshot, and the BE only needs the `partition_specs_json` map key to
     * equal `partition_spec_id` — it isn't reconciled with anything external (the
     * DuckLake partition id rides the commit fragment, not the Iceberg spec id).
     */
    const val SINK_SPEC_ID: Int = 0

    /**
     * DuckLake partition [spec] + table [schema] → iceberg `PartitionSpec`. [columnNameById]
     * maps `column_id` → column name (column_id == iceberg field_id) to name the source
     * columns. Throws if a partition column id isn't in the schema.
     */
    fun of(
        spec: DucklakePartitionSpec,
        schema: Schema,
        columnNameById: Map<Long, String>,
    ): PartitionSpec {
        val builder = PartitionSpec.builderFor(schema).withSpecId(SINK_SPEC_ID)
        for (field in spec.fields.sortedBy { it.partitionKeyIndex }) {
            val name = columnNameById[field.columnId]
                ?: throw IllegalArgumentException(
                    "partition column id ${field.columnId} is not a column of the table schema",
                )
            addField(builder, field, name)
        }
        return builder.build()
    }

    private fun addField(builder: PartitionSpec.Builder, field: DucklakePartitionField, name: String) {
        when (field.transform) {
            DucklakePartitionTransform.IDENTITY -> builder.identity(name)
            DucklakePartitionTransform.YEAR -> builder.year(name)
            DucklakePartitionTransform.MONTH -> builder.month(name)
            DucklakePartitionTransform.DAY -> builder.day(name)
            DucklakePartitionTransform.HOUR -> builder.hour(name)
            DucklakePartitionTransform.BUCKET -> builder.bucket(
                name,
                requireNotNull(field.arity) { "BUCKET partition field requires an arity" },
            )
        }
    }
}
