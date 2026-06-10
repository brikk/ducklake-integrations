package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import dev.brikk.ducklake.catalog.PartitionFieldSpec
import org.apache.doris.connector.api.DorisConnectorException
import org.apache.doris.connector.api.ddl.ConnectorBucketSpec
import org.apache.doris.connector.api.ddl.ConnectorPartitionField
import org.apache.doris.connector.api.ddl.ConnectorPartitionSpec

/**
 * Maps a Doris `CREATE TABLE`'s partition / bucket clauses onto DuckLake
 * [PartitionFieldSpec]s — the write-side inverse of [DuckLakeIcebergPartitionSpec]
 * (which is read/sink-side: DuckLake → Iceberg). The catalog assigns each spec a
 * `partition_key_index` by list order, so the returned order is meaningful.
 *
 * Conservative-throw, like [DuckLakeCreateTableMapper]: DuckLake only has the
 * Iceberg transform family (identity / year / month / day / hour / bucket), so
 * anything else is rejected rather than silently dropped or mis-mapped.
 *
 * **Two distinct SPI inputs, and why they're treated differently:**
 * - [ConnectorPartitionSpec] carries the Iceberg-style `PARTITIONED BY (...)`
 *   clause. The FE ([CreateTableInfoToConnectorRequestConverter]) lowercases the
 *   transform function name, so `bucket(16, c)` arrives as `transform="bucket",
 *   transformArgs=[16]`, `year(d)` as `"year"`, a plain column as `"identity"`.
 *   This is the path that produces a *real* DuckLake bucket — DuckLake's bucket is
 *   Iceberg murmur3, proven BE-equivalent by the W2c live smoke.
 * - [ConnectorBucketSpec] carries Doris's `DISTRIBUTED BY` clause. The FE only ever
 *   stamps its algorithm `"doris_default"` (CRC32) or `"doris_random"` — **neither is
 *   murmur3**. Treating Doris hash-distribution as DuckLake bucketing would silently
 *   bucket files by the wrong hash, so we reject any non-murmur3 algorithm and point
 *   the user at `PARTITIONED BY (bucket(N, col))` instead. (We still accept an
 *   explicit murmur3 / iceberg_bucket algorithm should a future FE pass one.)
 */
internal object DuckLakeCreatePartitionMapper {

    /** Bucket algorithms that ARE DuckLake's (Iceberg murmur3) bucketing. */
    private val MURMUR3_BUCKET_ALGORITHMS = setOf("murmur3", "iceberg_bucket")

    /**
     * The combined partition fields for a CREATE TABLE, or `null` if the table is
     * unpartitioned (so the caller passes `null` straight to `catalog.createTable`).
     * `PARTITIONED BY` fields come first (in clause order), then any accepted
     * `DISTRIBUTED BY` bucket columns.
     */
    fun toPartitionFields(
        partitionSpec: ConnectorPartitionSpec?,
        bucketSpec: ConnectorBucketSpec?,
    ): List<PartitionFieldSpec>? {
        val fields = ArrayList<PartitionFieldSpec>()
        if (partitionSpec != null) {
            fields.addAll(fromPartitionSpec(partitionSpec))
        }
        if (bucketSpec != null) {
            fields.addAll(fromBucketSpec(bucketSpec))
        }
        return fields.ifEmpty { null }
    }

    private fun fromPartitionSpec(spec: ConnectorPartitionSpec): List<PartitionFieldSpec> {
        when (spec.style) {
            ConnectorPartitionSpec.Style.IDENTITY,
            ConnectorPartitionSpec.Style.TRANSFORM,
            -> Unit // Iceberg-shaped — supported
            ConnectorPartitionSpec.Style.LIST,
            ConnectorPartitionSpec.Style.RANGE,
            -> throw DorisConnectorException(
                "DuckLake supports only Iceberg-style partitioning " +
                    "(identity / year / month / day / hour / bucket); " +
                    "PARTITION BY ${spec.style} is not supported",
            )
        }
        return spec.fields.map { toFieldSpec(it) }
    }

    private fun toFieldSpec(field: ConnectorPartitionField): PartitionFieldSpec {
        val column = field.columnName
        return when (val transform = field.transform.lowercase()) {
            "identity" -> PartitionFieldSpec(column, DucklakePartitionTransform.IDENTITY)
            "year" -> PartitionFieldSpec(column, DucklakePartitionTransform.YEAR)
            "month" -> PartitionFieldSpec(column, DucklakePartitionTransform.MONTH)
            "day" -> PartitionFieldSpec(column, DucklakePartitionTransform.DAY)
            "hour" -> PartitionFieldSpec(column, DucklakePartitionTransform.HOUR)
            "bucket" -> PartitionFieldSpec(column, DucklakePartitionTransform.BUCKET, bucketArity(field))
            else -> throw DorisConnectorException(
                "partition transform '$transform' on column '$column' is not supported by DuckLake " +
                    "(supported: identity, year, month, day, hour, bucket)",
            )
        }
    }

    /** The `N` in `bucket(N, col)`; the FE puts it first in `transformArgs`. */
    private fun bucketArity(field: ConnectorPartitionField): Int =
        field.transformArgs.firstOrNull()
            ?: throw DorisConnectorException(
                "bucket(${field.columnName}) requires a bucket count, e.g. bucket(16, ${field.columnName})",
            )

    private fun fromBucketSpec(spec: ConnectorBucketSpec): List<PartitionFieldSpec> {
        if (spec.columns.isEmpty()) {
            return emptyList()
        }
        val algorithm = spec.algorithm.lowercase()
        if (algorithm !in MURMUR3_BUCKET_ALGORITHMS) {
            throw DorisConnectorException(
                "DISTRIBUTED BY uses algorithm '${spec.algorithm}', which is Doris-native distribution, " +
                    "not DuckLake bucketing — DuckLake buckets by Iceberg murmur3. " +
                    "Use PARTITIONED BY (bucket(N, col)) instead",
            )
        }
        if (spec.numBuckets <= 0) {
            throw DorisConnectorException("bucket count must be positive, got ${spec.numBuckets}")
        }
        return spec.columns.map { column ->
            PartitionFieldSpec(column, DucklakePartitionTransform.BUCKET, spec.numBuckets)
        }
    }
}
