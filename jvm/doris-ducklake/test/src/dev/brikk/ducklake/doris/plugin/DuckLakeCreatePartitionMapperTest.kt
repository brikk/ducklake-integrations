package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import org.apache.doris.connector.api.DorisConnectorException
import org.apache.doris.connector.api.ddl.ConnectorBucketSpec
import org.apache.doris.connector.api.ddl.ConnectorPartitionField
import org.apache.doris.connector.api.ddl.ConnectorPartitionSpec
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * Pure-logic coverage of [DuckLakeCreatePartitionMapper] — the SPI partition/bucket
 * clause → DuckLake [dev.brikk.ducklake.catalog.PartitionFieldSpec] mapping. No catalog
 * needed; the catalog round-trip (CREATE → getPartitionSpecs) lives in [DuckLakeDdlTest].
 */
internal class DuckLakeCreatePartitionMapperTest {

    private fun transformSpec(vararg fields: ConnectorPartitionField) =
        ConnectorPartitionSpec(ConnectorPartitionSpec.Style.TRANSFORM, fields.toList(), emptyList())

    @Test
    fun unpartitionedYieldsNull() {
        assertThat(DuckLakeCreatePartitionMapper.toPartitionFields(null, null)).isNull()
        // An empty bucket spec (no columns) contributes nothing.
        assertThat(
            DuckLakeCreatePartitionMapper.toPartitionFields(null, ConnectorBucketSpec(emptyList(), 0, "doris_default")),
        ).isNull()
    }

    @Test
    fun mapsEveryIcebergTransform() {
        val fields = DuckLakeCreatePartitionMapper.toPartitionFields(
            transformSpec(
                ConnectorPartitionField("a", "identity", emptyList()),
                ConnectorPartitionField("b", "year", emptyList()),
                ConnectorPartitionField("b", "month", emptyList()),
                ConnectorPartitionField("b", "day", emptyList()),
                ConnectorPartitionField("b", "hour", emptyList()),
                ConnectorPartitionField("a", "bucket", listOf(16)),
            ),
            null,
        )!!
        assertThat(fields.map { it.transform }).containsExactly(
            DucklakePartitionTransform.IDENTITY,
            DucklakePartitionTransform.YEAR,
            DucklakePartitionTransform.MONTH,
            DucklakePartitionTransform.DAY,
            DucklakePartitionTransform.HOUR,
            DucklakePartitionTransform.BUCKET,
        )
        assertThat(fields.last().arity).isEqualTo(16)
    }

    @Test
    fun acceptsExplicitMurmur3BucketSpec() {
        // Should a future FE pass an explicit murmur3 / iceberg_bucket DISTRIBUTED BY, honor it.
        for (algorithm in listOf("murmur3", "ICEBERG_BUCKET")) {
            val fields = DuckLakeCreatePartitionMapper.toPartitionFields(
                null,
                ConnectorBucketSpec(listOf("name"), 8, algorithm),
            )!!
            assertThat(fields).singleElement().satisfies({
                assertThat(it.transform).isEqualTo(DucklakePartitionTransform.BUCKET)
                assertThat(it.arity).isEqualTo(8)
                assertThat(it.columnName).isEqualTo("name")
            })
        }
    }

    @Test
    fun rejectsDorisDistribution() {
        assertThatThrownBy {
            DuckLakeCreatePartitionMapper.toPartitionFields(null, ConnectorBucketSpec(listOf("name"), 4, "doris_default"))
        }.isInstanceOf(DorisConnectorException::class.java).hasMessageContaining("DuckLake bucketing")
    }

    @Test
    fun rejectsBucketTransformWithoutArity() {
        assertThatThrownBy {
            DuckLakeCreatePartitionMapper.toPartitionFields(transformSpec(ConnectorPartitionField("a", "bucket", emptyList())), null)
        }.isInstanceOf(DorisConnectorException::class.java).hasMessageContaining("bucket count")
    }

    @Test
    fun rejectsUnknownTransformAndListRange() {
        assertThatThrownBy {
            DuckLakeCreatePartitionMapper.toPartitionFields(transformSpec(ConnectorPartitionField("a", "truncate", listOf(10))), null)
        }.isInstanceOf(DorisConnectorException::class.java).hasMessageContaining("truncate")

        assertThatThrownBy {
            DuckLakeCreatePartitionMapper.toPartitionFields(
                ConnectorPartitionSpec(ConnectorPartitionSpec.Style.RANGE, emptyList(), emptyList()),
                null,
            )
        }.isInstanceOf(DorisConnectorException::class.java).hasMessageContaining("RANGE")
    }
}
