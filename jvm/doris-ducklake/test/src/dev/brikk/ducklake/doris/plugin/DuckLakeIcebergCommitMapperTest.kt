package dev.brikk.ducklake.doris.plugin

import org.apache.doris.thrift.TIcebergColumnStats
import org.apache.doris.thrift.TIcebergCommitData
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets

/**
 * Pure-logic coverage of [DuckLakeIcebergCommitMapper] — the BE `TIcebergCommitData`
 * → DuckLake `DucklakeWriteFragment` mapping that the write transaction commits. No
 * catalog / BE needed; bounds are hand-encoded in Iceberg single-value binary.
 */
internal class DuckLakeIcebergCommitMapperTest {

    private fun leInt(value: Int): ByteBuffer {
        val buffer = ByteBuffer.allocate(Int.SIZE_BYTES).order(ByteOrder.LITTLE_ENDIAN)
        buffer.putInt(value)
        buffer.flip()
        return buffer
    }

    private fun leLong(value: Long): ByteBuffer {
        val buffer = ByteBuffer.allocate(Long.SIZE_BYTES).order(ByteOrder.LITTLE_ENDIAN)
        buffer.putLong(value)
        buffer.flip()
        return buffer
    }

    private fun utf8(value: String): ByteBuffer =
        ByteBuffer.wrap(value.toByteArray(StandardCharsets.UTF_8))

    @Test
    fun mapsFileMetadataAndDecodesIntAndStringBounds() {
        val stats = TIcebergColumnStats().apply {
            columnSizes = mapOf(1 to 100L, 2 to 200L)
            valueCounts = mapOf(1 to 10L, 2 to 10L) // Iceberg total counts (incl. nulls)
            nullValueCounts = mapOf(1 to 2L, 2 to 0L)
            lowerBounds = mapOf(1 to leInt(5), 2 to utf8("alice"))
            upperBounds = mapOf(1 to leInt(50), 2 to utf8("zoe"))
        }
        val data = TIcebergCommitData().apply {
            filePath = "sales/orders/part-0.parquet"
            rowCount = 10
            fileSize = 1234
            columnStats = stats
        }

        val fragment = DuckLakeIcebergCommitMapper.toWriteFragment(
            data, mapOf(1L to "int32", 2L to "varchar"),
        )

        assertThat(fragment.path).isEqualTo("sales/orders/part-0.parquet")
        assertThat(fragment.pathIsRelative).isTrue()
        assertThat(fragment.fileFormat).isEqualTo("parquet")
        assertThat(fragment.recordCount).isEqualTo(10)
        assertThat(fragment.fileSizeBytes).isEqualTo(1234)
        assertThat(fragment.footerSize).isZero() // not carried by TIcebergCommitData → catalog stores NULL
        assertThat(fragment.partitionValues).isEmpty()
        assertThat(fragment.partitionId).isNull()

        val byId = fragment.columnStats.associateBy { it.columnId }
        val intCol = byId.getValue(1L)
        assertThat(intCol.columnSizeBytes).isEqualTo(100)
        assertThat(intCol.valueCount).isEqualTo(8) // 10 total - 2 null = 8 non-null
        assertThat(intCol.nullCount).isEqualTo(2)
        assertThat(intCol.minValue).isEqualTo("5")
        assertThat(intCol.maxValue).isEqualTo("50")
        assertThat(intCol.containsNan).isFalse()

        val strCol = byId.getValue(2L)
        assertThat(strCol.valueCount).isEqualTo(10)
        assertThat(strCol.nullCount).isZero()
        assertThat(strCol.minValue).isEqualTo("alice")
        assertThat(strCol.maxValue).isEqualTo("zoe")
    }

    @Test
    fun decodesLongBoundsAndNullsUnsupportedTypes() {
        val stats = TIcebergColumnStats().apply {
            valueCounts = mapOf(3 to 5L, 4 to 5L)
            nullValueCounts = mapOf(3 to 0L, 4 to 0L)
            lowerBounds = mapOf(3 to leLong(1000), 4 to leLong(7))
            upperBounds = mapOf(3 to leLong(9000), 4 to leLong(99))
        }
        val data = TIcebergCommitData().apply {
            filePath = "t/f.parquet"
            rowCount = 5
            columnStats = stats
        }

        val fragment = DuckLakeIcebergCommitMapper.toWriteFragment(
            data, mapOf(3L to "int64", 4L to "float64"),
        )
        val byId = fragment.columnStats.associateBy { it.columnId }

        // int64 → little-endian 8-byte decode.
        assertThat(byId.getValue(3L).minValue).isEqualTo("1000")
        assertThat(byId.getValue(3L).maxValue).isEqualTo("9000")
        // float64 is not a type we decode → bounds null (safe: no pruning), counts still mapped.
        assertThat(byId.getValue(4L).minValue).isNull()
        assertThat(byId.getValue(4L).maxValue).isNull()
        assertThat(byId.getValue(4L).valueCount).isEqualTo(5)
    }

    @Test
    fun dropsStatsForColumnsNotInTheSchema() {
        val stats = TIcebergColumnStats().apply {
            valueCounts = mapOf(1 to 3L, 99 to 3L) // field 99 isn't a known column
            lowerBounds = mapOf(1 to leInt(1), 99 to leInt(1))
        }
        val data = TIcebergCommitData().apply {
            filePath = "t/f.parquet"
            rowCount = 3
            columnStats = stats
        }

        val fragment = DuckLakeIcebergCommitMapper.toWriteFragment(data, mapOf(1L to "int32"))

        assertThat(fragment.columnStats).hasSize(1)
        assertThat(fragment.columnStats.single().columnId).isEqualTo(1L)
    }

    @Test
    fun noColumnStatsYieldsEmptyList() {
        val data = TIcebergCommitData().apply {
            filePath = "t/f.parquet"
            rowCount = 0
        }
        val fragment = DuckLakeIcebergCommitMapper.toWriteFragment(data, emptyMap())
        assertThat(fragment.columnStats).isEmpty()
    }

    @Test
    fun mapsPositionalPartitionValuesByKeyIndex() {
        val data = TIcebergCommitData().apply {
            filePath = "sales/by_region/region=us/part-0.parquet"
            rowCount = 2
            partitionValues = listOf("us", "3")
        }
        val fragment = DuckLakeIcebergCommitMapper.toWriteFragment(data, emptyMap())
        assertThat(fragment.partitionValues).containsExactlyInAnyOrderEntriesOf(
            mapOf(0 to "us", 1 to "3"),
        )
    }

    @Test
    fun stampsBoundPartitionIdOnPartitionedFile() {
        // For a partitioned write the FE binds the active DuckLake spec id; it isn't
        // derivable from the fragment, so the mapper takes it as a parameter.
        val data = TIcebergCommitData().apply {
            filePath = "sales/by_region/region=eu/part-0.parquet"
            rowCount = 2
            partitionValues = listOf("eu")
        }
        val fragment = DuckLakeIcebergCommitMapper.toWriteFragment(
            data, emptyMap(), "sales/by_region", partitionId = 55L,
        )
        assertThat(fragment.partitionId).isEqualTo(55L)
        assertThat(fragment.partitionValues).containsExactlyInAnyOrderEntriesOf(mapOf(0 to "eu"))
    }

    @Test
    fun partitionIdNullForUnpartitionedFile() {
        val data = TIcebergCommitData().apply {
            filePath = "t/f.parquet"
            rowCount = 1
        }
        // No partitionId argument → unpartitioned default.
        val fragment = DuckLakeIcebergCommitMapper.toWriteFragment(data, emptyMap())
        assertThat(fragment.partitionId).isNull()
        assertThat(fragment.partitionValues).isEmpty()
    }

    @Test
    fun relativizesAbsoluteBePathAgainstTableDataDir() {
        // The BE reports an absolute s3:// path under the table dir (with a stray
        // double slash). It must be stored relative, else DuckLake joins it under
        // the table dir again → the doubled-path read-back failure.
        val data = TIcebergCommitData().apply {
            filePath = "s3://ducklake/data/tpch/doris_w//abc-0.zstd.parquet"
            rowCount = 1
        }
        val fragment = DuckLakeIcebergCommitMapper.toWriteFragment(
            data, emptyMap(), "s3://ducklake/data/tpch/doris_w",
        )
        assertThat(fragment.path).isEqualTo("abc-0.zstd.parquet")
        assertThat(fragment.pathIsRelative).isTrue()
    }

    @Test
    fun keepsPathAbsoluteWhenOutsideTheTableDataDir() {
        val data = TIcebergCommitData().apply {
            filePath = "s3://other-bucket/x.parquet"
            rowCount = 1
        }
        val fragment = DuckLakeIcebergCommitMapper.toWriteFragment(
            data, emptyMap(), "s3://ducklake/data/tpch/doris_w",
        )
        assertThat(fragment.path).isEqualTo("s3://other-bucket/x.parquet")
        assertThat(fragment.pathIsRelative).isFalse()
    }
}
