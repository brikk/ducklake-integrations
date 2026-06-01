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

import dev.brikk.ducklake.catalog.DucklakeFileColumnStats
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import io.airlift.json.JsonCodec
import io.airlift.json.JsonCodecFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.OptionalLong

internal class TestDucklakeWriteFragment {
    private val fragmentCodec: JsonCodec<DucklakeWriteFragment> =
            JsonCodecFactory().jsonCodec(DucklakeWriteFragment::class.java)

    @Test
    fun testFragmentRoundTrip() {
        val original = DucklakeWriteFragment(
                "ducklake-abc123.parquet",
                1024L,
                200L,
                100L,
                listOf(
                        DucklakeFileColumnStats(1L, 512L, 100L, 5L, Optional.of("1"), Optional.of("99"), false),
                        DucklakeFileColumnStats(2L, 256L, 100L, 0L, Optional.of("alice"), Optional.of("zulu"), false)))

        val json = fragmentCodec.toJson(original)
        val deserialized = fragmentCodec.fromJson(json)

        assertThat(deserialized.path).isEqualTo(original.path)
        assertThat(deserialized.fileSizeBytes).isEqualTo(original.fileSizeBytes)
        assertThat(deserialized.recordCount).isEqualTo(original.recordCount)
        assertThat(deserialized.columnStats).hasSize(2)
        assertThat(deserialized.columnStats[0].columnId).isEqualTo(1L)
        assertThat(deserialized.columnStats[0].nullCount).isEqualTo(5L)
        assertThat(deserialized.columnStats[0].minValue).isEqualTo(Optional.of("1"))
        assertThat(deserialized.columnStats[1].maxValue).isEqualTo(Optional.of("zulu"))
    }

    @Test
    fun testFragmentWithEmptyStats() {
        val original = DucklakeWriteFragment(
                "ducklake-empty.parquet",
                0L,
                0L,
                0L,
                emptyList())

        val json = fragmentCodec.toJson(original)
        val deserialized = fragmentCodec.fromJson(json)

        assertThat(deserialized.path).isEqualTo("ducklake-empty.parquet")
        assertThat(deserialized.recordCount).isEqualTo(0L)
        assertThat(deserialized.columnStats).isEmpty()
    }

    @Test
    fun testColumnStatsWithNulls() {
        val stats = DucklakeFileColumnStats(
                42L, 1024L, 500L, 100L, Optional.empty(), Optional.empty(), true)

        val original = DucklakeWriteFragment(
                "ducklake-nan.parquet", 2048L, 300L, 500L, listOf(stats))

        val json = fragmentCodec.toJson(original)
        val deserialized = fragmentCodec.fromJson(json)

        val roundTripped = deserialized.columnStats[0]
        assertThat(roundTripped.columnId).isEqualTo(42L)
        assertThat(roundTripped.minValue).isEmpty
        assertThat(roundTripped.maxValue).isEmpty
        assertThat(roundTripped.containsNan).isTrue()
        assertThat(roundTripped.nullCount).isEqualTo(100L)
    }

    @Test
    fun testFragmentWithPartitionValues() {
        val original = DucklakeWriteFragment(
                "region=US/ducklake-abc.parquet",
                1024L,
                200L,
                50L,
                listOf(DucklakeFileColumnStats(1L, 512L, 50L, 0L, Optional.of("1"), Optional.of("50"), false)),
                mapOf(0 to "US"),
                OptionalLong.of(42L))

        val json = fragmentCodec.toJson(original)
        val deserialized = fragmentCodec.fromJson(json)

        assertThat(deserialized.partitionValues).isEqualTo(mapOf(0 to "US"))
        assertThat(deserialized.partitionId).isEqualTo(OptionalLong.of(42L))
    }

    @Test
    fun testFragmentWithoutPartitionValues() {
        val original = DucklakeWriteFragment(
                "ducklake-nopart.parquet", 1024L, 200L, 50L, emptyList())

        val json = fragmentCodec.toJson(original)
        val deserialized = fragmentCodec.fromJson(json)

        assertThat(deserialized.partitionValues).isEmpty()
        assertThat(deserialized.partitionId).isEmpty
    }
}
