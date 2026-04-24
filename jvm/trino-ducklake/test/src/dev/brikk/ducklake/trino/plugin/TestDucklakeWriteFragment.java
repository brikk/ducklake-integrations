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
package dev.brikk.ducklake.trino.plugin;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import dev.brikk.ducklake.catalog.DucklakeFileColumnStats;
import dev.brikk.ducklake.catalog.DucklakeWriteFragment;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

class TestDucklakeWriteFragment
{
    private static final JsonCodec<DucklakeWriteFragment> FRAGMENT_CODEC =
            new JsonCodecFactory().jsonCodec(DucklakeWriteFragment.class);

    @Test
    void testFragmentRoundTrip()
    {
        DucklakeWriteFragment original = new DucklakeWriteFragment(
                "ducklake-abc123.parquet",
                1024L,
                200L,
                100L,
                List.of(
                        new DucklakeFileColumnStats(1L, 512L, 100L, 5L, Optional.of("1"), Optional.of("99"), false),
                        new DucklakeFileColumnStats(2L, 256L, 100L, 0L, Optional.of("alice"), Optional.of("zulu"), false)));

        String json = FRAGMENT_CODEC.toJson(original);
        DucklakeWriteFragment deserialized = FRAGMENT_CODEC.fromJson(json);

        assertThat(deserialized.path()).isEqualTo(original.path());
        assertThat(deserialized.fileSizeBytes()).isEqualTo(original.fileSizeBytes());
        assertThat(deserialized.recordCount()).isEqualTo(original.recordCount());
        assertThat(deserialized.columnStats()).hasSize(2);
        assertThat(deserialized.columnStats().get(0).columnId()).isEqualTo(1L);
        assertThat(deserialized.columnStats().get(0).nullCount()).isEqualTo(5L);
        assertThat(deserialized.columnStats().get(0).minValue()).isEqualTo(Optional.of("1"));
        assertThat(deserialized.columnStats().get(1).maxValue()).isEqualTo(Optional.of("zulu"));
    }

    @Test
    void testFragmentWithEmptyStats()
    {
        DucklakeWriteFragment original = new DucklakeWriteFragment(
                "ducklake-empty.parquet",
                0L,
                0L,
                0L,
                List.of());

        String json = FRAGMENT_CODEC.toJson(original);
        DucklakeWriteFragment deserialized = FRAGMENT_CODEC.fromJson(json);

        assertThat(deserialized.path()).isEqualTo("ducklake-empty.parquet");
        assertThat(deserialized.recordCount()).isEqualTo(0L);
        assertThat(deserialized.columnStats()).isEmpty();
    }

    @Test
    void testColumnStatsWithNulls()
    {
        DucklakeFileColumnStats stats = new DucklakeFileColumnStats(
                42L, 1024L, 500L, 100L, Optional.empty(), Optional.empty(), true);

        DucklakeWriteFragment original = new DucklakeWriteFragment(
                "ducklake-nan.parquet", 2048L, 300L, 500L, List.of(stats));

        String json = FRAGMENT_CODEC.toJson(original);
        DucklakeWriteFragment deserialized = FRAGMENT_CODEC.fromJson(json);

        DucklakeFileColumnStats roundTripped = deserialized.columnStats().get(0);
        assertThat(roundTripped.columnId()).isEqualTo(42L);
        assertThat(roundTripped.minValue()).isEmpty();
        assertThat(roundTripped.maxValue()).isEmpty();
        assertThat(roundTripped.containsNan()).isTrue();
        assertThat(roundTripped.nullCount()).isEqualTo(100L);
    }

    @Test
    void testFragmentWithPartitionValues()
    {
        DucklakeWriteFragment original = new DucklakeWriteFragment(
                "region=US/ducklake-abc.parquet",
                1024L,
                200L,
                50L,
                List.of(new DucklakeFileColumnStats(1L, 512L, 50L, 0L, Optional.of("1"), Optional.of("50"), false)),
                Map.of(0, "US"),
                OptionalLong.of(42L));

        String json = FRAGMENT_CODEC.toJson(original);
        DucklakeWriteFragment deserialized = FRAGMENT_CODEC.fromJson(json);

        assertThat(deserialized.partitionValues()).isEqualTo(Map.of(0, "US"));
        assertThat(deserialized.partitionId()).isEqualTo(OptionalLong.of(42L));
    }

    @Test
    void testFragmentWithoutPartitionValues()
    {
        // Unpartitioned fragment using convenience constructor
        DucklakeWriteFragment original = new DucklakeWriteFragment(
                "ducklake-nopart.parquet", 1024L, 200L, 50L, List.of());

        String json = FRAGMENT_CODEC.toJson(original);
        DucklakeWriteFragment deserialized = FRAGMENT_CODEC.fromJson(json);

        assertThat(deserialized.partitionValues()).isEmpty();
        assertThat(deserialized.partitionId()).isEmpty();
    }
}
