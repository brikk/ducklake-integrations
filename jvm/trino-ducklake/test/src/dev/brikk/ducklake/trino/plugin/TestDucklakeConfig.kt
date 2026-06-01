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

import dev.brikk.ducklake.trino.plugin.DucklakeTemporalPartitionEncoding.CALENDAR
import dev.brikk.ducklake.trino.plugin.DucklakeTemporalPartitionEncoding.EPOCH
import io.airlift.units.DataSize
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class TestDucklakeConfig {
    @Test
    fun testTemporalPartitionEncodingDefaults() {
        val config = DucklakeConfig()

        assertThat(config.temporalPartitionEncoding).isEqualTo(CALENDAR)
        assertThat(config.isTemporalPartitionEncodingReadLeniency).isTrue()
    }

    @Test
    fun testTemporalPartitionEncodingParsing() {
        val config = DucklakeConfig()
                .setTemporalPartitionEncoding("epoch")
                .setTemporalPartitionEncodingReadLeniency(false)

        assertThat(config.temporalPartitionEncoding).isEqualTo(EPOCH)
        assertThat(config.isTemporalPartitionEncodingReadLeniency).isFalse()
    }

    @Test
    fun testTemporalPartitionEncodingInvalidValueFails() {
        val config = DucklakeConfig()

        assertThatThrownBy { config.setTemporalPartitionEncoding("invalid-encoding") }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("ducklake.temporal-partition-encoding")
    }

    @Test
    fun testDuckdbAutoHttpfsThresholdDefault() {
        // 64 MiB — files smaller than this materialize, larger ones stream via httpfs.
        // The default is small enough to keep typical tests on the materialize path
        // (test files are kilobytes) while large enough that real production scans hit
        // httpfs without any tuning. If a future change moves this default, the tests
        // that rely on threshold-based routing in TestDucklakeDuckDbReadMode will need
        // updating too.
        assertThat(DucklakeConfig().duckdbAutoHttpfsThreshold)
                .isEqualTo(DataSize.ofBytes(64L * 1024 * 1024))
    }

    @Test
    fun testDuckdbAutoHttpfsThresholdParsing() {
        val config = DucklakeConfig()
                .setDuckdbAutoHttpfsThreshold(DataSize.valueOf("128MB"))

        assertThat(config.duckdbAutoHttpfsThreshold.toBytes()).isEqualTo(128L * 1024 * 1024)
    }
}
