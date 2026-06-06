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

import dev.brikk.ducklake.catalog.DucklakeFilePartitionValue
import dev.brikk.ducklake.catalog.DucklakePartitionField
import dev.brikk.ducklake.catalog.DucklakePartitionSpec
import dev.brikk.ducklake.catalog.DucklakePartitionTransform
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.entry
import org.junit.jupiter.api.Test
import java.util.Optional

/**
 * Regression for the partition-evolution mis-fill: identity partition constant-fill must
 * key the stored {@code partition_key_index -> partition_value} pairs through the spec the
 * file was actually WRITTEN under, never the table's currently-active spec. Each spec
 * numbers its keys from 0, and {@code ducklake_file_partition_value} carries no
 * partition_id, so mapping an old-spec file's values through the active spec would surface
 * the wrong column's value as data.
 */
class TestDucklakeIdentityPartitionSpecMismatch {

    private val dataFileId = 7L

    // Active spec (partition_id = 200): identity partition, key index 0 -> column 10.
    private val activeSpec = DucklakePartitionSpec(
        200L,
        1L,
        listOf(DucklakePartitionField(0, 10L, DucklakePartitionTransform.IDENTITY)),
    )

    // The file's stored value: key index 0 -> "OLD". Under the active spec key 0 is column
    // 10, but this file was written under a different (retired) spec where key 0 meant a
    // different column.
    private val partitionValuesByFile = mapOf(
        dataFileId to listOf(DucklakeFilePartitionValue(dataFileId, 0, "OLD")),
    )

    @Test
    fun fileFromForeignSpecIsNotConstantFilled() {
        val result = DucklakeSplitManager.buildIdentityPartitionValues(
            dataFileId,
            100L, // written under retired spec 100, not the active 200
            Optional.of(activeSpec),
            partitionValuesByFile,
        )

        // Must decline to fill rather than map "OLD" into column 10 (the wrong column).
        assertThat(result).isEmpty()
    }

    @Test
    fun fileFromActiveSpecIsConstantFilled() {
        val result = DucklakeSplitManager.buildIdentityPartitionValues(
            dataFileId,
            200L, // same spec as the active one
            Optional.of(activeSpec),
            partitionValuesByFile,
        )

        assertThat(result).containsExactly(entry(10L, "OLD"))
    }

    @Test
    fun fileWithNoPartitionIdTrustsActiveSpec() {
        // Backward-compat: an absent partition_id keeps the prior behavior (trust the
        // active spec). Such files carry no foreign-spec values to mis-map.
        val result = DucklakeSplitManager.buildIdentityPartitionValues(
            dataFileId,
            null,
            Optional.of(activeSpec),
            partitionValuesByFile,
        )

        assertThat(result).containsExactly(entry(10L, "OLD"))
    }
}
