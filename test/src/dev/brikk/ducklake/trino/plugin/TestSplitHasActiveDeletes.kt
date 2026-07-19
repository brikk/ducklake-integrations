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

import io.trino.spi.predicate.TupleDomain
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Pins B3b's predicate-disabling trigger. [DucklakePageSourceProvider] branches on
 * [DucklakePageSourceProvider.splitHasActiveDeletes] when deciding whether to push
 * the query predicate into the parquet reader / DuckDB scan; this test guarantees the
 * branch fires for every form of "active deletes" the catalog produces (external delete
 * files, puffin DV files, and inlined deletes from `ducklake_inlined_delete_*`)
 * and ONLY for those.
 *
 *
 * Without this guard, predicate pushdown prunes row groups and pages inside the
 * underlying parquet file, breaking `RowIdInjectingPageSource` /
 * `DeleteRowFilterTransform`'s cumulative-offset math and masking the wrong rows.
 */
internal class TestSplitHasActiveDeletes {
    @Test
    fun splitWithoutAnyDeletesAllowsPushdown() {
        val split = baseSplit(listOf(), setOf())
        assertThat(DucklakePageSourceProvider.splitHasActiveDeletes(split))
                .`as`("no delete files and no inlined deletes — predicate pushdown is safe")
                .isFalse()
    }

    @Test
    fun splitWithExternalDeleteFileDisablesPushdown() {
        val split = baseSplit(listOf("/data/ducklake-delete-aaa.parquet"), setOf())
        assertThat(DucklakePageSourceProvider.splitHasActiveDeletes(split))
                .`as`("external parquet delete file is present — pushdown would break cumulative-offset math")
                .isTrue()
    }

    @Test
    fun splitWithPuffinDeleteFileDisablesPushdown() {
        val split = baseSplit(listOf("/data/ducklake-aaa-delete.puffin"), setOf())
        assertThat(DucklakePageSourceProvider.splitHasActiveDeletes(split))
                .`as`("puffin DV file is also a delete file — same pruning hazard")
                .isTrue()
    }

    @Test
    fun splitWithInlinedDeletesDisablesPushdown() {
        val split = baseSplit(listOf(), setOf(3L, 7L))
        assertThat(DucklakePageSourceProvider.splitHasActiveDeletes(split))
                .`as`("inlined deletes from ducklake_inlined_delete_* must also disable pushdown")
                .isTrue()
    }

    @Test
    fun splitWithBothExternalAndInlinedDeletesDisablesPushdown() {
        val split = baseSplit(
                listOf("/data/ducklake-delete-aaa.parquet"),
                setOf(3L))
        assertThat(DucklakePageSourceProvider.splitHasActiveDeletes(split)).isTrue()
    }

    private fun baseSplit(deleteFilePaths: List<String>, inlinedDeletedRowPositions: Set<Long>): DucklakeSplit {
        return DucklakeSplit(
                "/data/00000-data.parquet",
                deleteFilePaths,
                /* rowIdStart */ 0L,
                /* recordCount */ 100L,
                /* fileSizeBytes */ 4096L,
                "parquet",
                TupleDomain.all(),
                /* footerSize */ 0L,
                mapOf(),
                mapOf(),
                mapOf(),
                inlinedDeletedRowPositions)
    }
}
