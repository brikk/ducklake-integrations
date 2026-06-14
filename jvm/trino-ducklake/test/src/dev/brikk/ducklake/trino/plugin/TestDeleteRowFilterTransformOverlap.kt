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

import io.trino.spi.connector.SourcePage
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Pins [DucklakePageSourceProvider.DeleteRowFilterTransform]'s two-vocabulary contract:
 * global row ids and file-local offsets are matched ONLY under their own interpretation.
 *
 * Two regressions are guarded:
 *
 *  1. **Vocabulary aliasing** — global ids numerically overlap local offsets whenever a
 *     file's `rowIdStart < recordCount`. The previous single-set design checked every value
 *     under both interpretations and phantom-deleted rows (first observed as the
 *     nondeterministic spread-delete failure in [AbstractDucklakeRowLevelFormatTest]).
 *  2. **Duplicate overlap** (upstream
 *     [duckdb/ducklake#1084](https://github.com/duckdb/ducklake/issues/1084)) — their C++
 *     reader merges delete positions into a sorted vector *without dedup* and the filter
 *     wedges on the duplicate, leaking every later tombstone. Our per-vocabulary [Set]s
 *     dedup intrinsically and the same logical row encoded in both vocabularies must drop
 *     exactly once.
 */
internal class TestDeleteRowFilterTransformOverlap {
    @Test
    fun vocabulariesDoNotCrossMatch() {
        // rowIdStart=1000, 20-row page: offsets 0..19 / global ids 1000..1019.
        // global {5} names offset -995 of this file — nothing here; under the old both-ways
        // check it would phantom-delete offset 5. Same for local {1005}: an offset far past
        // this page, which the old check matched as global id 1005 = offset 5.
        val globalRowIds: Set<Long> = setOf(5L, 1010L)
        val localOffsets: Set<Long> = setOf(15L, 1005L)

        val filter = DucklakePageSourceProvider.DeleteRowFilterTransform(globalRowIds, localOffsets, 1000L)
        val filtered = filter.apply(SourcePage.create(20))

        // Only global 1010 (offset 10) and local 15 may drop — 18 retained, not the old 17.
        assertThat(filtered.positionCount)
                .`as`("each vocabulary must match only under its own interpretation")
                .isEqualTo(20 - 2)
    }

    @Test
    fun mergedOverlapDoesNotLeakSubsequentTombstones() {
        // Simulates the merge in DucklakePageSourceProvider.applyDeleteFile: inlined deletes
        // contribute local {5, 10}, a DuckLake-spec `pos` delete file contributes local
        // {10, 15} — overlap on offset 10 collapses in the set.
        val localOffsets: MutableSet<Long> = hashSetOf(5L, 10L)
        localOffsets.addAll(setOf(10L, 15L))
        assertThat(localOffsets)
                .`as`("HashSet merge dedupes overlapping positions")
                .containsExactlyInAnyOrder(5L, 10L, 15L)

        val filter = DucklakePageSourceProvider.DeleteRowFilterTransform(emptySet(), localOffsets, 0L)
        val filtered = filter.apply(SourcePage.create(20))

        // The smoking gun: offset 15 must be dropped. Under the upstream C++ bug the
        // duplicate at offset 10 wedges the sorted-index pointer and every later
        // tombstoned position leaks.
        assertThat(filtered.positionCount)
                .`as`("all three distinct tombstoned positions must be dropped, "
                        + "regardless of overlap between the two delete sources")
                .isEqualTo(20 - 3)
    }

    @Test
    fun sameRowInBothVocabulariesDropsOnce() {
        // One logical row tombstoned twice — as global id 1010 (Trino `row_id` delete file)
        // and as local offset 10 (inlined delete). It must drop exactly once, and the
        // overlap must not disturb neighbouring tombstones (5 local, 15 global).
        val filter = DucklakePageSourceProvider.DeleteRowFilterTransform(
                setOf(1010L, 1015L),
                setOf(5L, 10L),
                1000L)
        val filtered = filter.apply(SourcePage.create(20))

        assertThat(filtered.positionCount)
                .`as`("offsets 5, 10, 15 are the distinct logical tombstones")
                .isEqualTo(20 - 3)
    }
}
