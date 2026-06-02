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

import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression coverage for upstream <a
 * href="https://github.com/duckdb/ducklake/issues/1084">duckdb/ducklake#1084</a>.
 *
 * <p>Upstream symptom: their C++ reader merges parquet delete positions and
 * inlined delete positions into a sorted vector <em>without deduplication</em>,
 * and the per-position filter only advances on exact match — so a single
 * duplicate position causes every later tombstoned position in the same vector
 * chunk to leak through.
 *
 * <p>Our reader merges the two sources into a {@link Set}, which is intrinsically
 * dedup'd, and the filter checks via {@code contains(...)} per position — no
 * sorted-index advance, no "stuck on duplicate" failure mode. These tests pin
 * that contract so a future refactor that switches the merge to a sorted list
 * (or an algorithm that depends on it) catches the regression here.
 */
final class TestDeleteRowFilterTransformOverlap
{
    @Test
    void mergedOverlapDoesNotLeakSubsequentTombstones()
    {
        // Simulates the actual merge in DucklakePageSourceProvider.applyDeleteFile:
        // inlinedDeletedRowPositions on the split contributes {5, 10},
        // a parquet delete file contributes {10, 15} — overlap on position 10.
        Set<Long> inlinedDeletes = Set.of(5L, 10L);
        Set<Long> parquetDeletes = Set.of(10L, 15L);
        Set<Long> merged = new HashSet<>(inlinedDeletes);
        merged.addAll(parquetDeletes);
        assertThat(merged)
                .as("HashSet merge dedupes overlapping positions")
                .containsExactlyInAnyOrder(5L, 10L, 15L);

        DucklakePageSourceProvider.DeleteRowFilterTransform filter =
                new DucklakePageSourceProvider.DeleteRowFilterTransform(merged, 0L);

        SourcePage page = SourcePage.create(20);
        SourcePage filtered = filter.apply(page);

        // The smoking gun: position 15 must be dropped. Under the upstream C++
        // bug the duplicate at position 10 wedges the sorted-index pointer and
        // every later tombstoned position leaks.
        assertThat(filtered.getPositionCount())
                .as("all three distinct tombstoned positions must be dropped, "
                        + "regardless of overlap between the two delete sources")
                .isEqualTo(20 - 3);
    }

    @Test
    void overlapDoesNotLeakWhenRowIdStartIsNonZero()
    {
        // Same scenario with rowIdStart != 0 — exercises the global-rowId branch
        // of the filter. The split's parquet delete file stores global row IDs
        // (rowIdStart + offset); inlined deletes store file-local offsets. Both
        // branches in DeleteRowFilterTransform.apply must respect the overlap.
        long rowIdStart = 1000L;
        Set<Long> merged = new HashSet<>();
        merged.add(5L);                  // file-local offset (inlined)
        merged.add(rowIdStart + 10L);    // global row id (parquet)
        merged.add(10L);                 // file-local offset that ALIASES the parquet id
        merged.add(rowIdStart + 15L);    // global row id (parquet)

        DucklakePageSourceProvider.DeleteRowFilterTransform filter =
                new DucklakePageSourceProvider.DeleteRowFilterTransform(merged, rowIdStart);

        SourcePage page = SourcePage.create(20);
        SourcePage filtered = filter.apply(page);

        // Three distinct logical positions (5, 10, 15) are tombstoned; the
        // duplicate-encoded 10 (once as offset, once as rowIdStart+10) shouldn't
        // cause undercount or overcount.
        assertThat(filtered.getPositionCount()).isEqualTo(20 - 3);
    }
}
