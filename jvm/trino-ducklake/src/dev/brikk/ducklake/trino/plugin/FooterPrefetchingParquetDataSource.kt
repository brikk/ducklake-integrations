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

import com.google.common.collect.ListMultimap
import io.airlift.slice.Slice
import io.trino.memory.context.AggregatedMemoryContext
import io.trino.parquet.DiskRange
import io.trino.parquet.ParquetDataSource
import io.trino.parquet.ParquetDataSourceId
import io.trino.parquet.reader.ChunkedInputStream
import io.trino.spi.metrics.Metrics
import java.io.IOException

/**
 * Wraps a [ParquetDataSource] and pre-fetches the exact Parquet footer bytes
 * given a footer-size hint from the DuckLake catalog (ducklake_data_file.footer_size /
 * ducklake_delete_file.footer_size). The first `readTail` call returns the
 * pre-fetched tail, letting Trino's [io.trino.parquet.reader.MetadataReader] skip
 * its default 48 KB blind read — and avoid the fallback re-read when the actual footer
 * exceeds 48 KB. If the hint is wrong (too small), the caller's re-read with the correct
 * size falls through to the delegate, matching today's behavior at no extra cost.
 *
 *
 * The wrapper is intended to be short-lived, used only for the single
 * `MetadataReader.readFooter()` invocation on a file. After that, the pre-fetched
 * tail is dropped and further `readTail` calls delegate directly. Non-tail reads
 * (`readFully`, `planRead`) always delegate.
 */
public class FooterPrefetchingParquetDataSource private constructor(
        private val delegate: ParquetDataSource,
        prefetchedTail: Slice) : ParquetDataSource {
    private var prefetchedTail: Slice? = prefetchedTail

    override fun getId(): ParquetDataSourceId {
        return delegate.id
    }

    override fun getReadBytes(): Long {
        return delegate.readBytes
    }

    override fun getReadTimeNanos(): Long {
        return delegate.readTimeNanos
    }

    override fun getEstimatedSize(): Long {
        return delegate.estimatedSize
    }

    @Throws(IOException::class)
    override fun readTail(length: Int): Slice {
        val cached = prefetchedTail
        if (cached != null) {
            // Single-shot: only the first readTail uses the cache. Any re-read from
            // MetadataReader (footer larger than our hint) must go to the delegate.
            prefetchedTail = null
            // Always return the full cache on the first call, regardless of the requested
            // length. Trino's MetadataReader only uses buffer.length() (not the requested
            // size) to decide whether a re-read is needed — returning a *larger* slice
            // than asked is both safe and exactly what lets it skip the re-read when the
            // true footer is bigger than its default 48 KB guess. Returning a *smaller*
            // slice is also safe: when our hint is correct the cache already covers the
            // whole footer + post-script. The only edge case — a caller asking for fewer
            // bytes than we pre-fetched — shouldn't happen in practice because
            // MetadataReader always requests min(fileSize, 48 KB), and we don't wrap when
            // the hint exceeds the file size.
            return cached
        }
        return delegate.readTail(length)
    }

    @Throws(IOException::class)
    override fun readFully(position: Long, length: Int): Slice {
        return delegate.readFully(position, length)
    }

    override fun <K> planRead(diskRanges: ListMultimap<K, DiskRange>, memoryContext: AggregatedMemoryContext): Map<K, ChunkedInputStream> {
        return delegate.planRead(diskRanges, memoryContext)
    }

    override fun getMetrics(): Metrics {
        return delegate.metrics
    }

    @Throws(IOException::class)
    override fun close() {
        delegate.close()
    }

    companion object {
        // Matches Trino's MetadataReader.POST_SCRIPT_SIZE (4-byte metadata length + 4-byte
        // "PAR1"/"PARE" magic). DuckLake's footer_size is the Thrift FileMetaData length only
        // (see tables/ducklake_data_file.md); the post-script sits immediately after.
        private const val POST_SCRIPT_SIZE: Int = 8

        // TODO(review:after id=eff-paritext-prefetch-no-max-cap): prefetch ignores maxFooterReadSize cap, can read/allocate huge tail
        // TODO(review:after id=lowtail-paritext-hint-overflow): footerSizeHint + POST_SCRIPT_SIZE can overflow to negative long bypassing guards
        @JvmStatic
        @Throws(IOException::class)
        public fun wrapIfHintUsable(delegate: ParquetDataSource, footerSizeHint: Long): ParquetDataSource {
            if (footerSizeHint <= 0) {
                return delegate
            }
            val completeFooterSize = footerSizeHint + POST_SCRIPT_SIZE
            val estimatedSize = delegate.estimatedSize
            // A hint that doesn't fit inside the file is obviously stale; fall back silently.
            if (completeFooterSize > estimatedSize || completeFooterSize > Int.MAX_VALUE) {
                return delegate
            }
            val prefetched = delegate.readTail(completeFooterSize.toInt())
            return FooterPrefetchingParquetDataSource(delegate, prefetched)
        }
    }
}
