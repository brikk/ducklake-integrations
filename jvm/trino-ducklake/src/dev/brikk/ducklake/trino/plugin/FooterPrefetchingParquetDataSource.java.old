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

import com.google.common.collect.ListMultimap;
import io.airlift.slice.Slice;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.reader.ChunkedInputStream;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Wraps a {@link ParquetDataSource} and pre-fetches the exact Parquet footer bytes
 * given a footer-size hint from the DuckLake catalog (ducklake_data_file.footer_size /
 * ducklake_delete_file.footer_size). The first {@code readTail} call returns the
 * pre-fetched tail, letting Trino's {@link io.trino.parquet.reader.MetadataReader} skip
 * its default 48 KB blind read — and avoid the fallback re-read when the actual footer
 * exceeds 48 KB. If the hint is wrong (too small), the caller's re-read with the correct
 * size falls through to the delegate, matching today's behavior at no extra cost.
 *
 * <p>The wrapper is intended to be short-lived, used only for the single
 * {@code MetadataReader.readFooter()} invocation on a file. After that, the pre-fetched
 * tail is dropped and further {@code readTail} calls delegate directly. Non-tail reads
 * ({@code readFully}, {@code planRead}) always delegate.
 */
final class FooterPrefetchingParquetDataSource
        implements ParquetDataSource
{
    // Matches Trino's MetadataReader.POST_SCRIPT_SIZE (4-byte metadata length + 4-byte
    // "PAR1"/"PARE" magic). DuckLake's footer_size is the Thrift FileMetaData length only
    // (see tables/ducklake_data_file.md); the post-script sits immediately after.
    private static final int POST_SCRIPT_SIZE = 8;

    private final ParquetDataSource delegate;
    private Slice prefetchedTail;

    static ParquetDataSource wrapIfHintUsable(ParquetDataSource delegate, long footerSizeHint)
            throws IOException
    {
        requireNonNull(delegate, "delegate is null");
        if (footerSizeHint <= 0) {
            return delegate;
        }
        long completeFooterSize = footerSizeHint + POST_SCRIPT_SIZE;
        long estimatedSize = delegate.getEstimatedSize();
        // A hint that doesn't fit inside the file is obviously stale; fall back silently.
        if (completeFooterSize > estimatedSize || completeFooterSize > Integer.MAX_VALUE) {
            return delegate;
        }
        Slice prefetched = delegate.readTail((int) completeFooterSize);
        return new FooterPrefetchingParquetDataSource(delegate, prefetched);
    }

    private FooterPrefetchingParquetDataSource(ParquetDataSource delegate, Slice prefetchedTail)
    {
        this.delegate = delegate;
        this.prefetchedTail = requireNonNull(prefetchedTail, "prefetchedTail is null");
    }

    @Override
    public ParquetDataSourceId getId()
    {
        return delegate.getId();
    }

    @Override
    public long getReadBytes()
    {
        return delegate.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public long getEstimatedSize()
    {
        return delegate.getEstimatedSize();
    }

    @Override
    public Slice readTail(int length)
            throws IOException
    {
        Slice cached = prefetchedTail;
        if (cached != null) {
            // Single-shot: only the first readTail uses the cache. Any re-read from
            // MetadataReader (footer larger than our hint) must go to the delegate.
            prefetchedTail = null;
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
            return cached;
        }
        return delegate.readTail(length);
    }

    @Override
    public Slice readFully(long position, int length)
            throws IOException
    {
        return delegate.readFully(position, length);
    }

    @Override
    public <K> Map<K, ChunkedInputStream> planRead(ListMultimap<K, DiskRange> diskRanges, AggregatedMemoryContext memoryContext)
    {
        return delegate.planRead(diskRanges, memoryContext);
    }

    @Override
    public Metrics getMetrics()
    {
        return delegate.getMetrics();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }
}
