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
import io.airlift.slice.Slices;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.reader.ChunkedInputStream;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FooterPrefetchingParquetDataSource}.
 *
 * <p>Uses an in-memory {@link CountingInMemoryDataSource} so we can assert the byte count
 * actually pulled from the delegate — this is the signal that the hint is doing its job
 * (reads fewer bytes than the default 48 KB blind read, and in one round trip instead of
 * two for oversized footers).
 */
public class TestFooterPrefetchingParquetDataSource
{
    // 64 KB of pseudo-random bytes — larger than Trino's default 48 KB footer guess so we
    // can distinguish "the wrapper took over" from "the default path happened to succeed".
    private static final byte[] FILE_BYTES = generateBytes(64 * 1024);

    @Test
    public void testZeroHintFallsThroughUnwrapped()
            throws IOException
    {
        CountingInMemoryDataSource delegate = new CountingInMemoryDataSource(FILE_BYTES);
        ParquetDataSource wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, 0L);

        assertThat(wrapped).isSameAs(delegate);
    }

    @Test
    public void testNegativeHintFallsThroughUnwrapped()
            throws IOException
    {
        CountingInMemoryDataSource delegate = new CountingInMemoryDataSource(FILE_BYTES);
        ParquetDataSource wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, -5L);

        assertThat(wrapped).isSameAs(delegate);
    }

    @Test
    public void testStaleHintLargerThanFileFallsThroughUnwrapped()
            throws IOException
    {
        CountingInMemoryDataSource delegate = new CountingInMemoryDataSource(FILE_BYTES);
        // Hint larger than the entire file — the catalog row is stale / corrupt; bail out
        // to the default path rather than attempting a nonsensical pre-fetch.
        ParquetDataSource wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, FILE_BYTES.length + 1);

        assertThat(wrapped).isSameAs(delegate);
    }

    @Test
    public void testHintShrinksFirstReadTailToExactFooter()
            throws IOException
    {
        CountingInMemoryDataSource delegate = new CountingInMemoryDataSource(FILE_BYTES);
        long hintedFooterSize = 2000; // plus 8-byte post-script = 2008 total tail bytes
        ParquetDataSource wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, hintedFooterSize);

        // First readTail: MetadataReader asks for the default 48 KB. The wrapper should
        // return the 2008-byte pre-fetch instead — saving 46 KB of I/O on this file.
        Slice tail = wrapped.readTail(48 * 1024);
        assertThat(tail.length()).isEqualTo(2008);
        assertThat(delegate.readTailBytes).isEqualTo(2008);
        assertThat(delegate.readTailCalls).isEqualTo(1);

        // Verify the returned bytes match the actual file tail.
        byte[] expectedTail = new byte[2008];
        System.arraycopy(FILE_BYTES, FILE_BYTES.length - 2008, expectedTail, 0, 2008);
        assertThat(tail.getBytes()).isEqualTo(expectedTail);
    }

    @Test
    public void testHintLargerThanDefaultAvoidsSecondReadTail()
            throws IOException
    {
        CountingInMemoryDataSource delegate = new CountingInMemoryDataSource(FILE_BYTES);
        // Oversized footer (60 KB) — the default 48 KB blind read would under-fetch and
        // force a second round trip. With the hint, we pre-fetch the right size once.
        long hintedFooterSize = 60 * 1024L;
        ParquetDataSource wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, hintedFooterSize);

        Slice tail = wrapped.readTail(48 * 1024);
        assertThat(tail.length()).isEqualTo(60 * 1024 + 8);
        assertThat(delegate.readTailCalls).isEqualTo(1);
    }

    @Test
    public void testSecondReadTailFallsThroughToDelegate()
            throws IOException
    {
        CountingInMemoryDataSource delegate = new CountingInMemoryDataSource(FILE_BYTES);
        ParquetDataSource wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, 1000);

        wrapped.readTail(48 * 1024); // consumes the cache
        wrapped.readTail(2048);       // cache was single-shot; must delegate
        assertThat(delegate.readTailCalls).isEqualTo(2);
    }

    @Test
    public void testFirstReadTailReturnsFullCacheRegardlessOfRequestedLength()
            throws IOException
    {
        CountingInMemoryDataSource delegate = new CountingInMemoryDataSource(FILE_BYTES);
        ParquetDataSource wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, 2000);

        // MetadataReader calls readTail(min(fileSize, 48 KB)) = 48 KB here. The wrapper
        // must return the full 2008-byte cache (not trim to 48 KB) — returning "more than
        // asked for" is precisely what lets the reader skip its fallback re-read when the
        // actual footer exceeds its default guess. Trino's reader bounds off buffer.length(),
        // not the size it passed in, so a tail-aligned larger slice is always safe.
        Slice tail = wrapped.readTail(48 * 1024);
        assertThat(tail.length()).isEqualTo(2008);

        byte[] expected = new byte[2008];
        System.arraycopy(FILE_BYTES, FILE_BYTES.length - 2008, expected, 0, 2008);
        assertThat(tail.getBytes()).isEqualTo(expected);
    }

    @Test
    public void testReadFullyAndMetadataAlwaysDelegate()
            throws IOException
    {
        CountingInMemoryDataSource delegate = new CountingInMemoryDataSource(FILE_BYTES);
        ParquetDataSource wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, 2000);

        // readFully is not intercepted — column data reads always hit the delegate.
        Slice chunk = wrapped.readFully(100, 50);
        assertThat(chunk.length()).isEqualTo(50);
        assertThat(delegate.readFullyCalls).isEqualTo(1);

        // Metadata getters pass through.
        assertThat(wrapped.getId()).isEqualTo(delegate.getId());
        assertThat(wrapped.getEstimatedSize()).isEqualTo(delegate.getEstimatedSize());
    }

    @Test
    public void testCloseDelegates()
            throws IOException
    {
        CountingInMemoryDataSource delegate = new CountingInMemoryDataSource(FILE_BYTES);
        ParquetDataSource wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, 2000);

        wrapped.close();
        assertThat(delegate.closed).isTrue();
    }

    private static byte[] generateBytes(int length)
    {
        byte[] bytes = new byte[length];
        // Deterministic content so assertions on slice equality are stable.
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) ((i * 31) ^ (i >>> 3));
        }
        return bytes;
    }

    // Minimal in-memory ParquetDataSource used to count I/O. Does not extend
    // AbstractParquetDataSource because the wrapper does not call planRead.
    private static final class CountingInMemoryDataSource
            implements ParquetDataSource
    {
        private final ParquetDataSourceId id = new ParquetDataSourceId("test");
        private final byte[] bytes;
        int readTailCalls;
        long readTailBytes;
        int readFullyCalls;
        boolean closed;

        CountingInMemoryDataSource(byte[] bytes)
        {
            this.bytes = bytes;
        }

        @Override
        public ParquetDataSourceId getId()
        {
            return id;
        }

        @Override
        public long getReadBytes()
        {
            return readTailBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public long getEstimatedSize()
        {
            return bytes.length;
        }

        @Override
        public Slice readTail(int length)
        {
            readTailCalls++;
            int readSize = Math.min(length, bytes.length);
            readTailBytes += readSize;
            byte[] copy = new byte[readSize];
            System.arraycopy(bytes, bytes.length - readSize, copy, 0, readSize);
            return Slices.wrappedBuffer(copy);
        }

        @Override
        public Slice readFully(long position, int length)
        {
            readFullyCalls++;
            byte[] copy = new byte[length];
            System.arraycopy(bytes, (int) position, copy, 0, length);
            return Slices.wrappedBuffer(copy);
        }

        @Override
        public <K> Map<K, ChunkedInputStream> planRead(ListMultimap<K, DiskRange> diskRanges, AggregatedMemoryContext memoryContext)
        {
            throw new UnsupportedOperationException("planRead not used in these tests");
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }
}
