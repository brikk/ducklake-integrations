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
import io.airlift.slice.Slices
import io.trino.memory.context.AggregatedMemoryContext
import io.trino.parquet.DiskRange
import io.trino.parquet.ParquetDataSource
import io.trino.parquet.ParquetDataSourceId
import io.trino.parquet.reader.ChunkedInputStream
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Unit tests for [FooterPrefetchingParquetDataSource].
 *
 * Uses an in-memory [CountingInMemoryDataSource] so we can assert the byte count
 * actually pulled from the delegate — this is the signal that the hint is doing its job
 * (reads fewer bytes than the default 48 KB blind read, and in one round trip instead of
 * two for oversized footers).
 */
class TestFooterPrefetchingParquetDataSource {
    @Test
    fun testZeroHintFallsThroughUnwrapped() {
        val delegate = CountingInMemoryDataSource(FILE_BYTES)
        val wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, 0L)

        assertThat(wrapped).isSameAs(delegate)
    }

    @Test
    fun testNegativeHintFallsThroughUnwrapped() {
        val delegate = CountingInMemoryDataSource(FILE_BYTES)
        val wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, -5L)

        assertThat(wrapped).isSameAs(delegate)
    }

    @Test
    fun testStaleHintLargerThanFileFallsThroughUnwrapped() {
        val delegate = CountingInMemoryDataSource(FILE_BYTES)
        // Hint larger than the entire file — the catalog row is stale / corrupt; bail out
        // to the default path rather than attempting a nonsensical pre-fetch.
        val wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, (FILE_BYTES.size + 1).toLong())

        assertThat(wrapped).isSameAs(delegate)
    }

    @Test
    fun testHintShrinksFirstReadTailToExactFooter() {
        val delegate = CountingInMemoryDataSource(FILE_BYTES)
        val hintedFooterSize = 2000L // plus 8-byte post-script = 2008 total tail bytes
        val wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, hintedFooterSize)

        // First readTail: MetadataReader asks for the default 48 KB. The wrapper should
        // return the 2008-byte pre-fetch instead — saving 46 KB of I/O on this file.
        val tail = wrapped.readTail(48 * 1024)
        assertThat(tail.length()).isEqualTo(2008)
        assertThat(delegate.readTailBytes).isEqualTo(2008)
        assertThat(delegate.readTailCalls).isEqualTo(1)

        // Verify the returned bytes match the actual file tail.
        val expectedTail = ByteArray(2008)
        System.arraycopy(FILE_BYTES, FILE_BYTES.size - 2008, expectedTail, 0, 2008)
        assertThat(tail.bytes).isEqualTo(expectedTail)
    }

    @Test
    fun testHintLargerThanDefaultAvoidsSecondReadTail() {
        val delegate = CountingInMemoryDataSource(FILE_BYTES)
        // Oversized footer (60 KB) — the default 48 KB blind read would under-fetch and
        // force a second round trip. With the hint, we pre-fetch the right size once.
        val hintedFooterSize = 60 * 1024L
        val wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, hintedFooterSize)

        val tail = wrapped.readTail(48 * 1024)
        assertThat(tail.length()).isEqualTo(60 * 1024 + 8)
        assertThat(delegate.readTailCalls).isEqualTo(1)
    }

    @Test
    fun testSecondReadTailFallsThroughToDelegate() {
        val delegate = CountingInMemoryDataSource(FILE_BYTES)
        val wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, 1000)

        wrapped.readTail(48 * 1024) // consumes the cache
        wrapped.readTail(2048)       // cache was single-shot; must delegate
        assertThat(delegate.readTailCalls).isEqualTo(2)
    }

    @Test
    fun testFirstReadTailReturnsFullCacheRegardlessOfRequestedLength() {
        val delegate = CountingInMemoryDataSource(FILE_BYTES)
        val wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, 2000)

        // MetadataReader calls readTail(min(fileSize, 48 KB)) = 48 KB here. The wrapper
        // must return the full 2008-byte cache (not trim to 48 KB) — returning "more than
        // asked for" is precisely what lets the reader skip its fallback re-read when the
        // actual footer exceeds its default guess. Trino's reader bounds off buffer.length(),
        // not the size it passed in, so a tail-aligned larger slice is always safe.
        val tail = wrapped.readTail(48 * 1024)
        assertThat(tail.length()).isEqualTo(2008)

        val expected = ByteArray(2008)
        System.arraycopy(FILE_BYTES, FILE_BYTES.size - 2008, expected, 0, 2008)
        assertThat(tail.bytes).isEqualTo(expected)
    }

    @Test
    fun testReadFullyAndMetadataAlwaysDelegate() {
        val delegate = CountingInMemoryDataSource(FILE_BYTES)
        val wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, 2000)

        // readFully is not intercepted — column data reads always hit the delegate.
        val chunk = wrapped.readFully(100, 50)
        assertThat(chunk.length()).isEqualTo(50)
        assertThat(delegate.readFullyCalls).isEqualTo(1)

        // Metadata getters pass through.
        assertThat(wrapped.id).isEqualTo(delegate.id)
        assertThat(wrapped.estimatedSize).isEqualTo(delegate.estimatedSize)
    }

    @Test
    fun testCloseDelegates() {
        val delegate = CountingInMemoryDataSource(FILE_BYTES)
        val wrapped = FooterPrefetchingParquetDataSource.wrapIfHintUsable(delegate, 2000)

        wrapped.close()
        assertThat(delegate.closed).isTrue()
    }

    // Minimal in-memory ParquetDataSource used to count I/O. Does not extend
    // AbstractParquetDataSource because the wrapper does not call planRead.
    private class CountingInMemoryDataSource(private val bytes: ByteArray) : ParquetDataSource {
        private val id = ParquetDataSourceId("test")
        var readTailCalls: Int = 0
        var readTailBytes: Long = 0
        var readFullyCalls: Int = 0
        var closed: Boolean = false

        override fun getId(): ParquetDataSourceId {
            return id
        }

        override fun getReadBytes(): Long {
            return readTailBytes
        }

        override fun getReadTimeNanos(): Long {
            return 0
        }

        override fun getEstimatedSize(): Long {
            return bytes.size.toLong()
        }

        override fun readTail(length: Int): Slice {
            readTailCalls++
            val readSize = Math.min(length, bytes.size)
            readTailBytes += readSize.toLong()
            val copy = ByteArray(readSize)
            System.arraycopy(bytes, bytes.size - readSize, copy, 0, readSize)
            return Slices.wrappedBuffer(copy, 0, copy.size)
        }

        override fun readFully(position: Long, length: Int): Slice {
            readFullyCalls++
            val copy = ByteArray(length)
            System.arraycopy(bytes, position.toInt(), copy, 0, length)
            return Slices.wrappedBuffer(copy, 0, copy.size)
        }

        override fun <K> planRead(diskRanges: ListMultimap<K, DiskRange>, memoryContext: AggregatedMemoryContext): Map<K, ChunkedInputStream> {
            throw UnsupportedOperationException("planRead not used in these tests")
        }

        override fun close() {
            closed = true
        }
    }

    companion object {
        // 64 KB of pseudo-random bytes — larger than Trino's default 48 KB footer guess so we
        // can distinguish "the wrapper took over" from "the default path happened to succeed".
        private val FILE_BYTES = generateBytes(64 * 1024)

        private fun generateBytes(length: Int): ByteArray {
            val bytes = ByteArray(length)
            // Deterministic content so assertions on slice equality are stable.
            for (i in 0 until length) {
                bytes[i] = ((i * 31) xor (i ushr 3)).toByte()
            }
            return bytes
        }
    }
}
