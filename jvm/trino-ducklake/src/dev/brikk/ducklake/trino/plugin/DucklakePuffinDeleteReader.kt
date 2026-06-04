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

import io.trino.filesystem.TrinoInputFile
import org.roaringbitmap.RoaringBitmap
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32

/**
 * Reader for DuckLake's {@code .puffin} deletion-vector files. Despite the
 * extension and {@code format='puffin'} metadata column, the file is NOT a
 * standard Iceberg Puffin file — DuckLake writes only the
 * <em>deletion-vector blob bytes</em> to disk, with no surrounding PFA1
 * magic, JSON footer, or blob framing. Mirror of upstream
 * {@code DuckLakeDeleteFilter::ScanDeletionVectorFile} →
 * {@code DuckLakeDeletionVectorData::FromBlob} in
 * {@code vendor/ducklake/src/storage/ducklake_deletion_vector.cpp}.
 *
 * <p>Blob layout (all length fields are 4 bytes):
 * <pre>
 *   bytes 0..3        vector_size              uint32 BIG-endian — length of (magic + bitmap_count + bitmaps),
 *                                              excluding vector_size itself and the trailing CRC
 *   bytes 4..7        magic                    0xD1 0xD3 0x39 0x64
 *   bytes 8..15       bitmap_count             int64 LITTLE-endian (DuckDB's Store/Load is native endian)
 *   for each bitmap:
 *     4 bytes         high_bits                int32 LITTLE-endian — high 32 bits of the row positions
 *                                              represented by the following Roaring bitmap
 *     N bytes         bitmap                   portable 32-bit Roaring serialization (CRoaring spec)
 *   bytes (end-3)..end CRC32                   uint32 BIG-endian — checksum over magic+bitmap_count+bitmaps
 * </pre>
 *
 * <p>Row positions are reconstructed as {@code (high_bits << 32) | (low_bits & 0xFFFFFFFFL)}.
 */
object DucklakePuffinDeleteReader {
    private val DELETION_VECTOR_MAGIC = byteArrayOf(0xD1.toByte(), 0xD3.toByte(), 0x39.toByte(), 0x64.toByte())
    private const val MIN_BLOB_SIZE = 4 + 4 + 8 + 4 // vector_size + magic + bitmap_count + CRC, no bitmaps

    /**
     * Read all deleted row positions from a DuckLake puffin file.
     */
    @Throws(IOException::class)
    fun readDeletedPositions(inputFile: TrinoInputFile): Set<Long> {
        val length = inputFile.length()
        if (length > Int.MAX_VALUE) {
            throw IOException("Puffin delete file is unreasonably large: $length bytes (path=${inputFile.location()})")
        }
        val blob = ByteArray(length.toInt())
        inputFile.newStream().use { `in` ->
            var read = 0
            while (read < blob.size) {
                val n = `in`.read(blob, read, blob.size - read)
                if (n < 0) {
                    throw IOException("Unexpected EOF reading puffin delete file at offset $read of ${blob.size} (path=${inputFile.location()})")
                }
                read += n
            }
        }
        return decodeBlob(blob)
    }

    /**
     * Decode the raw blob bytes into row positions. Visible for testing.
     */
    @Throws(IOException::class)
    fun decodeBlob(blob: ByteArray): Set<Long> {
        if (blob.size < MIN_BLOB_SIZE) {
            throw IOException("Puffin delete blob too small: ${blob.size} bytes (minimum $MIN_BLOB_SIZE)")
        }

        val buf = ByteBuffer.wrap(blob)

        buf.order(ByteOrder.BIG_ENDIAN)
        val vectorSize = Integer.toUnsignedLong(buf.getInt())
        val checksummedStart = buf.position()
        // vectorSize counts magic + bitmap_count + bitmaps (everything between vector_size and CRC).
        val expectedCheckedEnd = checksummedStart.toLong() + vectorSize
        if (expectedCheckedEnd != blob.size - 4L) {
            throw IOException("Puffin blob vector_size $vectorSize inconsistent with file length ${blob.size}")
        }

        val magic = ByteArray(4)
        buf.get(magic)
        if (!magic.contentEquals(DELETION_VECTOR_MAGIC)) {
            throw IOException("Puffin blob magic mismatch — not a DuckLake deletion vector file")
        }

        buf.order(ByteOrder.LITTLE_ENDIAN)
        val bitmapCount = buf.getLong()
        if (bitmapCount < 0 || bitmapCount > Int.MAX_VALUE) {
            throw IOException("Implausible bitmap_count in puffin blob: $bitmapCount")
        }
        // bitmap_count is consistency-checked against vector_size, but vector_size can itself be
        // a small consistent-but-short value, so cross-check against the bytes actually remaining:
        // each bitmap consumes at least 4 bytes (high_bits) before its Roaring body. Without this
        // an over-large count would walk off the end below.
        val bytesForBitmaps = expectedCheckedEnd - buf.position()
        if (bitmapCount > bytesForBitmaps / 4L) {
            throw IOException("Implausible bitmap_count $bitmapCount for $bytesForBitmaps remaining puffin-blob bytes")
        }

        val positions = hashSetOf<Long>()
        var b: Long = 0
        // Per-bitmap reads (getInt, RoaringBitmap.deserialize, buffer reposition) can throw
        // unchecked BufferUnderflowException / IndexOutOfBoundsException / IllegalArgumentException
        // on a truncated or garbage body — those would bypass this method's `throws IOException`
        // contract and surface as a raw RuntimeException. Translate them to IOException so a
        // corrupt delete file fails the split the same clean way every other corruption mode does.
        try {
            while (b < bitmapCount) {
                val highBits = buf.getInt()
                val highLong = highBits.toLong() shl 32

                val rbStart = buf.position()
                // RoaringBitmap.deserialize(ByteBuffer) internally slices the buffer (see
                // RoaringArray.deserialize line ~548) so it does NOT advance the input buffer's
                // position. We advance manually by the bitmap's serialized size after the read.
                // The CRoaring portable format is LITTLE_ENDIAN and the library forces that order
                // on its internal slice regardless of what we set here.
                val bitmap = RoaringBitmap()
                try {
                    bitmap.deserialize(buf)
                }
                catch (e: IOException) {
                    throw IOException("Failed to deserialize Roaring bitmap at offset $rbStart in puffin blob", e)
                }
                buf.position(rbStart + bitmap.serializedSizeInBytes())
                for (low in bitmap) {
                    positions.add(highLong or (low.toLong() and 0xFFFFFFFFL))
                }
                b++
            }
        }
        catch (e: RuntimeException) {
            throw IOException("Corrupt puffin deletion vector: bitmap decode failed (bitmap_count=$bitmapCount, blob size=${blob.size})", e)
        }

        val checksummedEnd = buf.position()
        if (checksummedEnd.toLong() != expectedCheckedEnd) {
            throw IOException("Puffin blob bitmaps consumed ${checksummedEnd - checksummedStart} bytes; vector_size declared $vectorSize")
        }

        buf.order(ByteOrder.BIG_ENDIAN)
        val storedCrc = Integer.toUnsignedLong(buf.getInt())
        val crc = CRC32()
        crc.update(blob, checksummedStart, checksummedEnd - checksummedStart)
        val computedCrc = crc.getValue()
        if (computedCrc != storedCrc) {
            throw IOException("Puffin blob CRC mismatch: stored=" + java.lang.Long.toHexString(storedCrc)
                    + " computed=" + java.lang.Long.toHexString(computedCrc))
        }

        return positions
    }
}
