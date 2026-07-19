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

import org.roaringbitmap.RoaringBitmap
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32

/**
 * Encoder counterpart to [DucklakePuffinDeleteReader]: serializes a set of file-local deleted row
 * positions into a single DuckLake deletion-vector-v1 blob — the bare-blob shape DuckLake writes
 * for one deletion vector (`DuckLakePuffinWriter::Write` with a single blob, no PFA1 container).
 * Byte-exact to `DuckLakeDeletionVectorData::ToBlob`
 * (`vendor/ducklake/src/storage/ducklake_deletion_vector.cpp`); see [DucklakePuffinDeleteReader] for
 * the full layout. Both Trino and DuckDB read what this produces.
 */
object DucklakePuffinDeleteWriter {
    private val DELETION_VECTOR_MAGIC = byteArrayOf(0xD1.toByte(), 0xD3.toByte(), 0x39.toByte(), 0x64.toByte())

    /**
     * Serialize [positions] (file-local row offsets) into a bare deletion-vector blob. Positions are
     * grouped by their high 32 bits into per-chunk Roaring bitmaps, exactly as DuckLake groups them;
     * a single position reconstructs as `(high_bits << 32) | (low & 0xFFFFFFFF)` on read.
     */
    fun encodeBlob(positions: Collection<Long>): ByteArray {
        // Group low 32 bits by high 32 bits (sorted high keys keep output deterministic; the reader
        // is order-independent). runOptimize so the serialized size matches what readSafe expects.
        val byHigh = sortedMapOf<Int, RoaringBitmap>()
        for (p in positions) {
            val high = (p shr 32).toInt()
            byHigh.getOrPut(high) { RoaringBitmap() }.add((p and 0xFFFFFFFFL).toInt())
        }
        val serialized = LinkedHashMap<Int, ByteArray>()
        for ((key, rb) in byHigh) {
            rb.runOptimize()
            val b = ByteBuffer.allocate(rb.serializedSizeInBytes()).order(ByteOrder.LITTLE_ENDIAN)
            rb.serialize(b)
            serialized[key] = b.array()
        }

        var bitmapsTotal = 0
        for (s in serialized.values) {
            bitmapsTotal += 4 + s.size // 4-byte high_bits + portable Roaring body
        }
        // vector_size counts magic + bitmap_count + bitmaps (everything between vector_size and CRC).
        val checksummedLength = 4 /*magic*/ + 8 /*bitmap_count*/ + bitmapsTotal
        val total = 4 /*vector_size*/ + checksummedLength + 4 /*CRC*/

        val buf = ByteBuffer.allocate(total)
        buf.order(ByteOrder.BIG_ENDIAN).putInt(checksummedLength)
        val checksummedStart = buf.position()
        buf.put(DELETION_VECTOR_MAGIC)
        buf.order(ByteOrder.LITTLE_ENDIAN).putLong(serialized.size.toLong())
        for ((key, body) in serialized) {
            buf.putInt(key)
            buf.put(body)
        }
        val crc = CRC32()
        crc.update(buf.array(), checksummedStart, buf.position() - checksummedStart)
        buf.order(ByteOrder.BIG_ENDIAN).putInt(crc.value.toInt())
        return buf.array()
    }
}
