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

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.roaringbitmap.RoaringBitmap
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.TreeMap
import java.util.TreeSet
import java.util.zip.CRC32

/**
 * Unit tests for `DucklakePuffinDeleteReader.decodeBlob`. The cross-engine
 * round trip with a real DuckDB-written puffin file is covered by
 * `TestDucklakeCrossEnginePuffinDeleteRoundTrip`.
 *
 *
 * Each test builds a blob using the same byte layout as DuckLake's
 * `DuckLakeDeletionVectorData::ToBlob` (see
 * `vendor/ducklake/src/storage/ducklake_deletion_vector.cpp`) so the decoder
 * exercises the exact wire format rather than a Java-friendly reformat.
 */
class TestDucklakePuffinDeleteReader {
    @Test
    fun testEmptyBitmapsDecodesToEmptySet() {
        val blob = buildBlob(HashMap())
        assertThat(DucklakePuffinDeleteReader.decodeBlob(blob)).isEmpty()
    }

    @Test
    fun testSingleLowGroupRoundTrips() {
        // All positions in low 32 bits (high=0). Bit 0 + 5 + 1000 + 65535.
        val bitmaps = HashMap<Int, LongArray>()
        bitmaps[0] = longArrayOf(0L, 5L, 1000L, 65535L)

        val blob = buildBlob(bitmaps)
        val result = DucklakePuffinDeleteReader.decodeBlob(blob)
        assertThat(result).containsExactlyInAnyOrder(0L, 5L, 1000L, 65535L)
    }

    @Test
    fun testMultipleHighGroupsRoundTrip() {
        // Two bitmaps, distinct high keys. Tests the high-bits reconstruction.
        val bitmaps = HashMap<Int, LongArray>()
        bitmaps[0] = longArrayOf(1L, 2L)
        bitmaps[1] = longArrayOf((1L shl 32) or 7L, (1L shl 32) or 99L)

        val blob = buildBlob(bitmaps)
        assertThat(DucklakePuffinDeleteReader.decodeBlob(blob))
            .containsExactlyInAnyOrder(1L, 2L, (1L shl 32) or 7L, (1L shl 32) or 99L)
    }

    @Test
    fun testSparseLargeBitmapDecodes() {
        // Mix of contiguous run + scattered bits to exercise Roaring's container types.
        val vals = LongArray(10_002)
        for (i in 0 until 10_000) {
            vals[i] = i.toLong()
        }
        vals[10_000] = 4_000_000L
        vals[10_001] = 4_294_967_290L
        val bitmaps = HashMap<Int, LongArray>()
        bitmaps[0] = vals

        val blob = buildBlob(bitmaps)
        val result = DucklakePuffinDeleteReader.decodeBlob(blob)
        assertThat(result).hasSize(10_002)
        assertThat(result).contains(0L, 9_999L, 4_000_000L, 4_294_967_290L)
    }

    @Test
    fun testNegativeHighBitsAreLogicallyAccurate() {
        // DuckLake stores high_bits as int32, so a row position > Long.MAX_VALUE/2 (high
        // bit 31 set) would deserialize with a negative high_bits value. Reconstruction
        // uses ((long) highBits << 32) which sign-extends; we want unsigned semantics.
        //
        // We mirror the C++ writer exactly: it does
        //   high_bits = (int32_t) (row_idx >> 32)
        // so e.g. row 0xFFFF_FFFF_0000_0001L has high_bits = -1 (0xFFFFFFFF) and low
        // bits 0x00000001. The reader's ((long) -1 << 32) | (1 & 0xFFFFFFFFL) yields
        // 0xFFFF_FFFF_0000_0001L exactly. Confirm that's what we get.
        val positiveHigh = (123L shl 32) or 0xABCDL
        val negativeHigh = -0xFFFFFFFFL // 0xFFFF_FFFF_0000_0001L expressed as Kotlin Long literal
        val bitmaps = HashMap<Int, LongArray>()
        bitmaps[123] = longArrayOf(positiveHigh)
        bitmaps[-1] = longArrayOf(negativeHigh)

        val blob = buildBlob(bitmaps)
        assertThat(DucklakePuffinDeleteReader.decodeBlob(blob))
            .containsExactlyInAnyOrder(positiveHigh, negativeHigh)
    }

    @Test
    fun testCrcMismatchRejected() {
        val bitmaps = HashMap<Int, LongArray>()
        bitmaps[0] = longArrayOf(1L, 2L)
        val blob = buildBlob(bitmaps)
        // Corrupt one byte inside the bitmap region.
        blob[blob.size - 5] = (blob[blob.size - 5].toInt() xor 0x01).toByte()
        assertThatThrownBy { DucklakePuffinDeleteReader.decodeBlob(blob) }
            .isInstanceOf(IOException::class.java)
            .hasMessageContaining("CRC mismatch")
    }

    @Test
    fun testMagicMismatchRejected() {
        val bitmaps = HashMap<Int, LongArray>()
        bitmaps[0] = longArrayOf(1L)
        val blob = buildBlob(bitmaps)
        // Trash the magic bytes (offset 4..7).
        blob[4] = 0
        assertThatThrownBy { DucklakePuffinDeleteReader.decodeBlob(blob) }
            .isInstanceOf(IOException::class.java)
            .hasMessageContaining("magic mismatch")
    }

    @Test
    fun testTooSmallBlobRejected() {
        assertThatThrownBy { DucklakePuffinDeleteReader.decodeBlob(ByteArray(8)) }
            .isInstanceOf(IOException::class.java)
            .hasMessageContaining("too small")
    }

    @Test
    fun testInconsistentVectorSizeRejected() {
        val bitmaps = HashMap<Int, LongArray>()
        bitmaps[0] = longArrayOf(1L)
        val blob = buildBlob(bitmaps)
        // Mangle the leading vector_size to a much larger value.
        blob[0] = 0x7F
        blob[1] = 0xFF.toByte()
        assertThatThrownBy { DucklakePuffinDeleteReader.decodeBlob(blob) }
            .isInstanceOf(IOException::class.java)
            .hasMessageContaining("vector_size")
    }

    // ---- PFA1 container (consolidated / "partial" puffin delete file) ----

    @Test
    fun containerWithSnapshotTaggedBlobsFiltersByReadSnapshot() {
        // Two cumulative deletion-vector blobs: snapshot 10 deletes {1}, snapshot 20 deletes {1,2}.
        val container = buildContainer(listOf(
            10L to mapOf(0 to longArrayOf(1L)),
            20L to mapOf(0 to longArrayOf(1L, 2L))))

        // Read AS OF 10 → only the first blob's deletions.
        assertThat(DucklakePuffinDeleteReader.decodeFile(container, "test.puffin", 10L)).containsExactlyInAnyOrder(1L)
        // Read AS OF 15 → still only snapshot-10 deletions (snapshot 20 > 15 is excluded).
        assertThat(DucklakePuffinDeleteReader.decodeFile(container, "test.puffin", 15L)).containsExactlyInAnyOrder(1L)
        // Read AS OF 20 → both.
        assertThat(DucklakePuffinDeleteReader.decodeFile(container, "test.puffin", 20L)).containsExactlyInAnyOrder(1L, 2L)
        // Unfiltered (latest) → the full union.
        assertThat(DucklakePuffinDeleteReader.decodeFile(container, "test.puffin", null)).containsExactlyInAnyOrder(1L, 2L)
    }

    @Test
    fun containerReadBelowEarliestSnapshotDeletesNothing() {
        val container = buildContainer(listOf(
            10L to mapOf(0 to longArrayOf(1L)),
            20L to mapOf(0 to longArrayOf(1L, 2L))))
        // Read AS OF 9 — before any consolidated deletion → empty.
        assertThat(DucklakePuffinDeleteReader.decodeFile(container, "test.puffin", 9L)).isEmpty()
    }

    @Test
    fun containerWithoutSnapshotsReturnsFullUnionEvenWhenFiltered() {
        // A container whose blobs carry NO ducklake-snapshot-id (has_embedded_snapshots = false):
        // every deletion already applied, so a filter is a no-op.
        val container = buildContainer(listOf(
            null to mapOf(0 to longArrayOf(1L)),
            null to mapOf(0 to longArrayOf(2L, 3L))))
        assertThat(DucklakePuffinDeleteReader.decodeFile(container, "test.puffin", 5L)).containsExactlyInAnyOrder(1L, 2L, 3L)
        assertThat(DucklakePuffinDeleteReader.decodeFile(container, "test.puffin", null)).containsExactlyInAnyOrder(1L, 2L, 3L)
    }

    @Test
    fun containerMixingSnapshotTaggedAndUntaggedBlobsIsRejected() {
        val container = buildContainer(listOf(
            10L to mapOf(0 to longArrayOf(1L)),
            null to mapOf(0 to longArrayOf(2L))))
        assertThatThrownBy { DucklakePuffinDeleteReader.decodeFile(container, "test.puffin", 10L) }
            .isInstanceOf(IOException::class.java)
            .hasMessageContaining("mixes deletion vectors")
    }

    @Test
    fun bareBlobIsTreatedAsAnUntaggedSingleBlobFile() {
        // A bare (non-container) blob decoded through decodeFile = the whole file is one untagged
        // blob; a snapshot filter is a no-op (its snapshot is the catalog begin_snapshot).
        val bare = buildBlob(mapOf(0 to longArrayOf(7L, 8L)))
        assertThat(DucklakePuffinDeleteReader.decodeFile(bare, "bare.puffin", 3L)).containsExactlyInAnyOrder(7L, 8L)
        assertThat(DucklakePuffinDeleteReader.decodeFile(bare, "bare.puffin", null)).containsExactlyInAnyOrder(7L, 8L)
    }

    // ---- Writer round trip (DucklakePuffinDeleteWriter encode → reader decode) ----

    @Test
    fun writerEncodeRoundTripsThroughReader() {
        for (positions in listOf(
                setOf<Long>(),
                setOf(0L),
                setOf(0L, 5L, 1000L, 65535L),
                setOf(1L, 2L, (1L shl 32) or 7L, (1L shl 32) or 99L),
                (0L until 5000L).toSet() + setOf(4_000_000L, 4_294_967_290L))) {
            val blob = DucklakePuffinDeleteWriter.encodeBlob(positions)
            assertThat(DucklakePuffinDeleteReader.decodeBlob(blob))
                .`as`("encode→decode round trip for %d positions", positions.size)
                .containsExactlyInAnyOrderElementsOf(positions)
        }
    }

    @Test
    fun testHelperBuildsBlobsTheReaderAccepts() {
        // Sanity: every other test depends on buildBlob producing a valid blob; this is the
        // explicit sanity check. Uses TreeSet only to keep the assertion deterministic.
        val expected = TreeSet<Long>()
        for (v in 0L until 50L) {
            expected.add(v)
        }
        val bitmaps = HashMap<Int, LongArray>()
        bitmaps[0] = expected.stream().mapToLong(Long::toLong).toArray()

        val blob = buildBlob(bitmaps)
        assertThat(DucklakePuffinDeleteReader.decodeBlob(blob)).containsExactlyInAnyOrderElementsOf(expected)
    }

    companion object {
        private val DELETION_VECTOR_MAGIC = byteArrayOf(0xD1.toByte(), 0xD3.toByte(), 0x39.toByte(), 0x64.toByte())
        private val PUFFIN_MAGIC = byteArrayOf('P'.code.toByte(), 'F'.code.toByte(), 'A'.code.toByte(), '1'.code.toByte())

        /**
         * Build a PFA1 puffin CONTAINER from a list of (snapshotId?, bitmaps) blobs, mirroring
         * `DuckLakePuffinWriter::Write` (multi-blob branch): `Magic Blob1..BlobN Footer`, footer =
         * `Magic | JSON | payloadSize(4 LE) | flags(4=0) | Magic`. A null snapshotId omits the
         * `ducklake-snapshot-id` property (an untagged blob).
         */
        @Throws(IOException::class)
        private fun buildContainer(blobs: List<Pair<Long?, Map<Int, LongArray>>>): ByteArray {
            val out = java.io.ByteArrayOutputStream()
            out.write(PUFFIN_MAGIC)
            data class Info(val snapshotId: Long?, val offset: Int, val length: Int)
            val infos = mutableListOf<Info>()
            var offset = PUFFIN_MAGIC.size
            for ((snap, bitmaps) in blobs) {
                val blobBytes = buildBlob(bitmaps)
                infos.add(Info(snap, offset, blobBytes.size))
                out.write(blobBytes)
                offset += blobBytes.size
            }
            val json = buildString {
                append("{\"blobs\":[")
                infos.forEachIndexed { i, info ->
                    if (i > 0) append(",")
                    append("{\"type\":\"deletion-vector-v1\",\"offset\":").append(info.offset)
                    append(",\"length\":").append(info.length)
                    if (info.snapshotId != null) {
                        append(",\"properties\":{\"ducklake-snapshot-id\":\"").append(info.snapshotId).append("\"}")
                    }
                    append("}")
                }
                append("],\"properties\":{\"created-by\":\"ducklake\"}}")
            }
            val payload = json.toByteArray(Charsets.UTF_8)
            out.write(PUFFIN_MAGIC)
            out.write(payload)
            val tail = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(payload.size).array()
            out.write(tail)
            out.write(ByteArray(4)) // flags = 0
            out.write(PUFFIN_MAGIC)
            return out.toByteArray()
        }

        /**
         * Build a DuckLake puffin delete blob from the given bitmaps. Mirrors
         * `DuckLakeDeletionVectorData::ToBlob` byte for byte.
         */
        @Throws(IOException::class)
        private fun buildBlob(rawBitmaps: Map<Int, LongArray>): ByteArray {
            val roaring = HashMap<Int, RoaringBitmap>()
            for ((key, value) in rawBitmaps) {
                val rb = RoaringBitmap()
                for (v in value) {
                    rb.add((v and 0xFFFFFFFFL).toInt())
                }
                rb.runOptimize()
                roaring[key] = rb
            }

            // Pre-serialize each bitmap to know its size. CRoaring's "portable" format is
            // LITTLE-endian; Java's DataOutputStream writes BIG-endian, so go through a
            // ByteBuffer with LE order to match the C++ writer's byte layout.
            val serialized = TreeMap<Int, ByteArray>()
            for ((key, value) in roaring) {
                val size = value.serializedSizeInBytes()
                val rbBuf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN)
                value.serialize(rbBuf)
                serialized[key] = rbBuf.array()
            }

            var bitmapsTotal = 0
            for (s in serialized.values) {
                bitmapsTotal += 4 + s.size // 4B high_bits + bitmap
            }
            val checksummedLength = 4 /*magic*/ + 8 /*count*/ + bitmapsTotal
            val totalSize = 4 /*vector_size*/ + checksummedLength + 4 /*CRC*/

            val buf = ByteBuffer.allocate(totalSize)

            buf.order(ByteOrder.BIG_ENDIAN)
            buf.putInt(checksummedLength)

            val checksummedStart = buf.position()
            buf.put(DELETION_VECTOR_MAGIC)

            buf.order(ByteOrder.LITTLE_ENDIAN)
            buf.putLong(serialized.size.toLong())

            // Deterministic iteration order (the writer's C++ uses unordered_map so order is
            // implementation-defined; we sort by key to keep the test stable). The reader doesn't
            // depend on order so any order works at runtime.
            for ((key, value) in serialized) {
                buf.putInt(key)
                buf.put(value)
            }

            // CRC over the checksummed region.
            val crc = CRC32()
            crc.update(buf.array(), checksummedStart, buf.position() - checksummedStart)
            val crcValue = crc.value

            buf.order(ByteOrder.BIG_ENDIAN)
            buf.putInt(crcValue.toInt())

            if (buf.position() != totalSize) {
                throw AssertionError("Built blob has wrong length: " + buf.position() + " vs " + totalSize)
            }
            return buf.array()
        }
    }
}
