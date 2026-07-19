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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.trino.filesystem.TrinoInputFile
import org.roaringbitmap.RoaringBitmap
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32

/**
 * Reader for DuckLake's `.puffin` deletion-vector files. Two on-disk shapes exist (see
 * `vendor/ducklake/src/storage/ducklake_puffin.cpp` `DuckLakePuffinWriter::Write`):
 *
 *  1. **Bare blob** — a single deletion-vector-v1 blob written with NO surrounding container
 *     (what DuckLake writes when a file has one deletion vector). Its snapshot is the catalog
 *     `begin_snapshot`; the blob carries no embedded snapshot.
 *  2. **PFA1 container** — `Magic("PFA1") Blob1 … BlobN Footer`, written when deletions from
 *     multiple snapshots are CONSOLIDATED into one file ("partial" delete file, `partial_max`
 *     set). The JSON footer lists each blob's `offset`/`length` and a `ducklake-snapshot-id`
 *     property. A time-travel read at snapshot S must apply only the blobs whose snapshot id
 *     `<= S` — this is what retires the old partial-puffin read gate.
 *
 * Mirror of upstream `DuckLakePuffinReader::ParseFooter` +
 * `DuckLakeDeleteFilter::ScanDeletionVectorFile`.
 *
 * Single deletion-vector blob layout (all length fields are 4 bytes):
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
 * Row positions are reconstructed as `(high_bits << 32) | (low_bits & 0xFFFFFFFFL)`.
 */
object DucklakePuffinDeleteReader {
    private val DELETION_VECTOR_MAGIC = byteArrayOf(0xD1.toByte(), 0xD3.toByte(), 0x39.toByte(), 0x64.toByte())
    private const val MIN_BLOB_SIZE = 4 + 4 + 8 + 4 // vector_size + magic + bitmap_count + CRC, no bitmaps

    // PFA1 container constants (mirror ducklake_puffin.cpp).
    private val PUFFIN_MAGIC = byteArrayOf('P'.code.toByte(), 'F'.code.toByte(), 'A'.code.toByte(), '1'.code.toByte())
    private const val PUFFIN_MAGIC_SIZE = 4
    // FooterPayloadSize(4) + Flags(4) + trailing Magic(4).
    private const val PUFFIN_FOOTER_TAIL_SIZE = 4 + 4 + PUFFIN_MAGIC_SIZE
    private const val PUFFIN_MIN_FILE_SIZE = PUFFIN_MAGIC_SIZE + PUFFIN_MAGIC_SIZE + PUFFIN_FOOTER_TAIL_SIZE
    private const val DELETION_VECTOR_BLOB_TYPE = "deletion-vector-v1"
    private const val DUCKLAKE_SNAPSHOT_PROPERTY = "ducklake-snapshot-id"
    private val MAPPER = ObjectMapper()

    /** One blob region in a puffin file plus its embedded snapshot id (null = no embedded snapshot). */
    private data class PuffinBlob(val offset: Int, val length: Int, val snapshotId: Long?)

    /**
     * Read ALL deleted row positions from a puffin file, ignoring any embedded per-blob snapshots
     * (the union of every blob). Use this for a read at the latest snapshot, or for a non-partial
     * (single-blob) file.
     */
    @Throws(IOException::class)
    fun readDeletedPositions(inputFile: TrinoInputFile): Set<Long> =
        decodeFile(readAllBytes(inputFile), inputFile.location().toString(), null)

    /**
     * Read the deleted positions visible at snapshot [snapshotFilterMax] from a CONSOLIDATED puffin
     * file. Mirrors `DuckLakeDeleteFilter::ScanDeletionVectorFile`:
     *  - if NO blob carries an embedded snapshot id → the file isn't snapshot-partitioned; return the
     *    full union (every deletion applies — same as [readDeletedPositions]);
     *  - if EVERY blob carries one → return only the positions of blobs whose snapshot id
     *    `<= snapshotFilterMax` (deletions that had taken effect by the read snapshot);
     *  - a MIX is corrupt (DuckLake rejects it identically).
     */
    @Throws(IOException::class)
    fun readDeletedPositions(inputFile: TrinoInputFile, snapshotFilterMax: Long): Set<Long> =
        decodeFile(readAllBytes(inputFile), inputFile.location().toString(), snapshotFilterMax)

    /**
     * Decode an entire puffin file (raw bytes) into deleted positions, applying [snapshotFilterMax]
     * if non-null (see [readDeletedPositions]). Visible for testing so unit tests can exercise the
     * PFA1-container + per-blob snapshot logic without filesystem plumbing.
     */
    @Throws(IOException::class)
    fun decodeFile(data: ByteArray, path: String, snapshotFilterMax: Long?): Set<Long> {
        val blobs = parseBlobs(data, path)
        val snapshotTagged = blobs.count { it.snapshotId != null }
        val mustFilter = snapshotFilterMax != null && snapshotTagged > 0
        if (snapshotTagged != 0 && snapshotTagged != blobs.size) {
            throw IOException("Puffin delete file mixes deletion vectors with and without a snapshot id: $path")
        }
        val positions = hashSetOf<Long>()
        for (blob in blobs) {
            if (mustFilter && blob.snapshotId!! > snapshotFilterMax!!) {
                continue
            }
            positions.addAll(decodeBlobRegion(data, blob.offset, blob.length))
        }
        return positions
    }

    @Throws(IOException::class)
    private fun readAllBytes(inputFile: TrinoInputFile): ByteArray {
        val length = inputFile.length()
        if (length > Int.MAX_VALUE) {
            throw IOException("Puffin delete file is unreasonably large: $length bytes (path=${inputFile.location()})")
        }
        val data = ByteArray(length.toInt())
        inputFile.newStream().use { `in` ->
            var read = 0
            while (read < data.size) {
                val n = `in`.read(data, read, data.size - read)
                if (n < 0) {
                    throw IOException("Unexpected EOF reading puffin delete file at offset $read of ${data.size} (path=${inputFile.location()})")
                }
                read += n
            }
        }
        return data
    }

    /**
     * Parse a puffin file into its blob regions. A real PFA1 container is parsed via its JSON
     * footer (capturing each blob's `ducklake-snapshot-id`); a bare deletion-vector blob is the
     * whole file as one untagged blob.
     */
    @Throws(IOException::class)
    private fun parseBlobs(data: ByteArray, path: String): List<PuffinBlob> {
        if (data.size >= PUFFIN_MIN_FILE_SIZE && regionEquals(data, 0, PUFFIN_MAGIC)) {
            return parseContainerBlobs(data, path)
        }
        if (isBareDeletionVector(data)) {
            return listOf(PuffinBlob(0, data.size, null))
        }
        return fail("File is not a valid deletion vector — magic mismatch: $path")
    }

    /** Single throw site so the parse/decode helpers below stay within detekt's per-fn throw budget. */
    private fun fail(message: String, cause: Throwable? = null): Nothing = throw IOException(message, cause)

    /** PFA1 footer: `Magic | FooterPayload | FooterPayloadSize(4 LE) | Flags(4) | Magic`. */
    @Throws(IOException::class)
    private fun parseContainerBlobs(data: ByteArray, path: String): List<PuffinBlob> {
        val payloadStart = locateFooterPayload(data, path)
        val payloadSize = data.size - PUFFIN_FOOTER_TAIL_SIZE - payloadStart
        val blobSectionEnd = payloadStart - PUFFIN_MAGIC_SIZE
        val root: JsonNode = MAPPER.readTree(data.copyOfRange(payloadStart, payloadStart + payloadSize))
        val blobsNode = root.get("blobs")
        if (blobsNode == null || !blobsNode.isArray) {
            fail("Puffin file corrupt — footer has no valid \"blobs\" list: $path")
        }
        val result = mutableListOf<PuffinBlob>()
        for (blobNode in blobsNode) {
            parseBlobEntry(blobNode, blobSectionEnd, path)?.let { result.add(it) }
        }
        return result
    }

    /** Validate the PFA1 footer and return the offset where the JSON FooterPayload begins. */
    private fun locateFooterPayload(data: ByteArray, path: String): Int {
        val size = data.size
        if (!regionEquals(data, size - PUFFIN_MAGIC_SIZE, PUFFIN_MAGIC)) {
            fail("Puffin file corrupt — trailing magic mismatch: $path")
        }
        val flags = leInt(data, size - PUFFIN_MAGIC_SIZE - 4)
        val payloadSize = leInt(data, size - PUFFIN_FOOTER_TAIL_SIZE)
        if (flags != 0 || payloadSize < 0 || payloadSize > size - PUFFIN_MIN_FILE_SIZE) {
            fail("Puffin file corrupt — unsupported flags ($flags) or footer payload size out of range: $path")
        }
        val payloadStart = size - PUFFIN_FOOTER_TAIL_SIZE - payloadSize
        if (!regionEquals(data, payloadStart - PUFFIN_MAGIC_SIZE, PUFFIN_MAGIC)) {
            fail("Puffin file corrupt — footer magic mismatch: $path")
        }
        return payloadStart
    }

    /** Parse one footer blob entry; null for blob types we don't handle (DuckLake skips them too). */
    private fun parseBlobEntry(blobNode: JsonNode, blobSectionEnd: Int, path: String): PuffinBlob? {
        if (blobNode.get("type")?.asText() != DELETION_VECTOR_BLOB_TYPE) {
            return null
        }
        val offset = blobNode.get("offset")?.asInt(-1) ?: -1
        val blobLen = blobNode.get("length")?.asInt(-1) ?: -1
        if (offset < PUFFIN_MAGIC_SIZE || blobLen < 0 || offset > blobSectionEnd - blobLen) {
            fail("Puffin file corrupt — blob offset/length out of range: $path")
        }
        val snapshotId: Long? = blobNode.get("properties")?.get(DUCKLAKE_SNAPSHOT_PROPERTY)?.asText()?.toLongOrNull()
        return PuffinBlob(offset, blobLen, snapshotId)
    }

    private fun isBareDeletionVector(data: ByteArray): Boolean =
        data.size >= 4 + DELETION_VECTOR_MAGIC.size && regionEquals(data, 4, DELETION_VECTOR_MAGIC)

    private fun regionEquals(data: ByteArray, offset: Int, expected: ByteArray): Boolean {
        if (offset < 0 || offset + expected.size > data.size) {
            return false
        }
        for (i in expected.indices) {
            if (data[offset + i] != expected[i]) {
                return false
            }
        }
        return true
    }

    /** Little-endian 4-byte int at [offset]. */
    private fun leInt(data: ByteArray, offset: Int): Int =
        (data[offset].toInt() and 0xFF) or
            ((data[offset + 1].toInt() and 0xFF) shl 8) or
            ((data[offset + 2].toInt() and 0xFF) shl 16) or
            ((data[offset + 3].toInt() and 0xFF) shl 24)

    /**
     * Decode a single bare deletion-vector blob (offset 0, whole array). Visible for testing.
     */
    @Throws(IOException::class)
    fun decodeBlob(blob: ByteArray): Set<Long> = decodeBlobRegion(blob, 0, blob.size)

    /**
     * Decode one deletion-vector-v1 blob occupying [length] bytes starting at [start] within [data].
     */
    @Throws(IOException::class)
    private fun decodeBlobRegion(data: ByteArray, start: Int, length: Int): Set<Long> {
        if (length < MIN_BLOB_SIZE) {
            fail("Puffin delete blob too small: $length bytes (minimum $MIN_BLOB_SIZE)")
        }
        val buf = ByteBuffer.wrap(data, start, length).slice()

        buf.order(ByteOrder.BIG_ENDIAN)
        val vectorSize = Integer.toUnsignedLong(buf.getInt())
        val checksummedStart = buf.position()
        // vectorSize counts magic + bitmap_count + bitmaps (everything between vector_size and CRC).
        val expectedCheckedEnd = checksummedStart.toLong() + vectorSize
        val magic = ByteArray(4)
        if (expectedCheckedEnd != length - 4L) {
            fail("Puffin blob vector_size $vectorSize inconsistent with blob length $length")
        }
        buf.get(magic)
        if (!magic.contentEquals(DELETION_VECTOR_MAGIC)) {
            fail("Puffin blob magic mismatch — not a DuckLake deletion vector file")
        }

        buf.order(ByteOrder.LITTLE_ENDIAN)
        val bitmapCount = buf.getLong()
        // bitmap_count is consistency-checked against vector_size, but vector_size can itself be a
        // small consistent-but-short value, so cross-check against the bytes actually remaining:
        // each bitmap consumes at least 4 bytes (high_bits) before its Roaring body. Without this an
        // over-large count would walk off the end while decoding.
        val bytesForBitmaps = expectedCheckedEnd - buf.position()
        if (bitmapCount < 0 || bitmapCount > Int.MAX_VALUE || bitmapCount > bytesForBitmaps / 4L) {
            fail("Implausible bitmap_count $bitmapCount for $bytesForBitmaps remaining puffin-blob bytes")
        }

        val positions = decodeBitmaps(buf, bitmapCount, length)

        val checksummedEnd = buf.position()
        if (checksummedEnd.toLong() != expectedCheckedEnd) {
            fail("Puffin blob bitmaps consumed ${checksummedEnd - checksummedStart} bytes; vector_size declared $vectorSize")
        }
        buf.order(ByteOrder.BIG_ENDIAN)
        val storedCrc = Integer.toUnsignedLong(buf.getInt())
        val crc = CRC32()
        crc.update(data, start + checksummedStart, checksummedEnd - checksummedStart)
        if (crc.value != storedCrc) {
            fail("Puffin blob CRC mismatch: stored=${java.lang.Long.toHexString(storedCrc)} computed=${java.lang.Long.toHexString(crc.value)}")
        }
        return positions
    }

    /**
     * Decode [bitmapCount] `(high_bits, Roaring)` entries from [buf], reconstructing each position as
     * `(high_bits << 32) | (low & 0xFFFFFFFF)`. Per-bitmap reads can throw unchecked
     * BufferUnderflow / IndexOutOfBounds / IllegalArgument on a truncated or garbage body; those are
     * translated to IOException so a corrupt file fails the split the same clean way every other
     * corruption mode does.
     */
    private fun decodeBitmaps(buf: ByteBuffer, bitmapCount: Long, length: Int): Set<Long> {
        val positions = hashSetOf<Long>()
        try {
            var b = 0L
            while (b < bitmapCount) {
                val highLong = buf.getInt().toLong() shl 32
                val rbStart = buf.position()
                // RoaringBitmap.deserialize(ByteBuffer) internally slices the buffer so it does NOT
                // advance the input position; we advance manually by the serialized size. CRoaring's
                // portable format is LITTLE_ENDIAN, which the library forces on its internal slice.
                val bitmap = RoaringBitmap()
                bitmap.deserialize(buf)
                buf.position(rbStart + bitmap.serializedSizeInBytes())
                for (low in bitmap) {
                    positions.add(highLong or (low.toLong() and 0xFFFFFFFFL))
                }
                b++
            }
        }
        catch (e: IOException) {
            fail("Corrupt puffin deletion vector: bitmap decode failed (bitmap_count=$bitmapCount, blob length=$length)", e)
        }
        catch (e: RuntimeException) {
            fail("Corrupt puffin deletion vector: bitmap decode failed (bitmap_count=$bitmapCount, blob length=$length)", e)
        }
        return positions
    }
}
