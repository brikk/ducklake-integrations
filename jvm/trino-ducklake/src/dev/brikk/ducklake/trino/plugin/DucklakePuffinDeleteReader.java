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

import io.trino.filesystem.TrinoInputFile;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.CRC32;

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
public final class DucklakePuffinDeleteReader
{
    private static final byte[] DELETION_VECTOR_MAGIC = {(byte) 0xD1, (byte) 0xD3, (byte) 0x39, (byte) 0x64};
    private static final int MIN_BLOB_SIZE = 4 + 4 + 8 + 4; // vector_size + magic + bitmap_count + CRC, no bitmaps

    private DucklakePuffinDeleteReader() {}

    /**
     * Read all deleted row positions from a DuckLake puffin file.
     */
    public static Set<Long> readDeletedPositions(TrinoInputFile inputFile)
            throws IOException
    {
        long length = inputFile.length();
        if (length > Integer.MAX_VALUE) {
            throw new IOException("Puffin delete file is unreasonably large: " + length + " bytes (path=" + inputFile.location() + ")");
        }
        byte[] blob = new byte[(int) length];
        try (InputStream in = inputFile.newStream()) {
            int read = 0;
            while (read < blob.length) {
                int n = in.read(blob, read, blob.length - read);
                if (n < 0) {
                    throw new IOException("Unexpected EOF reading puffin delete file at offset " + read
                            + " of " + blob.length + " (path=" + inputFile.location() + ")");
                }
                read += n;
            }
        }
        return decodeBlob(blob);
    }

    /**
     * Decode the raw blob bytes into row positions. Visible for testing.
     */
    static Set<Long> decodeBlob(byte[] blob)
            throws IOException
    {
        if (blob.length < MIN_BLOB_SIZE) {
            throw new IOException("Puffin delete blob too small: " + blob.length + " bytes (minimum " + MIN_BLOB_SIZE + ")");
        }

        ByteBuffer buf = ByteBuffer.wrap(blob);

        buf.order(ByteOrder.BIG_ENDIAN);
        long vectorSize = Integer.toUnsignedLong(buf.getInt());
        int checksummedStart = buf.position();
        // vectorSize counts magic + bitmap_count + bitmaps (everything between vector_size and CRC).
        long expectedCheckedEnd = (long) checksummedStart + vectorSize;
        if (expectedCheckedEnd != blob.length - 4L) {
            throw new IOException("Puffin blob vector_size " + vectorSize + " inconsistent with file length " + blob.length);
        }

        byte[] magic = new byte[4];
        buf.get(magic);
        if (!Arrays.equals(magic, DELETION_VECTOR_MAGIC)) {
            throw new IOException("Puffin blob magic mismatch — not a DuckLake deletion vector file");
        }

        buf.order(ByteOrder.LITTLE_ENDIAN);
        long bitmapCount = buf.getLong();
        if (bitmapCount < 0 || bitmapCount > Integer.MAX_VALUE) {
            throw new IOException("Implausible bitmap_count in puffin blob: " + bitmapCount);
        }

        Set<Long> positions = new HashSet<>();
        for (long b = 0; b < bitmapCount; b++) {
            int highBits = buf.getInt();
            long highLong = ((long) highBits) << 32;

            int rbStart = buf.position();
            // RoaringBitmap.deserialize(ByteBuffer) internally slices the buffer (see
            // RoaringArray.deserialize line ~548) so it does NOT advance the input buffer's
            // position. We advance manually by the bitmap's serialized size after the read.
            // The CRoaring portable format is LITTLE_ENDIAN and the library forces that order
            // on its internal slice regardless of what we set here.
            RoaringBitmap bitmap = new RoaringBitmap();
            try {
                bitmap.deserialize(buf);
            }
            catch (IOException e) {
                throw new IOException("Failed to deserialize Roaring bitmap at offset " + rbStart + " in puffin blob", e);
            }
            buf.position(rbStart + bitmap.serializedSizeInBytes());
            for (int low : bitmap) {
                positions.add(highLong | (low & 0xFFFFFFFFL));
            }
        }

        int checksummedEnd = buf.position();
        if (checksummedEnd != expectedCheckedEnd) {
            throw new IOException("Puffin blob bitmaps consumed " + (checksummedEnd - checksummedStart)
                    + " bytes; vector_size declared " + vectorSize);
        }

        buf.order(ByteOrder.BIG_ENDIAN);
        long storedCrc = Integer.toUnsignedLong(buf.getInt());
        CRC32 crc = new CRC32();
        crc.update(blob, checksummedStart, checksummedEnd - checksummedStart);
        long computedCrc = crc.getValue();
        if (computedCrc != storedCrc) {
            throw new IOException("Puffin blob CRC mismatch: stored=" + Long.toHexString(storedCrc)
                    + " computed=" + Long.toHexString(computedCrc));
        }

        return positions;
    }
}
