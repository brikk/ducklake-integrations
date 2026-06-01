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

import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@code DucklakePuffinDeleteReader.decodeBlob}. The cross-engine
 * round trip with a real DuckDB-written puffin file is covered by
 * {@code TestDucklakeCrossEnginePuffinDeleteRoundTrip}.
 *
 * <p>Each test builds a blob using the same byte layout as DuckLake's
 * {@code DuckLakeDeletionVectorData::ToBlob} (see
 * {@code vendor/ducklake/src/storage/ducklake_deletion_vector.cpp}) so the decoder
 * exercises the exact wire format rather than a Java-friendly reformat.
 */
public class TestDucklakePuffinDeleteReader
{
    private static final byte[] DELETION_VECTOR_MAGIC = {(byte) 0xD1, (byte) 0xD3, (byte) 0x39, (byte) 0x64};

    @Test
    public void testEmptyBitmapsDecodesToEmptySet()
            throws Exception
    {
        byte[] blob = buildBlob(new HashMap<>());
        assertThat(DucklakePuffinDeleteReader.decodeBlob(blob)).isEmpty();
    }

    @Test
    public void testSingleLowGroupRoundTrips()
            throws Exception
    {
        // All positions in low 32 bits (high=0). Bit 0 + 5 + 1000 + 65535.
        Map<Integer, long[]> bitmaps = new HashMap<>();
        bitmaps.put(0, new long[] {0L, 5L, 1000L, 65535L});

        byte[] blob = buildBlob(bitmaps);
        Set<Long> result = DucklakePuffinDeleteReader.decodeBlob(blob);
        assertThat(result).containsExactlyInAnyOrder(0L, 5L, 1000L, 65535L);
    }

    @Test
    public void testMultipleHighGroupsRoundTrip()
            throws Exception
    {
        // Two bitmaps, distinct high keys. Tests the high-bits reconstruction.
        Map<Integer, long[]> bitmaps = new HashMap<>();
        bitmaps.put(0, new long[] {1L, 2L});
        bitmaps.put(1, new long[] {(1L << 32) | 7L, (1L << 32) | 99L});

        byte[] blob = buildBlob(bitmaps);
        assertThat(DucklakePuffinDeleteReader.decodeBlob(blob))
                .containsExactlyInAnyOrder(1L, 2L, (1L << 32) | 7L, (1L << 32) | 99L);
    }

    @Test
    public void testSparseLargeBitmapDecodes()
            throws Exception
    {
        // Mix of contiguous run + scattered bits to exercise Roaring's container types.
        long[] vals = new long[10_002];
        for (int i = 0; i < 10_000; i++) {
            vals[i] = i;
        }
        vals[10_000] = 4_000_000L;
        vals[10_001] = 4_294_967_290L;
        Map<Integer, long[]> bitmaps = new HashMap<>();
        bitmaps.put(0, vals);

        byte[] blob = buildBlob(bitmaps);
        Set<Long> result = DucklakePuffinDeleteReader.decodeBlob(blob);
        assertThat(result).hasSize(10_002);
        assertThat(result).contains(0L, 9_999L, 4_000_000L, 4_294_967_290L);
    }

    @Test
    public void testNegativeHighBitsAreLogicallyAccurate()
            throws Exception
    {
        // DuckLake stores high_bits as int32, so a row position > Long.MAX_VALUE/2 (high
        // bit 31 set) would deserialize with a negative high_bits value. Reconstruction
        // uses ((long) highBits << 32) which sign-extends; we want unsigned semantics.
        //
        // We mirror the C++ writer exactly: it does
        //   high_bits = (int32_t) (row_idx >> 32)
        // so e.g. row 0xFFFF_FFFF_0000_0001L has high_bits = -1 (0xFFFFFFFF) and low
        // bits 0x00000001. The reader's ((long) -1 << 32) | (1 & 0xFFFFFFFFL) yields
        // 0xFFFF_FFFF_0000_0001L exactly. Confirm that's what we get.
        long positiveHigh = (123L << 32) | 0xABCDL;
        long negativeHigh = 0xFFFF_FFFF_0000_0001L;
        Map<Integer, long[]> bitmaps = new HashMap<>();
        bitmaps.put(123, new long[] {positiveHigh});
        bitmaps.put(-1, new long[] {negativeHigh});

        byte[] blob = buildBlob(bitmaps);
        assertThat(DucklakePuffinDeleteReader.decodeBlob(blob))
                .containsExactlyInAnyOrder(positiveHigh, negativeHigh);
    }

    @Test
    public void testCrcMismatchRejected()
            throws Exception
    {
        Map<Integer, long[]> bitmaps = new HashMap<>();
        bitmaps.put(0, new long[] {1L, 2L});
        byte[] blob = buildBlob(bitmaps);
        // Corrupt one byte inside the bitmap region.
        blob[blob.length - 5] ^= 0x01;
        assertThatThrownBy(() -> DucklakePuffinDeleteReader.decodeBlob(blob))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CRC mismatch");
    }

    @Test
    public void testMagicMismatchRejected()
            throws Exception
    {
        Map<Integer, long[]> bitmaps = new HashMap<>();
        bitmaps.put(0, new long[] {1L});
        byte[] blob = buildBlob(bitmaps);
        // Trash the magic bytes (offset 4..7).
        blob[4] = 0;
        assertThatThrownBy(() -> DucklakePuffinDeleteReader.decodeBlob(blob))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("magic mismatch");
    }

    @Test
    public void testTooSmallBlobRejected()
    {
        assertThatThrownBy(() -> DucklakePuffinDeleteReader.decodeBlob(new byte[8]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("too small");
    }

    @Test
    public void testInconsistentVectorSizeRejected()
            throws Exception
    {
        Map<Integer, long[]> bitmaps = new HashMap<>();
        bitmaps.put(0, new long[] {1L});
        byte[] blob = buildBlob(bitmaps);
        // Mangle the leading vector_size to a much larger value.
        blob[0] = 0x7F;
        blob[1] = (byte) 0xFF;
        assertThatThrownBy(() -> DucklakePuffinDeleteReader.decodeBlob(blob))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("vector_size");
    }

    /**
     * Build a DuckLake puffin delete blob from the given bitmaps. Mirrors
     * {@code DuckLakeDeletionVectorData::ToBlob} byte for byte.
     */
    private static byte[] buildBlob(Map<Integer, long[]> rawBitmaps)
            throws IOException
    {
        Map<Integer, RoaringBitmap> roaring = new HashMap<>();
        for (Map.Entry<Integer, long[]> entry : rawBitmaps.entrySet()) {
            RoaringBitmap rb = new RoaringBitmap();
            for (long v : entry.getValue()) {
                rb.add((int) (v & 0xFFFFFFFFL));
            }
            rb.runOptimize();
            roaring.put(entry.getKey(), rb);
        }

        // Pre-serialize each bitmap to know its size. CRoaring's "portable" format is
        // LITTLE-endian; Java's DataOutputStream writes BIG-endian, so go through a
        // ByteBuffer with LE order to match the C++ writer's byte layout.
        Map<Integer, byte[]> serialized = new java.util.TreeMap<>();
        for (Map.Entry<Integer, RoaringBitmap> entry : roaring.entrySet()) {
            int size = entry.getValue().serializedSizeInBytes();
            ByteBuffer rbBuf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
            entry.getValue().serialize(rbBuf);
            serialized.put(entry.getKey(), rbBuf.array());
        }

        int bitmapsTotal = 0;
        for (byte[] s : serialized.values()) {
            bitmapsTotal += 4 + s.length; // 4B high_bits + bitmap
        }
        int checksummedLength = 4 /*magic*/ + 8 /*count*/ + bitmapsTotal;
        int totalSize = 4 /*vector_size*/ + checksummedLength + 4 /*CRC*/;

        ByteBuffer buf = ByteBuffer.allocate(totalSize);

        buf.order(ByteOrder.BIG_ENDIAN);
        buf.putInt(checksummedLength);

        int checksummedStart = buf.position();
        buf.put(DELETION_VECTOR_MAGIC);

        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(serialized.size());

        // Deterministic iteration order (the writer's C++ uses unordered_map so order is
        // implementation-defined; we sort by key to keep the test stable). The reader doesn't
        // depend on order so any order works at runtime.
        for (Map.Entry<Integer, byte[]> entry : serialized.entrySet()) {
            buf.putInt(entry.getKey());
            buf.put(entry.getValue());
        }

        // CRC over the checksummed region.
        CRC32 crc = new CRC32();
        crc.update(buf.array(), checksummedStart, buf.position() - checksummedStart);
        long crcValue = crc.getValue();

        buf.order(ByteOrder.BIG_ENDIAN);
        buf.putInt((int) crcValue);

        if (buf.position() != totalSize) {
            throw new AssertionError("Built blob has wrong length: " + buf.position() + " vs " + totalSize);
        }
        return buf.array();
    }

    @Test
    public void testHelperBuildsBlobsTheReaderAccepts()
            throws Exception
    {
        // Sanity: every other test depends on buildBlob producing a valid blob; this is the
        // explicit sanity check. Uses TreeSet only to keep the assertion deterministic.
        TreeSet<Long> expected = new TreeSet<>();
        for (long v = 0; v < 50; v++) {
            expected.add(v);
        }
        Map<Integer, long[]> bitmaps = new HashMap<>();
        bitmaps.put(0, expected.stream().mapToLong(Long::longValue).toArray());

        byte[] blob = buildBlob(bitmaps);
        assertThat(DucklakePuffinDeleteReader.decodeBlob(blob)).containsExactlyInAnyOrderElementsOf(expected);
    }
}
