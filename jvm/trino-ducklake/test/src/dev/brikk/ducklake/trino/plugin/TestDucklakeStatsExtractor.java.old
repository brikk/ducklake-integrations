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

import dev.brikk.ducklake.catalog.DucklakeFileColumnStats;
import io.trino.spi.type.DecimalType;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestDucklakeStatsExtractor
{
    @Test
    void testConvertIntegerStats()
    {
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(42).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, INTEGER)).isEqualTo(Optional.of("42"));
    }

    @Test
    void testConvertNegativeInteger()
    {
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(-17).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, INTEGER)).isEqualTo(Optional.of("-17"));
    }

    @Test
    void testConvertBigintStats()
    {
        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(9_000_000_000L).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, BIGINT)).isEqualTo(Optional.of("9000000000"));
    }

    @Test
    void testConvertDoubleStats()
    {
        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(3.14).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, DOUBLE)).isEqualTo(Optional.of("3.14"));
    }

    @Test
    void testConvertDoubleNan()
    {
        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(Double.NaN).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, DOUBLE)).isEmpty();
    }

    @Test
    void testConvertRealStats()
    {
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(2.5f).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, REAL)).isEqualTo(Optional.of("2.5"));
    }

    @Test
    void testConvertVarcharStats()
    {
        byte[] bytes = "hello world".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, VARCHAR)).isEqualTo(Optional.of("hello world"));
    }

    @Test
    void testConvertBooleanTrue()
    {
        byte[] bytes = new byte[] {1};
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, BOOLEAN)).isEqualTo(Optional.of("true"));
    }

    @Test
    void testConvertBooleanFalse()
    {
        byte[] bytes = new byte[] {0};
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, BOOLEAN)).isEqualTo(Optional.of("false"));
    }

    @Test
    void testConvertDateStats()
    {
        // 2024-01-15 = epoch day 19737
        int epochDay = (int) java.time.LocalDate.of(2024, 1, 15).toEpochDay();
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(epochDay).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, DATE)).isEqualTo(Optional.of("2024-01-15"));
    }

    @Test
    void testConvertShortDecimalInt32IsLittleEndian()
    {
        // decimal(5,2) value 123.45 -> unscaled 12345, stored by parquet as a
        // little-endian INT32. Decoding it big-endian (the old bug) yields garbage.
        DecimalType type = DecimalType.createDecimalType(5, 2);
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(12345).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, type, org.apache.parquet.format.Type.INT32))
                .isEqualTo(Optional.of("123.45"));
    }

    @Test
    void testShortDecimalInt32NegativeIsLittleEndian()
    {
        DecimalType type = DecimalType.createDecimalType(5, 2);
        byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(-12345).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, type, org.apache.parquet.format.Type.INT32))
                .isEqualTo(Optional.of("-123.45"));
    }

    @Test
    void testShortDecimalInt64IsLittleEndian()
    {
        // decimal(18,4) value 12345.6789 -> unscaled 123456789, stored as little-endian INT64.
        DecimalType type = DecimalType.createDecimalType(18, 4);
        byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(123_456_789L).array();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, type, org.apache.parquet.format.Type.INT64))
                .isEqualTo(Optional.of("12345.6789"));
    }

    @Test
    void testLongDecimalFixedLenByteArrayIsBigEndian()
    {
        // High-precision decimals are FIXED_LEN_BYTE_ARRAY / BINARY: big-endian two's complement.
        DecimalType type = DecimalType.createDecimalType(38, 0);
        byte[] bytes = BigInteger.valueOf(1_000_000L).toByteArray();
        assertThat(DucklakeStatsExtractor.convertStatValue(bytes, type, org.apache.parquet.format.Type.FIXED_LEN_BYTE_ARRAY))
                .isEqualTo(Optional.of("1000000"));
    }

    @Test
    void testConvertEmptyValue()
    {
        assertThat(DucklakeStatsExtractor.convertStatValue(new byte[0], INTEGER)).isEmpty();
    }

    @Test
    void testConvertNullValue()
    {
        assertThat(DucklakeStatsExtractor.convertStatValue(null, INTEGER)).isEmpty();
    }

    // ==================== extractStats — nested-leaf coverage ====================
    //
    // Synthesizes a thrift FileMetaData with N row-group columns at known indices,
    // then asserts extractStats emits one DucklakeFileColumnStats per LeafStatsTarget
    // keyed by the target's fieldId. The leaf walk and the parquet writer's leaf
    // emission order are tested separately (see TestDucklakeStatsLeafProjector); this
    // suite pins the alignment contract — `parquetColumnIndex` indexes
    // `RowGroup.columns` directly.

    @Test
    void extractStatsKeysOutputByLeafFieldId()
    {
        // Two flat columns in row group.
        FileMetaData file = fileMetaData(
                rowGroup(100L,
                        column(INT_TYPE, intBytes(1), intBytes(5), 0L),
                        column(BYTE_ARRAY_TYPE, utf8Bytes("alpha"), utf8Bytes("delta"), 1L)));

        List<DucklakeFileColumnStats> stats = DucklakeStatsExtractor.extractStats(
                file,
                List.of(
                        new LeafStatsTarget(42, INTEGER, 0),
                        new LeafStatsTarget(43, VARCHAR, 1)));

        assertThat(stats).hasSize(2);
        assertThat(stats.get(0).columnId()).isEqualTo(42L);
        assertThat(stats.get(0).minValue()).contains("1");
        assertThat(stats.get(0).maxValue()).contains("5");
        assertThat(stats.get(0).nullCount()).isEqualTo(0L);
        assertThat(stats.get(1).columnId()).isEqualTo(43L);
        assertThat(stats.get(1).minValue()).contains("alpha");
        assertThat(stats.get(1).maxValue()).contains("delta");
        assertThat(stats.get(1).nullCount()).isEqualTo(1L);
    }

    @Test
    void extractStatsForStructLeavesUsesChildFieldIds()
    {
        // A table-shaped (id INTEGER, data ROW(a INTEGER, b VARCHAR)). Three parquet
        // leaves: id at index 0, data.a at index 1, data.b at index 2. Targets carry
        // the leaf-level field_ids (catalog children of the ROW).
        FileMetaData file = fileMetaData(
                rowGroup(50L,
                        column(INT_TYPE, intBytes(1), intBytes(3), 0L),
                        column(INT_TYPE, intBytes(7), intBytes(9), 2L),
                        column(BYTE_ARRAY_TYPE, utf8Bytes("aa"), utf8Bytes("zz"), 5L)));

        List<DucklakeFileColumnStats> stats = DucklakeStatsExtractor.extractStats(
                file,
                List.of(
                        new LeafStatsTarget(10, INTEGER, 0),
                        new LeafStatsTarget(21, INTEGER, 1),
                        new LeafStatsTarget(22, VARCHAR, 2)));

        assertThat(stats).extracting(DucklakeFileColumnStats::columnId).containsExactly(10L, 21L, 22L);
        assertThat(stats.get(1).minValue()).contains("7");
        assertThat(stats.get(1).maxValue()).contains("9");
        assertThat(stats.get(1).nullCount()).isEqualTo(2L);
        assertThat(stats.get(2).minValue()).contains("aa");
        assertThat(stats.get(2).maxValue()).contains("zz");
        assertThat(stats.get(2).nullCount()).isEqualTo(5L);
    }

    @Test
    void extractStatsRespectsNonContiguousParquetColumnIndices()
    {
        // Simulates an add_files case where a parquet column at index 1 has been
        // skipped (ignore_extra_columns) so the mapped leaves are at indices 0 and 2.
        // The extractor must look up the correct column chunk by the explicit index.
        FileMetaData file = fileMetaData(
                rowGroup(20L,
                        column(INT_TYPE, intBytes(1), intBytes(1), 0L),
                        column(INT_TYPE, intBytes(99), intBytes(99), 0L), // skipped
                        column(BYTE_ARRAY_TYPE, utf8Bytes("x"), utf8Bytes("y"), 0L)));

        List<DucklakeFileColumnStats> stats = DucklakeStatsExtractor.extractStats(
                file,
                List.of(
                        new LeafStatsTarget(100, INTEGER, 0),
                        new LeafStatsTarget(101, VARCHAR, 2)));

        assertThat(stats).extracting(DucklakeFileColumnStats::columnId).containsExactly(100L, 101L);
        assertThat(stats.get(0).maxValue()).contains("1");
        // If parquetColumnIndex were positional in the leaf list, the second target
        // would have picked up the skipped column's INT bytes and decoded "99" instead
        // of the VARCHAR-decoded "x"/"y" — pin against that regression.
        assertThat(stats.get(1).minValue().orElseThrow()).doesNotContain("99");
        assertThat(stats.get(1).minValue()).contains("x");
        assertThat(stats.get(1).maxValue()).contains("y");
    }

    @Test
    void extractStatsMergesStatisticsAcrossRowGroups()
    {
        // Two row groups with overlapping ranges; min should be the file-wide
        // minimum (1) and max the file-wide maximum (12). null counts accumulate.
        FileMetaData file = fileMetaData(
                rowGroup(10L, column(INT_TYPE, intBytes(3), intBytes(8), 1L)),
                rowGroup(10L, column(INT_TYPE, intBytes(1), intBytes(12), 2L)));

        List<DucklakeFileColumnStats> stats = DucklakeStatsExtractor.extractStats(
                file, List.of(new LeafStatsTarget(7, INTEGER, 0)));

        assertThat(stats).hasSize(1);
        assertThat(stats.get(0).valueCount()).isEqualTo(20L);
        assertThat(stats.get(0).nullCount()).isEqualTo(3L);
        // INTEGER stats merge numerically across row groups: file-wide min=1, max=12.
        // Guards the old regression where string compare picked "8" as max ("8" > "12"
        // lexically) even though DuckLake stores min/max as text.
        assertThat(stats.get(0).minValue()).contains("1");
        assertThat(stats.get(0).maxValue()).contains("12");
    }

    // ==================== Thrift FileMetaData builders ====================

    private static final Type INT_TYPE = Type.INT32;
    private static final Type BYTE_ARRAY_TYPE = Type.BYTE_ARRAY;

    private static FileMetaData fileMetaData(RowGroup... rowGroups)
    {
        FileMetaData fm = new FileMetaData();
        fm.setVersion(1);
        fm.setSchema(List.of(new SchemaElement("root")));
        fm.setNum_rows(0L);
        fm.setRow_groups(List.of(rowGroups));
        return fm;
    }

    private static RowGroup rowGroup(long valueCountPerColumn, ColumnChunk... columns)
    {
        RowGroup rg = new RowGroup();
        rg.setColumns(List.of(columns));
        // num_rows must be set on a valid thrift RowGroup, but extractStats reads
        // num_values from each column chunk, not row count.
        rg.setNum_rows(valueCountPerColumn);
        rg.setTotal_byte_size(0L);
        // overwrite per-column num_values to the requested value
        for (ColumnChunk chunk : columns) {
            chunk.getMeta_data().setNum_values(valueCountPerColumn);
        }
        return rg;
    }

    private static ColumnChunk column(Type type, byte[] minValue, byte[] maxValue, long nullCount)
    {
        ColumnMetaData meta = new ColumnMetaData();
        meta.setType(type);
        meta.setEncodings(List.of(Encoding.PLAIN));
        meta.setPath_in_schema(List.of("leaf"));
        meta.setCodec(CompressionCodec.UNCOMPRESSED);
        meta.setNum_values(0L); // overwritten per row group
        meta.setTotal_uncompressed_size(0L);
        meta.setTotal_compressed_size(0L);
        meta.setData_page_offset(0L);

        Statistics stats = new Statistics();
        stats.setMin_value(minValue);
        stats.setMax_value(maxValue);
        stats.setNull_count(nullCount);
        meta.setStatistics(stats);

        ColumnChunk chunk = new ColumnChunk();
        chunk.setFile_offset(0L);
        chunk.setMeta_data(meta);
        return chunk;
    }

    private static byte[] intBytes(int v)
    {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).array();
    }

    private static byte[] utf8Bytes(String s)
    {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
