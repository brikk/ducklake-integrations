package dev.brikk.ducklake.doris.plugin

import java.math.BigDecimal
import java.nio.file.Files

import dev.brikk.ducklake.catalog.DucklakeColumn

import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType

import org.junit.jupiter.api.Test

import org.assertj.core.api.Assertions.assertThat

/**
 * Round-trips DuckLake inlined rows through [DuckLakeInlinedParquetWriter] and
 * reads the Parquet back, asserting field ids (`== column_id`, which the BE
 * binds on), column names, scalar type encodings, nulls, and the DuckDB
 * text-form conversions jOOQ can hand back (blob `\xNN`, non-finite floats,
 * decimals). Pure — no cluster.
 *
 * Runs on JDK 17 (the `:doris-ducklake:test` task's launcher, matching the
 * Doris FE runtime), so parquet-format's shaded thrift is ABI-consistent — the
 * same environment the writer runs in inside the FE. (On a JDK 25 launcher the
 * shaded-thrift `PageHeader.setData_page_header` ABI break would appear; that
 * never happens on the FE's 17.)
 */
internal class DuckLakeInlinedParquetWriterTest {

    private var nextId = 0L

    private fun col(name: String, type: String, id: Long = ++nextId) =
        DucklakeColumn(id, 1L, null, 1L, id, name, type, true, null)

    // Reads via ParquetFileReader.open(LocalInputFile) — no Hadoop Path/UGI
    // (broken on JDK 25).
    private fun writeAndReadBack(
        columns: List<DucklakeColumn>,
        rows: List<List<Any?>>,
    ): Pair<List<Group>, MessageType> {
        val file = Files.createTempFile("inlined-writer-test", ".parquet")
        Files.deleteIfExists(file)
        DuckLakeInlinedParquetWriter.write(file, columns, rows)
        val out = ArrayList<Group>()
        val schema: MessageType
        ParquetFileReader.open(LocalInputFile(file)).use { reader ->
            schema = reader.footer.fileMetaData.schema
            val columnIO = ColumnIOFactory().getColumnIO(schema)
            var pages: PageReadStore? = reader.readNextRowGroup()
            while (pages != null) {
                val rowCount = pages.rowCount
                val recordReader = columnIO.getRecordReader(pages, GroupRecordConverter(schema))
                for (i in 0 until rowCount) {
                    out.add(recordReader.read())
                }
                pages = reader.readNextRowGroup()
            }
        }
        Files.deleteIfExists(file)
        return out to schema
    }

    private fun fieldId(schema: MessageType, name: String): Int =
        schema.getType(name).id.intValue()

    @Test
    fun writesFieldIdsEqualToColumnIdAndTableColumnNames() {
        val cols = listOf(col("id", "int32", id = 7), col("name", "varchar", id = 9))
        val (recs, schema) = writeAndReadBack(cols, listOf(listOf(1, "alice")))

        // Field ids == column ids (the DuckLake invariant the BE matches on).
        assertThat(fieldId(schema, "id")).isEqualTo(7)
        assertThat(fieldId(schema, "name")).isEqualTo(9)
        assertThat(schema.fields.map { it.name }).containsExactly("id", "name")
        assertThat(recs[0].getInteger("id", 0)).isEqualTo(1)
        assertThat(recs[0].getBinary("name", 0).toStringUsingUTF8()).isEqualTo("alice")
    }

    @Test
    fun preservesNulls() {
        val cols = listOf(col("id", "int64"), col("note", "varchar"))
        val (recs, _) = writeAndReadBack(cols, listOf(listOf(1L, null)))
        assertThat(recs[0].getLong("id", 0)).isEqualTo(1L)
        // A null OPTIONAL column has zero repetition in the group.
        assertThat(recs[0].getFieldRepetitionCount("note")).isEqualTo(0)
    }

    @Test
    fun convertsIntegerFamilyFromTypedAndTextForms() {
        val cols = listOf(col("a", "int8"), col("b", "int16"), col("c", "int32"), col("d", "int64"))
        val (recs, _) = writeAndReadBack(cols, listOf(listOf<Any?>(1.toByte(), "200", 3, 4_000_000_000L)))
        assertThat(recs[0].getInteger("a", 0)).isEqualTo(1)
        assertThat(recs[0].getInteger("b", 0)).isEqualTo(200)
        assertThat(recs[0].getInteger("c", 0)).isEqualTo(3)
        assertThat(recs[0].getLong("d", 0)).isEqualTo(4_000_000_000L)
    }

    @Test
    fun convertsFloatsIncludingNonFiniteTextTokens() {
        val cols = listOf(col("f", "float32"), col("g", "float64"))
        val (recs, _) = writeAndReadBack(cols, listOf(listOf<Any?>("inf", "-infinity")))
        assertThat(recs[0].getFloat("f", 0)).isEqualTo(Float.POSITIVE_INFINITY)
        assertThat(recs[0].getDouble("g", 0)).isEqualTo(Double.NEGATIVE_INFINITY)
    }

    @Test
    fun decimalIsBinaryWithDecimalAnnotationAndUnscaledValue() {
        val cols = listOf(col("amount", "decimal(10,2)"))
        val (recs, schema) = writeAndReadBack(cols, listOf(listOf<Any?>(BigDecimal("1.50"))))
        val ann = schema.getType("amount").logicalTypeAnnotation
        assertThat(ann).isInstanceOf(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation::class.java)
        // unscaled(1.50 @ scale 2) == 150
        val bytes = recs[0].getBinary("amount", 0).bytes
        assertThat(java.math.BigInteger(bytes).toInt()).isEqualTo(150)
    }

    @Test
    fun convertsBlobFromByteArrayAndDuckDbTextForm() {
        val cols = listOf(col("data", "blob"))
        val (recs, _) = writeAndReadBack(
            cols,
            listOf(listOf<Any?>(byteArrayOf(0x01, 0x02)), listOf<Any?>("a\\x00b")),
        )
        assertThat(recs[0].getBinary("data", 0).bytes).containsExactly(0x01, 0x02)
        assertThat(recs[1].getBinary("data", 0).bytes)
            .containsExactly('a'.code.toByte(), 0x00, 'b'.code.toByte())
    }

    @Test
    fun convertsDateToEpochDay() {
        val cols = listOf(col("d", "date"))
        val (recs, _) = writeAndReadBack(cols, listOf(listOf<Any?>(java.time.LocalDate.of(1970, 1, 11))))
        assertThat(recs[0].getInteger("d", 0)).isEqualTo(10)
    }

    @Test
    fun convertsTimestampToEpochMicros() {
        val cols = listOf(col("ts", "timestamp"))
        val dt = java.time.LocalDateTime.of(1970, 1, 1, 0, 0, 1, 500_000_000)
        val (recs, _) = writeAndReadBack(cols, listOf(listOf<Any?>(dt)))
        // 1.5 seconds after epoch = 1_500_000 micros.
        assertThat(recs[0].getLong("ts", 0)).isEqualTo(1_500_000L)
    }
}
