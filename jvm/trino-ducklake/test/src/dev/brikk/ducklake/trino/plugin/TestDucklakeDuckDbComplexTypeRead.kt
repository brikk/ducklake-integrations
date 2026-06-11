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

import io.trino.spi.Page
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.ArrayType
import io.trino.spi.type.DecimalType
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.RowType
import io.trino.spi.type.TimestampType.TIMESTAMP_MICROS
import io.trino.spi.type.Type
import io.trino.spi.type.TypeOperators
import io.trino.spi.type.UuidType
import io.trino.spi.type.VarcharType.VARCHAR
import io.trino.spi.type.MapType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.sql.DriverManager

/**
 * Converter-focused read coverage for complex types over the `.db` ATTACH path: the fixture is
 * written with raw DuckDB SQL (so it can carry shapes the connector's writer does not produce
 * yet — timestamp/uuid/decimal ARRAY elements, ARRAY of STRUCT, MAP values with NULLs), then read
 * through the real [InProcessDuckDbExecutor] and materialized by [DucklakeArrowToPageConverter].
 * This pins the recursive ROW/MAP/nested-element support at value level; the
 * writer-producible subset is additionally covered end-to-end in
 * [TestDucklakeDuckDbArrowStreamWriter].
 *
 * No network needed — the `.db` ATTACH path uses no extensions.
 */
class TestDucklakeDuckDbComplexTypeRead {

    @Test
    fun complexTypesRoundTripThroughExecutorAndConverter() {
        val file = Files.createTempDirectory("duckdb-complex-read").resolve("complex.db")
        writeFixture(file)

        val columns = fixtureColumns()
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(file),
                columns,
                TupleDomain.all<DucklakeColumnHandle>())

        val converter = DucklakeArrowToPageConverter(columns.map { it.columnType })
        val rows = sortedMapOf<Int, List<Any?>>()
        InProcessDuckDbExecutor().execute(request).use { ctx ->
            val reader = ctx.arrowReader()
            while (reader.loadNextBatch()) {
                val page: Page = converter.convert(reader.vectorSchemaRoot)
                for (p in 0 until page.positionCount) {
                    val values = columns.indices.map { col ->
                        objectValue(columns[col].columnType, page, col, p)
                    }
                    rows[(values[0] as Number).toInt()] = values
                }
            }
        }

        assertThat(rows.keys).containsExactly(1, 2, 3)
        assertValueRow(rows[1]!!)
        assertEmptyShapesRow(rows[2]!!)
        val r3 = rows[3]!!
        for (col in 1..7) {
            assertThat(r3[col]).`as`("row 3, column ${columns[col].columnName} is null").isNull()
        }
    }

    private fun writeFixture(file: java.nio.file.Path) {
        DriverManager.getConnection("jdbc:duckdb:" + file.toAbsolutePath()).use { c ->
            c.createStatement().use { s ->
                s.execute("""
                    CREATE TABLE t AS SELECT * FROM (VALUES
                      (1,
                       {'i': 10, 's': 'alpha'},
                       MAP {'a': 1, 'b': NULL},
                       [TIMESTAMP '2024-01-01 12:34:56.123456', TIMESTAMP '2025-06-11 00:00:00'],
                       [{'i': 1, 's': 'x'}, {'i': 2, 's': NULL}],
                       {'tags': ['t1', 't2']},
                       [UUID 'aabbccdd-eeff-0011-2233-445566778899'],
                       [CAST('12345678901234567.123' AS DECIMAL(20,3))]),
                      (2,
                       {'i': NULL, 's': 'beta'},
                       MAP {},
                       [],
                       NULL,
                       {'tags': NULL},
                       NULL,
                       NULL),
                      (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                    ) v(id, st, mp, ts_arr, st_arr, nested, uuid_arr, dec_arr)
                """.trimIndent())
            }
        }
    }

    private fun fixtureColumns(): List<DucklakeColumnHandle> {
        val rowIS: RowType = RowType.from(listOf(
                RowType.field("i", INTEGER), RowType.field("s", VARCHAR)))
        val nestedRow: RowType = RowType.from(listOf(RowType.field("tags", ArrayType(VARCHAR))))
        return listOf(
                DucklakeColumnHandle(1L, "id", INTEGER, false),
                DucklakeColumnHandle(2L, "st", rowIS, true),
                DucklakeColumnHandle(3L, "mp", MapType(VARCHAR, INTEGER, TypeOperators()), true),
                DucklakeColumnHandle(4L, "ts_arr", ArrayType(TIMESTAMP_MICROS), true),
                DucklakeColumnHandle(5L, "st_arr", ArrayType(rowIS), true),
                DucklakeColumnHandle(6L, "nested", nestedRow, true),
                DucklakeColumnHandle(7L, "uuid_arr", ArrayType(UuidType.UUID), true),
                DucklakeColumnHandle(8L, "dec_arr", ArrayType(DecimalType.createDecimalType(20, 3)), true))
    }

    /** Row 1: every column populated, including nulls INSIDE complex values. */
    private fun assertValueRow(r1: List<Any?>) {
        assertThat(r1[1]).`as`("struct value").isEqualTo(listOf(10, "alpha"))
        assertThat(r1[2]).`as`("map with null value").isEqualTo(mapOf("a" to 1, "b" to null))
        assertThat((r1[3] as List<*>).map { it.toString() })
                .`as`("timestamp array elements")
                .containsExactly("2024-01-01 12:34:56.123456", "2025-06-11 00:00:00.000000")
        assertThat(r1[4]).`as`("array of struct, null field inside")
                .isEqualTo(listOf(listOf(1, "x"), listOf(2, null)))
        assertThat(r1[5]).`as`("struct containing array").isEqualTo(listOf(listOf("t1", "t2")))
        assertThat((r1[6] as List<*>).map { it.toString() })
                .`as`("uuid array").containsExactly("aabbccdd-eeff-0011-2233-445566778899")
        assertThat((r1[7] as List<*>).map { it.toString() })
                .`as`("long-decimal array").containsExactly("12345678901234567.123")
    }

    /** Row 2: empty/None shapes — empty map, empty array, null fields inside structs. */
    private fun assertEmptyShapesRow(r2: List<Any?>) {
        assertThat(r2[1]).`as`("struct with null field").isEqualTo(listOf(null, "beta"))
        assertThat(r2[2]).`as`("empty map").isEqualTo(emptyMap<String, Int>())
        assertThat(r2[3]).`as`("empty array").isEqualTo(emptyList<Any>())
        assertThat(r2[4]).`as`("null array").isNull()
        assertThat(r2[5]).`as`("struct with null array field").isEqualTo(listOf(null))
    }

    /** Materializes one value via the Trino type's object representation (List for ROW/ARRAY,
     * Map for MAP, SqlTimestamp/etc. for temporals). */
    private fun objectValue(type: Type, page: Page, channel: Int, position: Int): Any? =
        type.getObjectValue(page.getBlock(channel), position)
}
