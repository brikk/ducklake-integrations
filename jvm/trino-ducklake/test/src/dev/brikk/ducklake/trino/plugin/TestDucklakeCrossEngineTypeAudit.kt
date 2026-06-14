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

import dev.brikk.ducklake.catalog.testing.CatalogQueries
import dev.brikk.ducklake.catalog.testing.CatalogTestSupport
import io.trino.testing.MaterializedResult
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import java.sql.DriverManager
import java.util.function.Consumer

/**
 * Type-coverage slice of cross-engine compatibility: DuckDB writes a fixture, Trino reads it
 * back. Exercises the inlined-then-Parquet flush boundary on ALTER TABLE ADD COLUMN, the
 * scalar inlined read path (date/timestamp/blob/varchar-with-null-byte), the full `list<T>`
 * read audit through both the inlined PG-text parser and Trino's Parquet array reader,
 * and DuckDB's wide-int types (HUGEINT/UHUGEINT) widening on read.
 *
 *
 * Each `list<T>` test goes through [runListRoundTrip] which writes the rows
 * twice — inlined via `data_inlining_row_limit=100` and again flushed to Parquet via
 * `0` — and checks both outcomes against the same assertion closure. See
 * `DucklakeInlinedValueConverter.parseDucklakeListText` for the inlined-path contract
 * and `dev-docs/archive/COMPARE-pg_ducklake.md` B2 for background.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
internal class TestDucklakeCrossEngineTypeAudit : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String {
        return "cross-engine-type-audit"
    }

    // ==================== Inlined ALTER TABLE ADD COLUMN flows ====================

    @Test
    @Throws(Exception::class)
    fun testDuckdbInlineAlterThenInlineMatchesTrino() {
        val tableName = "xengine_inline_alter_inline"
        val fullDuckdbTable = "ducklake_db.test_schema.$tableName"
        val fullTrinoTable = "test_schema.$tableName"

        try {
            createDuckdbConnection().use { duckConn ->
                duckConn.createStatement().use { duckStmt ->
                    duckStmt.execute("DROP TABLE IF EXISTS $fullDuckdbTable")
                    duckStmt.execute("CREATE TABLE $fullDuckdbTable (id INTEGER, name VARCHAR)")
                    duckStmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 10, schema => 'test_schema', table_name => '$tableName')")
                    duckStmt.execute("INSERT INTO $fullDuckdbTable VALUES (1, 'pre_1'), (2, 'pre_2'), (3, 'pre_3'), (4, 'pre_4')")
                    duckStmt.execute("ALTER TABLE $fullDuckdbTable ADD COLUMN score INTEGER")
                    duckStmt.execute("INSERT INTO $fullDuckdbTable VALUES (5, 'post_1', 50), (6, 'post_2', 60), (7, 'post_3', 70), (8, 'post_4', 80)")
                }
            }

            val duckdbRows: MutableList<String> = ArrayList()
            createDuckdbConnection().use { duckConn ->
                duckConn.createStatement().use { duckStmt ->
                    duckStmt.executeQuery("SELECT id, name, score FROM $fullDuckdbTable ORDER BY id").use { rs ->
                        while (rs.next()) {
                            duckdbRows.add(formatRow(rs.getObject(1), rs.getObject(2), rs.getObject(3)))
                        }
                    }
                }
            }

            assertThat(duckdbRows).containsExactly(
                    "1|pre_1|null",
                    "2|pre_2|null",
                    "3|pre_3|null",
                    "4|pre_4|null",
                    "5|post_1|50",
                    "6|post_2|60",
                    "7|post_3|70",
                    "8|post_4|80")

            val catalog = getIsolatedCatalog()
            DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { pgConn ->
                val dsl = CatalogTestSupport.dsl(pgConn)
                val snapshotId = CatalogQueries.latestSnapshotId(dsl)
                val tableId = CatalogQueries.activeTableId(dsl, tableName)

                val activeInlineRowsBySchemaVersion: Map<Long, Long> =
                        CatalogQueries.activeInlinedRowCountsBySchemaVersion(dsl, tableId, snapshotId)
                val totalActiveInlineRows = activeInlineRowsBySchemaVersion.values.stream()
                        .mapToLong { it }
                        .sum()

                // With 4 + 4 rows and limit 10, DuckDB should keep rows in metadata inlined tables.
                assertThat(CatalogQueries.activeDataFileCount(dsl, tableId))
                        .`as`("DuckDB active Parquet data files after inline->alter->inline")
                        .isZero()
                assertThat(activeInlineRowsBySchemaVersion.size)
                        .`as`("DuckDB inlined schema-version tables after ADD COLUMN")
                        .isGreaterThanOrEqualTo(2)
                assertThat(totalActiveInlineRows)
                        .`as`("DuckDB active inlined rows across schema versions")
                        .isEqualTo(8)
            }

            val trinoResult = computeActual("SELECT id, name, score FROM $fullTrinoTable ORDER BY id")
            val trinoRows = trinoResult.materializedRows.stream()
                    .map { row -> formatRow(row.getField(0), row.getField(1), row.getField(2)) }
                    .toList()

            assertThat(trinoRows).isEqualTo(duckdbRows)
        }
        finally {
            tryDropTable(fullTrinoTable)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbNineAlterNineStaysInlinedAndMatchesTrino() {
        val tableName = "xengine_inline9_alter_inline9"
        val fullDuckdbTable = "ducklake_db.test_schema.$tableName"
        val fullTrinoTable = "test_schema.$tableName"

        try {
            createDuckdbConnection().use { duckConn ->
                duckConn.createStatement().use { duckStmt ->
                    duckStmt.execute("DROP TABLE IF EXISTS $fullDuckdbTable")
                    duckStmt.execute("CREATE TABLE $fullDuckdbTable (id INTEGER, name VARCHAR)")
                    duckStmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 10, schema => 'test_schema', table_name => '$tableName')")
                    duckStmt.execute("INSERT INTO $fullDuckdbTable VALUES " + buildInlineValues(1, 9, false))
                    duckStmt.execute("ALTER TABLE $fullDuckdbTable ADD COLUMN score INTEGER")
                    duckStmt.execute("INSERT INTO $fullDuckdbTable VALUES " + buildInlineValues(10, 18, true))
                }
            }

            val duckdbRows: MutableList<String> = ArrayList()
            createDuckdbConnection().use { duckConn ->
                duckConn.createStatement().use { duckStmt ->
                    duckStmt.executeQuery("SELECT id, name, score FROM $fullDuckdbTable ORDER BY id").use { rs ->
                        while (rs.next()) {
                            duckdbRows.add(formatRow(rs.getObject(1), rs.getObject(2), rs.getObject(3)))
                        }
                    }
                }
            }

            assertThat(duckdbRows).hasSize(18)
            assertThat(duckdbRows.first()).isEqualTo("1|pre_1|null")
            assertThat(duckdbRows[8]).isEqualTo("9|pre_9|null")
            assertThat(duckdbRows[9]).isEqualTo("10|post_10|100")
            assertThat(duckdbRows.last()).isEqualTo("18|post_18|180")

            val catalog = getIsolatedCatalog()
            DriverManager.getConnection(catalog.jdbcUrl, catalog.user, catalog.password).use { pgConn ->
                val dsl = CatalogTestSupport.dsl(pgConn)
                val snapshotId = CatalogQueries.latestSnapshotId(dsl)
                val tableId = CatalogQueries.activeTableId(dsl, tableName)

                val activeInlineRowsBySchemaVersion: Map<Long, Long> =
                        CatalogQueries.activeInlinedRowCountsBySchemaVersion(dsl, tableId, snapshotId)
                val totalActiveInlineRows = activeInlineRowsBySchemaVersion.values.stream()
                        .mapToLong { it }
                        .sum()

                // Even with 9 + 9 rows around ALTER, DuckDB keeps rows in inlined metadata tables.
                assertThat(CatalogQueries.activeDataFileCount(dsl, tableId))
                        .`as`("DuckDB active Parquet data files after inline9->alter->inline9")
                        .isZero()
                assertThat(activeInlineRowsBySchemaVersion.size)
                        .`as`("DuckDB inlined schema-version tables after ADD COLUMN")
                        .isGreaterThanOrEqualTo(2)
                assertThat(totalActiveInlineRows)
                        .`as`("DuckDB active inlined rows across schema versions")
                        .isEqualTo(18)
            }

            val trinoResult = computeActual("SELECT id, name, score FROM $fullTrinoTable ORDER BY id")
            val trinoRows = trinoResult.materializedRows.stream()
                    .map { row -> formatRow(row.getField(0), row.getField(1), row.getField(2)) }
                    .toList()

            assertThat(trinoRows).isEqualTo(duckdbRows)
        }
        finally {
            tryDropTable(fullTrinoTable)
        }
    }

    // ==================== list<T> read audit ====================
    //
    // Each test below runs through `runListRoundTrip` which writes DuckDB rows twice — once kept
    // in the inlined metadata table (`data_inlining_row_limit=100`) and once flushed to Parquet
    // (default `data_inlining_row_limit=0`) — and reads both back through Trino with the same
    // assertions. That covers both the inlined PG-text parser in `DucklakeInlinedValueConverter`
    // and Trino's Parquet array reader on DuckDB-written files.

    @Test
    @Throws(Exception::class)
    fun testDuckdbListIntReadsInTrino() {
        runListRoundTrip(
                "xengine_list_int",
                "INTEGER[]",
                "(1, [10, 20, 30]), (2, [42]), (3, NULL), (4, [100, NULL, 300]), (5, [])",
                5,
                Consumer { result ->
                    assertThat(result.materializedRows).hasSize(5)
                    assertThat(result.materializedRows[0].getField(1)).isEqualTo(listOf(10, 20, 30))
                    assertThat(result.materializedRows[1].getField(1)).isEqualTo(listOf(42))
                    assertThat(result.materializedRows[2].getField(1)).isNull()
                    assertThat(result.materializedRows[3].getField(1)).isEqualTo(listOf(100, null, 300))
                    assertThat(result.materializedRows[4].getField(1)).isEqualTo(listOf<Int>())
                })
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbListBigintReadsInTrino() {
        runListRoundTrip(
                "xengine_list_bigint",
                "BIGINT[]",
                "(1, [1, 9223372036854775807, -9223372036854775808]), (2, [42, NULL]), (3, NULL)",
                3,
                Consumer { result ->
                    assertThat(result.materializedRows).hasSize(3)
                    assertThat(result.materializedRows[0].getField(1))
                            .isEqualTo(listOf(1L, 9223372036854775807L, Long.MIN_VALUE))
                    assertThat(result.materializedRows[1].getField(1)).isEqualTo(listOf(42L, null))
                    assertThat(result.materializedRows[2].getField(1)).isNull()
                })
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbListSmallintReadsInTrino() {
        runListRoundTrip(
                "xengine_list_smallint",
                "SMALLINT[]",
                "(1, [1, 32767, -32768, NULL])",
                1,
                Consumer { result ->
                    assertThat(result.materializedRows).hasSize(1)
                    assertThat(result.materializedRows[0].getField(1))
                            .isEqualTo(listOf(1.toShort(), 32767.toShort(), (-32768).toShort(), null))
                })
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbListTinyintReadsInTrino() {
        runListRoundTrip(
                "xengine_list_tinyint",
                "TINYINT[]",
                "(1, [1, 127, -128, NULL])",
                1,
                Consumer { result ->
                    assertThat(result.materializedRows).hasSize(1)
                    assertThat(result.materializedRows[0].getField(1))
                            .isEqualTo(listOf(1.toByte(), 127.toByte(), (-128).toByte(), null))
                })
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbListBooleanReadsInTrino() {
        runListRoundTrip(
                "xengine_list_bool",
                "BOOLEAN[]",
                "(1, [true, false, NULL])",
                1,
                Consumer { result ->
                    assertThat(result.materializedRows).hasSize(1)
                    assertThat(result.materializedRows[0].getField(1))
                            .isEqualTo(listOf(true, false, null))
                })
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbListDoubleReadsInTrino() {
        runListRoundTrip(
                "xengine_list_double",
                "DOUBLE[]",
                "(1, [1.5, -2.25, 0.0, NULL])",
                1,
                Consumer { result ->
                    assertThat(result.materializedRows).hasSize(1)
                    assertThat(result.materializedRows[0].getField(1))
                            .isEqualTo(listOf(1.5, -2.25, 0.0, null))
                })
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbListRealReadsInTrino() {
        runListRoundTrip(
                "xengine_list_real",
                "REAL[]",
                "(1, [1.5, -2.25, 0.0, NULL])",
                1,
                Consumer { result ->
                    assertThat(result.materializedRows).hasSize(1)
                    assertThat(result.materializedRows[0].getField(1))
                            .isEqualTo(listOf(1.5f, -2.25f, 0.0f, null))
                })
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbListDecimalReadsInTrino() {
        runListRoundTrip(
                "xengine_list_decimal",
                "DECIMAL(10,2)[]",
                "(1, [12.34, -56.78, NULL])",
                1,
                Consumer { result ->
                    assertThat(result.materializedRows).hasSize(1)
                    assertThat(result.materializedRows[0].getField(1))
                            .isEqualTo(listOf(
                                    java.math.BigDecimal("12.34"),
                                    java.math.BigDecimal("-56.78"),
                                    null))
                })
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbListVarcharReadsInTrino() {
        // Covers the tricky parts of the inlined text path: single-quoted elements, `\'` backslash
        // escape for an embedded apostrophe, commas inside quoted strings, empty strings, NULL
        // elements, and whole-list NULL. The Parquet path routes through Trino's native reader.
        runListRoundTrip(
                "xengine_list_varchar",
                "VARCHAR[]",
                "(1, ['abc', 'def']), (2, ['it''s', 'a, b', '']), (3, ['x', NULL, 'y']), (4, NULL)",
                4,
                Consumer { result ->
                    assertThat(result.materializedRows).hasSize(4)
                    assertThat(result.materializedRows[0].getField(1)).isEqualTo(listOf("abc", "def"))
                    assertThat(result.materializedRows[1].getField(1)).isEqualTo(listOf("it's", "a, b", ""))
                    assertThat(result.materializedRows[2].getField(1)).isEqualTo(listOf("x", null, "y"))
                    assertThat(result.materializedRows[3].getField(1)).isNull()
                })
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbListDateReadsInTrino() {
        runListRoundTrip(
                "xengine_list_date",
                "DATE[]",
                "(1, [DATE '2024-06-15', DATE '2024-12-31', NULL])",
                1,
                Consumer { result ->
                    assertThat(result.materializedRows).hasSize(1)
                    val days = result.materializedRows[0].getField(1) as List<*>
                    assertThat(days).hasSize(3)
                    assertThat(days[0].toString()).isEqualTo("2024-06-15")
                    assertThat(days[1].toString()).isEqualTo("2024-12-31")
                    assertThat(days[2]).isNull()
                })
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbListTimestampReadsInTrino() {
        runListRoundTrip(
                "xengine_list_ts",
                "TIMESTAMP[]",
                "(1, [TIMESTAMP '2024-06-15 12:34:56.123456', NULL])",
                1,
                Consumer { result ->
                    assertThat(result.materializedRows).hasSize(1)
                    val tss = result.materializedRows[0].getField(1) as List<*>
                    assertThat(tss).hasSize(2)
                    assertThat(tss[0].toString()).contains("2024-06-15")
                    assertThat(tss[0].toString()).contains("12:34:56.123456")
                    assertThat(tss[1]).isNull()
                })
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbListTimestampTzReadsInTrino() {
        runListRoundTrip(
                "xengine_list_tstz",
                "TIMESTAMPTZ[]",
                "(1, [TIMESTAMPTZ '2024-06-15 12:34:56.123456+00', NULL])",
                1,
                Consumer { result ->
                    assertThat(result.materializedRows).hasSize(1)
                    val tss = result.materializedRows[0].getField(1) as List<*>
                    assertThat(tss).hasSize(2)
                    assertThat(tss[0].toString()).contains("2024-06-15")
                    assertThat(tss[0].toString()).contains("12:34:56.123456")
                    assertThat(tss[1]).isNull()
                })
    }

    // ==================== Partial coverage: list<blob> ====================
    //
    // `list<blob>` still fails end-to-end on the inlined path (see TODO-READ-MODE § Inlined-Read
    // Type Gaps): we do not decode DuckDB's `\xNN` text escapes back into bytes. May work on the
    // Parquet path (Trino's Parquet reader handles ARRAY<BINARY>) but unconfirmed until the
    // inlined-path fix lands. Kept `@Disabled` so the missing behavior stays tracked — don't
    // delete.

    @Test
    @Throws(Exception::class)
    fun testDuckdbListBlobReadsInTrino() {
        runListRoundTrip(
                "xengine_list_blob",
                "BLOB[]",
                "(1, ['\\x00\\x01\\xFF'::BLOB, NULL])",
                1,
                Consumer { result ->
                    assertThat(result.materializedRows).hasSize(1)
                    val vals = result.materializedRows[0].getField(1) as List<*>
                    assertThat(vals).hasSize(2)
                    assertThat(vals[0]).isInstanceOf(ByteArray::class.java)
                    assertThat(vals[0] as ByteArray).containsExactly(0x00.toByte(), 0x01.toByte(), 0xFF.toByte())
                    assertThat(vals[1]).isNull()
                })
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbListUuidReadsInTrino() {
        runListRoundTrip(
                "xengine_list_uuid",
                "UUID[]",
                "(1, [UUID '550e8400-e29b-41d4-a716-446655440000', NULL])",
                1,
                Consumer { result ->
                    assertThat(result.materializedRows).hasSize(1)
                    val ids = result.materializedRows[0].getField(1) as List<*>
                    assertThat(ids).hasSize(2)
                    assertThat(ids[0].toString()).isEqualTo("550e8400-e29b-41d4-a716-446655440000")
                    assertThat(ids[1]).isNull()
                })
    }

    // ==================== Inlined scalar reads ====================

    @Test
    @Throws(Exception::class)
    fun testDuckdbInlinedDateAndTimestampReadInTrino() {
        val tableName = "xengine_inlined_date_ts"
        val fullDuckdb = "ducklake_db.test_schema.$tableName"
        val fullTrino = "test_schema.$tableName"
        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("DROP TABLE IF EXISTS $fullDuckdb")
                    stmt.execute("CREATE TABLE $fullDuckdb (id INTEGER, d DATE, ts TIMESTAMP)")
                    stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 100, schema => 'test_schema', table_name => '$tableName')")
                    stmt.execute("INSERT INTO $fullDuckdb VALUES " +
                            "(1, DATE '2024-06-15', TIMESTAMP '2024-06-15 12:34:56.123456'), " +
                            "(2, NULL, NULL)")
                }
            }
            assertRowsStayedInlined(tableName, 2)

            val result = computeActual("SELECT id, d, ts FROM $fullTrino ORDER BY id")
            assertThat(result.materializedRows).hasSize(2)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(1).toString()).isEqualTo("2024-06-15")
            // Upstream serializes timestamps into VARCHAR columns as ISO strings; our converter
            // must parse them back to TIMESTAMP_MICROS. Don't assert exact form here — just that
            // Trino got a non-null value with the expected microsecond component visible.
            assertThat(result.materializedRows[0].getField(2)).isNotNull()
            assertThat(result.materializedRows[0].getField(2).toString()).contains("2024-06-15")
            assertThat(result.materializedRows[0].getField(2).toString()).contains("12:34:56.123456")
            assertThat(result.materializedRows[1].getField(1)).isNull()
            assertThat(result.materializedRows[1].getField(2)).isNull()
        }
        finally {
            tryDropTable(fullTrino)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbInlinedUuidReadsInTrino() {
        val tableName = "xengine_inlined_uuid"
        val fullDuckdb = "ducklake_db.test_schema.$tableName"
        val fullTrino = "test_schema.$tableName"
        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("DROP TABLE IF EXISTS $fullDuckdb")
                    stmt.execute("CREATE TABLE $fullDuckdb (id INTEGER, key UUID)")
                    stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 100, schema => 'test_schema', table_name => '$tableName')")
                    stmt.execute("INSERT INTO $fullDuckdb VALUES " +
                            "(1, UUID '550e8400-e29b-41d4-a716-446655440000'), " +
                            "(2, NULL)")
                }
            }
            assertRowsStayedInlined(tableName, 2)

            val result = computeActual("SELECT id, key FROM $fullTrino ORDER BY id")
            assertThat(result.materializedRows).hasSize(2)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(1).toString())
                    .isEqualTo("550e8400-e29b-41d4-a716-446655440000")
            assertThat(result.materializedRows[1].getField(1)).isNull()
        }
        finally {
            tryDropTable(fullTrino)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbInlinedBlobReadsInTrino() {
        val tableName = "xengine_inlined_blob"
        val fullDuckdb = "ducklake_db.test_schema.$tableName"
        val fullTrino = "test_schema.$tableName"
        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("DROP TABLE IF EXISTS $fullDuckdb")
                    stmt.execute("CREATE TABLE $fullDuckdb (id INTEGER, payload BLOB)")
                    stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 100, schema => 'test_schema', table_name => '$tableName')")
                    // Include a null byte to confirm BYTEA (not TEXT) is actually used upstream.
                    stmt.execute("INSERT INTO $fullDuckdb VALUES (1, '\\x00\\x01\\x02\\xFF'::BLOB), (2, NULL)")
                }
            }
            assertRowsStayedInlined(tableName, 2)

            val result = computeActual("SELECT id, payload FROM $fullTrino ORDER BY id")
            assertThat(result.materializedRows).hasSize(2)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
            // Whatever representation Trino chooses, it must preserve all four bytes.
            val payload = result.materializedRows[0].getField(1)
            assertThat(payload).isNotNull()
            if (payload is ByteArray) {
                assertThat(payload).containsExactly(0x00.toByte(), 0x01.toByte(), 0x02.toByte(), 0xFF.toByte())
            }
            assertThat(result.materializedRows[1].getField(1)).isNull()
        }
        finally {
            tryDropTable(fullTrino)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbInlinedVarcharWithEmbeddedNullByteReadsInTrino() {
        // pg_ducklake docs/data_types.md explicitly calls out that DuckDB VARCHAR can carry
        // embedded null bytes and that PG TEXT/VARCHAR cannot, which is why upstream stores
        // inlined VARCHAR as BYTEA. This test probes whether our read path survives a null byte
        // — if this fails with "null character not permitted", we likely need the BYTEA-aware
        // read path that pg_ducklake documents.
        val tableName = "xengine_inlined_varchar_nullbyte"
        val fullDuckdb = "ducklake_db.test_schema.$tableName"
        val fullTrino = "test_schema.$tableName"
        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("DROP TABLE IF EXISTS $fullDuckdb")
                    stmt.execute("CREATE TABLE $fullDuckdb (id INTEGER, s VARCHAR)")
                    stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', 100, schema => 'test_schema', table_name => '$tableName')")
                    stmt.execute("INSERT INTO $fullDuckdb VALUES (1, 'ABC' || chr(0) || '123')")
                }
            }
            assertRowsStayedInlined(tableName, 1)

            // Current expectation: this may throw — that's informative. If it returns a value,
            // verify the null byte survived. Either way, don't make the test falsely pass.
            val result = computeActual("SELECT id, length(s), s FROM $fullTrino")
            assertThat(result.materializedRows).hasSize(1)
            assertThat(result.materializedRows[0].getField(1))
                    .`as`("length must be 7 if the null byte round-tripped")
                    .isEqualTo(7L)
        }
        finally {
            tryDropTable(fullTrino)
        }
    }

    // ==================== hugeint / uhugeint ====================

    @Test
    @Throws(Exception::class)
    fun testDuckdbHugeintColumnReadsAsDecimalInTrino() {
        val tableName = "xengine_hugeint"
        val fullDuckdb = "ducklake_db.test_schema.$tableName"
        val fullTrino = "test_schema.$tableName"
        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("DROP TABLE IF EXISTS $fullDuckdb")
                    stmt.execute("CREATE TABLE $fullDuckdb (id INTEGER, big HUGEINT)")
                    // A value that fits in DECIMAL(38, 0). Values past ~1.7e38 would overflow.
                    stmt.execute("INSERT INTO $fullDuckdb VALUES (1, 99999999999999999999999999999999999999)")
                }
            }

            val result = computeActual("SELECT id, CAST(big AS VARCHAR) FROM $fullTrino")
            assertThat(result.materializedRows).hasSize(1)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(1))
                    .isEqualTo("99999999999999999999999999999999999999")
        }
        finally {
            tryDropTable(fullTrino)
        }
    }

    @Test
    @Throws(Exception::class)
    fun testDuckdbUhugeintColumnReadsAsVarcharInTrino() {
        val tableName = "xengine_uhugeint"
        val fullDuckdb = "ducklake_db.test_schema.$tableName"
        val fullTrino = "test_schema.$tableName"
        try {
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("DROP TABLE IF EXISTS $fullDuckdb")
                    stmt.execute("CREATE TABLE $fullDuckdb (id INTEGER, big UHUGEINT)")
                    // Max uhugeint exceeds DECIMAL(38) — that's why we degrade to VARCHAR.
                    stmt.execute("INSERT INTO $fullDuckdb VALUES (1, 340282366920938463463374607431768211455)")
                }
            }

            val result = computeActual("SELECT id, big FROM $fullTrino")
            assertThat(result.materializedRows).hasSize(1)
            assertThat(result.materializedRows[0].getField(0)).isEqualTo(1)
            assertThat(result.materializedRows[0].getField(1).toString())
                    .isEqualTo("340282366920938463463374607431768211455")
        }
        finally {
            tryDropTable(fullTrino)
        }
    }

    // ==================== Helpers ====================

    /**
     * Runs a single-column `list<T>` round-trip test (DuckDB writer → Trino reader) twice:
     * once with rows kept in the inlined metadata table, once with rows flushed to Parquet.
     * Both paths share the same assertion closure, which validates the read-back result.
     *
     *
     * The table schema is always `(id INTEGER, val <ducklakeListDdl>)`; `insertValues`
     * is the body after `INSERT INTO <tbl> VALUES`. The closure is handed a result set of
     * `SELECT id, val FROM <tbl> ORDER BY id`.
     */
    @Throws(Exception::class)
    private fun runListRoundTrip(
            baseName: String,
            ducklakeListDdl: String,
            insertValuesSql: String,
            expectedRowCount: Int,
            assertions: Consumer<MaterializedResult>) {
        for (mode in Mode.values()) {
            val tableName = baseName + mode.suffix
            val fullDuckdb = "ducklake_db.test_schema.$tableName"
            val fullTrino = "test_schema.$tableName"
            try {
                createDuckdbConnection().use { duck ->
                    duck.createStatement().use { stmt ->
                        stmt.execute("DROP TABLE IF EXISTS $fullDuckdb")
                        stmt.execute("CREATE TABLE $fullDuckdb (id INTEGER, val $ducklakeListDdl)")
                        // DuckDB's default `ducklake_default_data_inlining_row_limit` is 10, so for
                        // small fixtures the PARQUET mode would otherwise silently inline; force 0.
                        val inliningRowLimit = if (mode == Mode.INLINED) 100 else 0
                        stmt.execute("CALL ducklake_db.set_option('data_inlining_row_limit', $inliningRowLimit" +
                                ", schema => 'test_schema', table_name => '$tableName')")
                        stmt.execute("INSERT INTO $fullDuckdb VALUES $insertValuesSql")
                    }
                }
                if (mode == Mode.INLINED) {
                    assertRowsStayedInlined(tableName, expectedRowCount.toLong())
                }
                else {
                    assertRowsWrittenToParquet(tableName)
                }

                val result = computeActual("SELECT id, val FROM $fullTrino ORDER BY id")
                try {
                    assertions.accept(result)
                }
                catch (e: AssertionError) {
                    throw AssertionError("[$mode path] " + e.message, e)
                }
            }
            finally {
                tryDropTable(fullTrino)
            }
        }
    }

    private enum class Mode(val suffix: String) {
        INLINED("_inl"),
        PARQUET("_pq"),
    }

    companion object {
        private fun formatRow(id: Any?, name: Any?, score: Any?): String {
            return id.toString() + "|" + name.toString() + "|" + score.toString()
        }

        private fun buildInlineValues(startInclusive: Int, endInclusive: Int, withScore: Boolean): String {
            val rows: MutableList<String> = ArrayList()
            for (id in startInclusive..endInclusive) {
                val prefix = if (withScore) "post_" else "pre_"
                if (withScore) {
                    rows.add("(" + id + ", '" + prefix + id + "', " + (id * 10) + ")")
                }
                else {
                    rows.add("(" + id + ", '" + prefix + id + "')")
                }
            }
            return rows.joinToString(", ")
        }
    }
}
