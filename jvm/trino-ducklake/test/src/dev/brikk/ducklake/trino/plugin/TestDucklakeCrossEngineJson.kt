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

import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.Companion.DATA_FILE_FORMAT
import io.trino.Session
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * Native Trino JSON-type support (F8): DuckLake `json` maps to Trino's `io.trino.type.JsonType`
 * (physically a UTF-8 string in parquet / DuckDB `JSON`). Covers the four engine directions on
 * all three write formats:
 *
 *   - Trino writes JSON → Trino reads it back (parquet + duckdb-format appender + arrow-stream).
 *   - Trino writes JSON parquet → DuckDB reads it (cross-engine, via the shared PG-backed catalog).
 *   - DuckDB writes JSON → Trino reads it.
 *
 * The external-DuckDB read is asserted only for the PARQUET write. Upstream DuckLake never reads
 * the DATA-file `file_format` column at all — its scan select-list
 * (`DuckLakeMetadataManager::GetFileSelectList`) is only path/size/footer/encryption, and it hands
 * every data file straight to `parquet_scan` (`ducklake_scan.cpp` binds it). (Contrast
 * `GetDeleteFileSelectList`, which DOES read `.format` and dispatches parquet-vs-puffin — so the
 * machinery exists, it's just not wired to data files.) Consequently a connector-written
 * duckdb-format `.db` data file — a perfectly valid standalone DuckDB database, which is exactly
 * how our own read path ATTACHes it — is mis-routed into upstream's parquet reader and fails with
 * "No magic bytes", for ANY column type. Verified independent of the `trino/duckdb` catalog
 * namespacing (a bare `duckdb` format string fails identically) and of JSON (plain VARCHAR fails
 * the same way). Reported upstream: https://github.com/duckdb/ducklake/issues/1289. The
 * connector's duckdb data format is a Trino/Doris-internal storage optimization; parquet is the
 * cross-engine interchange format. So the duckdb-format cases assert the Trino round-trip only.
 *
 * Also pins that the catalog column type is `json` (not the old VARCHAR degradation).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
internal class TestDucklakeCrossEngineJson : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String = "cross-engine-json"

    private fun parquetSession(): Session =
            Session.builder(session)
                    .setCatalogSessionProperty("ducklake", DATA_FILE_FORMAT, DucklakeSessionProperties.FORMAT_PARQUET)
                    .build()

    /** Compact (already-canonical) JSON so Trino's whitespace-stripping canonicalization is a no-op. */
    private val doc1 = """{"a":1,"b":[1,2,3]}"""
    private val doc2 = """{"nested":{"x":true},"s":"hi"}"""

    private fun assertColumnTypeIsJson(fullTrinoTable: String) {
        val cols = computeActual("SHOW COLUMNS FROM $fullTrinoTable")
        val docType = cols.materializedRows.first { it.getField(0) == "doc" }.getField(1) as String
        assertThat(docType).`as`("catalog column type must be native json, not degraded varchar").isEqualTo("json")
    }

    private fun writeReadRoundTrip(
            session: Session,
            table: String,
            expectedFormat: String,
            crossEngineDuckdbRead: Boolean) {
        val fullTrinoTable = "test_schema.$table"
        try {
            computeActual(session, "CREATE TABLE $fullTrinoTable (id INTEGER, doc JSON)")
            computeActual(session,
                    "INSERT INTO $fullTrinoTable VALUES " +
                            "(1, JSON '$doc1'), (2, JSON '$doc2'), (3, CAST(NULL AS JSON))")

            assertColumnTypeIsJson(fullTrinoTable)

            assertThat(computeScalar("SELECT DISTINCT file_format FROM \"$table\$files\"") as String)
                    .isEqualTo(expectedFormat)

            // Trino reads back what it wrote (JSON renders as its canonical text).
            val rows = computeActual("SELECT id, json_format(doc) FROM $fullTrinoTable ORDER BY id")
                    .materializedRows
            assertThat(rows.map { it.getField(0) as Int to it.getField(1) as String? })
                    .containsExactly(1 to doc1, 2 to doc2, 3 to null)

            if (crossEngineDuckdbRead) {
                // DuckDB reads the same catalog: the JSON column comes back as text.
                assertThat(duckdbReadDocRows(fullTrinoTable))
                        .containsExactly(1 to doc1, 2 to doc2, 3 to null)
            }
        }
        finally {
            tryDropTable(fullTrinoTable)
        }
    }

    /** Reads (id, doc) from a DuckLake table via an external DuckDB connection over the shared catalog. */
    private fun duckdbReadDocRows(fullTrinoTable: String): List<Pair<Int, String?>> {
        val duckdbRows: MutableList<Pair<Int, String?>> = mutableListOf()
        createDuckdbConnection().use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT id, doc FROM ducklake_db.$fullTrinoTable ORDER BY id").use { rs ->
                    while (rs.next()) {
                        val doc = rs.getString(2)
                        duckdbRows.add(rs.getInt(1) to (if (rs.wasNull()) null else doc))
                    }
                }
            }
        }
        return duckdbRows
    }

    @Test
    fun trinoWritesJsonParquet() {
        // Parquet is the cross-engine format — assert DuckDB reads the JSON column back too.
        writeReadRoundTrip(parquetSession(), "json_parquet", "parquet", crossEngineDuckdbRead = true)
    }

    @Test
    fun duckdbWritesJsonTrinoReads() {
        val table = "json_duckdb_written"
        val fullTrinoTable = "test_schema.$table"
        val fullDuckdbTable = "ducklake_db.$fullTrinoTable"
        try {
            createDuckdbConnection().use { conn ->
                conn.createStatement().use { stmt ->
                    stmt.execute("CREATE TABLE $fullDuckdbTable (id INTEGER, doc JSON)")
                    stmt.execute("INSERT INTO $fullDuckdbTable VALUES (1, '$doc1'), (2, '$doc2'), (3, NULL)")
                }
            }

            assertColumnTypeIsJson(fullTrinoTable)

            val rows = computeActual("SELECT id, json_format(doc) FROM $fullTrinoTable ORDER BY id")
                    .materializedRows
            assertThat(rows.map { it.getField(0) as Int to it.getField(1) as String? })
                    .containsExactly(1 to doc1, 2 to doc2, 3 to null)

            // JSON is queryable as a native type: a JSON-path extraction over the column works.
            assertThat(computeScalar("SELECT json_extract_scalar(doc, '\$.s') FROM $fullTrinoTable WHERE id = 2") as String?)
                    .isEqualTo("hi")
        }
        finally {
            tryDropTable(fullTrinoTable)
        }
    }
}
