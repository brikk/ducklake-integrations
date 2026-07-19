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
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * Write-guard slice of cross-engine compatibility: cases where a Trino write would otherwise
 * silently corrupt a DuckLake column that DuckDB owns the type of. Today that's the unsigned
 * integer family — DuckLake stores u8/u16/u32/u64 but Trino has no native unsigned types, so
 * the connector widens on read to the next-larger signed type, and the write path needs an
 * explicit range check ([DucklakeUnsignedRangeChecker]) to convert silent corruption
 * into a loud `NUMERIC_VALUE_OUT_OF_RANGE` at INSERT time.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeCrossEngineWriteGuards : AbstractDucklakeCrossEngineTest() {
    override fun isolatedCatalogName(): String {
        return "cross-engine-write-guards"
    }

    /**
     * DuckLake stores unsigned integer types (uint8/16/32/64) but Trino has no native
     * unsigned types, so the connector widens on read to the next-larger signed type
     * (uint8 → SMALLINT, etc.). The round-trip gap is the write path: without a range
     * check, SMALLINT 300 inserted into a DuckDB-created `uint8` column would be
     * encoded as INT32 300 in Parquet, and DuckDB's uint8 cast on read would silently
     * truncate to 44 (300 &amp; 0xFF). The checker in [DucklakeUnsignedRangeChecker]
     * converts that silent corruption into a loud `NUMERIC_VALUE_OUT_OF_RANGE`
     * error at INSERT time. This test walks every unsigned type and covers both the
     * "valid in-range value round-trips" and "out-of-range value is rejected" paths.
     */
    @Test
    @Throws(Exception::class)
    fun testTrinoRejectsOutOfRangeInsertsIntoUnsignedColumns() {
        val tableName = "xengine_unsigned_reject"
        val fullDuckdb = "ducklake_db.test_schema.$tableName"
        val fullTrino = "test_schema.$tableName"
        try {
            // DuckDB owns CREATE TABLE here because Trino has no way to declare unsigned
            // types (DucklakeTypeConverter.toDucklakeType only emits signed forms).
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.execute("DROP TABLE IF EXISTS $fullDuckdb")
                    stmt.execute("CREATE TABLE $fullDuckdb" +
                            " (id INTEGER, u8 UTINYINT, u16 USMALLINT, u32 UINTEGER, u64 UBIGINT)")
                }
            }

            // In-range values from every unsigned bound must round-trip cleanly.
            computeActual("INSERT INTO $fullTrino VALUES (" +
                    "1, SMALLINT '255', INTEGER '65535', BIGINT '4294967295', DECIMAL '18446744073709551615')")

            // Pin the assumption that the range check covers every Trino write path:
            // today Trino has no inlined write path — every INSERT/CTAS/MERGE-insert
            // flows through DucklakePageSink → ParquetWriter, which is where the check
            // lives. If someone later adds a catalog-backed inlined-write shortcut
            // without extending the checker, this assertion flips to a loud failure
            // here instead of silently letting uint8 values wrap through the
            // ducklake_inlined_data_* path.
            assertRowsWrittenToParquet(tableName)

            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.executeQuery(
                            "SELECT id, u8, u16, u32, CAST(u64 AS VARCHAR) FROM $fullDuckdb ORDER BY id").use { rs ->
                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getInt("id")).isEqualTo(1)
                        assertThat(rs.getInt("u8")).isEqualTo(255)
                        assertThat(rs.getInt("u16")).isEqualTo(65535)
                        assertThat(rs.getLong("u32")).isEqualTo(4_294_967_295L)
                        assertThat(rs.getString(5)).isEqualTo("18446744073709551615")
                        assertThat(rs.next()).isFalse()
                    }
                }
            }

            // 300 is the canonical symptom: it survives SMALLINT encoding unharmed, but the
            // low 8 bits are 44 — so pre-fix, DuckDB would silently read back 44.
            assertInsertFailsWithOverflow(
                    "INSERT INTO $fullTrino VALUES (2, SMALLINT '300', INTEGER '1', BIGINT '1', DECIMAL '1')",
                    "uint8")
            // Also reject signed overflow: uint8 of -1 would wrap to 255, also corruption.
            assertInsertFailsWithOverflow(
                    "INSERT INTO $fullTrino VALUES (3, SMALLINT '-1', INTEGER '1', BIGINT '1', DECIMAL '1')",
                    "uint8")

            // One check per unsigned type so a regression pins down which size broke.
            assertInsertFailsWithOverflow(
                    "INSERT INTO $fullTrino VALUES (4, SMALLINT '0', INTEGER '65536', BIGINT '1', DECIMAL '1')",
                    "uint16")
            assertInsertFailsWithOverflow(
                    "INSERT INTO $fullTrino VALUES (5, SMALLINT '0', INTEGER '0', BIGINT '4294967296', DECIMAL '1')",
                    "uint32")
            assertInsertFailsWithOverflow(
                    "INSERT INTO $fullTrino VALUES (6, SMALLINT '0', INTEGER '0', BIGINT '0', DECIMAL '18446744073709551616')",
                    "uint64")

            // The rejected rows must NOT have reached DuckDB — only row id=1 exists.
            createDuckdbConnection().use { duck ->
                duck.createStatement().use { stmt ->
                    stmt.executeQuery("SELECT count(*) FROM $fullDuckdb").use { rs ->
                        assertThat(rs.next()).isTrue()
                        assertThat(rs.getInt(1))
                                .`as`("rejected INSERTs must not leak partial rows into the catalog")
                                .isEqualTo(1)
                    }
                }
            }
        }
        finally {
            tryDropTable(fullTrino)
        }
    }

    private fun assertInsertFailsWithOverflow(insertSql: String, expectedDucklakeType: String) {
        try {
            computeActual(insertSql)
            throw AssertionError("Expected INSERT to fail for $expectedDucklakeType overflow: $insertSql")
        }
        catch (e: RuntimeException) {
            // Trino wraps validation TrinoExceptions in QueryFailedException; search the
            // entire cause chain for our error text rather than pinning on a specific
            // wrapper class that may shift across Trino versions.
            var cursor: Throwable? = e
            var combined = ""
            while (cursor != null) {
                combined += " || " + cursor.javaClass.simpleName + ": " + cursor.message
                cursor = cursor.cause
            }
            assertThat(combined)
                    .`as`("INSERT should fail with %s overflow error: %s", expectedDucklakeType, insertSql)
                    .contains("out of range for DuckLake $expectedDucklakeType")
        }
    }
}
