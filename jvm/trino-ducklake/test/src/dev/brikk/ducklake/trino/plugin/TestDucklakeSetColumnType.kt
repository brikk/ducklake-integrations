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
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * `ALTER TABLE … ALTER COLUMN … SET DATA TYPE` (widening type promotion). DuckLake permits only
 * widening changes; a file written under the old physical type must still read correctly under the
 * new type. Covers the write-side validation (widening accepted, narrowing / incompatible rejected)
 * and the read side across BOTH generations of data files (pre- and post-promotion) for the parquet
 * and DuckDB (`.db`) formats.
 */
@Execution(ExecutionMode.SAME_THREAD)
internal open class TestDucklakeSetColumnType : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "set-column-type"

    @Test
    fun integerWideningReadsAcrossFileGenerationsParquet() {
        assertIntegerWideningAcrossGenerations("parquet")
    }

    @Test
    fun integerWideningReadsAcrossFileGenerationsDuckDb() {
        assertIntegerWideningAcrossGenerations("duckdb")
    }

    private fun assertIntegerWideningAcrossGenerations(format: String) {
        val table = "test_schema.promote_int_$format"
        try {
            computeActual("CREATE TABLE $table (c INTEGER) WITH (data_file_format = '$format')")
            // Row written under the OLD physical type (INTEGER).
            computeActual("INSERT INTO $table VALUES (1), (2)")
            computeActual("ALTER TABLE $table ALTER COLUMN c SET DATA TYPE BIGINT")
            // Row written under the NEW type (BIGINT) — a second file generation.
            computeActual("INSERT INTO $table VALUES (3000000000)")

            assertThat(computeScalar("SELECT typeof(c) FROM $table LIMIT 1")).isEqualTo("bigint")
            val values = computeActual("SELECT c FROM $table ORDER BY c").materializedRows
                    .map { (it.getField(0) as Number).toLong() }
            assertThat(values).containsExactly(1L, 2L, 3_000_000_000L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun realToDoubleWideningReadsOldRows() {
        val table = "test_schema.promote_real"
        try {
            computeActual("CREATE TABLE $table (c REAL)")
            computeActual("INSERT INTO $table VALUES (REAL '1.5')")
            computeActual("ALTER TABLE $table ALTER COLUMN c SET DATA TYPE DOUBLE")

            assertThat(computeScalar("SELECT typeof(c) FROM $table LIMIT 1")).isEqualTo("double")
            assertThat((computeScalar("SELECT c FROM $table") as Number).toDouble()).isEqualTo(1.5)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun timestampToTimestampTzWideningReadsOldRows() {
        val table = "test_schema.promote_ts"
        try {
            computeActual("CREATE TABLE $table (c TIMESTAMP(6))")
            computeActual("INSERT INTO $table VALUES (TIMESTAMP '2025-01-15 12:30:45')")
            computeActual("ALTER TABLE $table ALTER COLUMN c SET DATA TYPE TIMESTAMP(6) WITH TIME ZONE")

            assertThat(computeScalar("SELECT typeof(c) FROM $table LIMIT 1"))
                    .isEqualTo("timestamp(6) with time zone")
            // The old (zoneless) row reads back at UTC — the connector's storage zone.
            assertThat(computeScalar("SELECT CAST(c AS VARCHAR) FROM $table").toString())
                    .startsWith("2025-01-15 12:30:45")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun sameTypeIsANoOp() {
        val table = "test_schema.promote_noop"
        try {
            computeActual("CREATE TABLE $table (c INTEGER)")
            computeActual("INSERT INTO $table VALUES (7)")
            computeActual("ALTER TABLE $table ALTER COLUMN c SET DATA TYPE INTEGER")
            assertThat((computeScalar("SELECT c FROM $table") as Number).toLong()).isEqualTo(7L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun narrowingIsRejected() {
        val table = "test_schema.promote_narrow"
        try {
            computeActual("CREATE TABLE $table (c BIGINT)")
            assertThatThrownBy {
                computeActual("ALTER TABLE $table ALTER COLUMN c SET DATA TYPE INTEGER")
            }.hasMessageContaining("widening")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun incompatiblePromotionIsRejected() {
        val table = "test_schema.promote_incompatible"
        try {
            computeActual("CREATE TABLE $table (c INTEGER)")
            assertThatThrownBy {
                computeActual("ALTER TABLE $table ALTER COLUMN c SET DATA TYPE VARCHAR")
            }.hasMessageContaining("widening")
        }
        finally {
            tryDropTable(table)
        }
    }
}
