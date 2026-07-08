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
 * `ALTER TABLE … ALTER COLUMN <struct>.<field> SET DATA TYPE …` (nested struct-FIELD widening type
 * promotion). The nested generalization of [TestDucklakeSetColumnType]: DuckLake permits only
 * widening changes, so a struct field written under the old physical type must still read correctly
 * under the new type. Covers the write-side validation (widening accepted, narrowing / incompatible
 * rejected) and the read side across BOTH generations of data files (pre- and post-promotion) for
 * the parquet and DuckDB (`.db`) formats.
 *
 * There is NO corpus test for nested field type change, so these hand-written tests are the only
 * safety net for the feature.
 */
@Execution(ExecutionMode.SAME_THREAD)
internal open class TestDucklakeSetFieldType : AbstractDucklakeIntegrationTest() {
    override fun isolatedCatalogName(): String = "set-field-type"

    @Test
    fun nestedFieldWideningReadsAcrossFileGenerationsParquet() {
        assertNestedFieldWideningAcrossGenerations("parquet")
    }

    @Test
    fun nestedFieldWideningReadsAcrossFileGenerationsDuckDb() {
        assertNestedFieldWideningAcrossGenerations("duckdb")
    }

    private fun assertNestedFieldWideningAcrossGenerations(format: String) {
        val table = "test_schema.promote_field_$format"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, s ROW(a INTEGER)) WITH (data_file_format = '$format')")
            // Row written under the OLD physical type (INTEGER).
            computeActual("INSERT INTO $table VALUES (1, CAST(ROW(1) AS ROW(a INTEGER)))")
            computeActual("ALTER TABLE $table ALTER COLUMN s.a SET DATA TYPE BIGINT")
            // Row written under the NEW type (BIGINT) — a second file generation.
            computeActual("INSERT INTO $table VALUES (2, CAST(ROW(3000000000) AS ROW(a BIGINT)))")

            assertThat(computeScalar("SELECT typeof(s) FROM $table LIMIT 1")).isEqualTo("row(\"a\" bigint)")
            val values = computeActual("SELECT id, s.a FROM $table ORDER BY id").materializedRows
                    .map { (it.getField(1) as Number).toLong() }
            assertThat(values).containsExactly(1L, 3_000_000_000L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun deeplyNestedFieldWideningReadsOldRowsDuckDb() {
        val table = "test_schema.promote_field_deep"
        try {
            computeActual(
                    "CREATE TABLE $table (id INTEGER, s ROW(m ROW(a INTEGER))) WITH (data_file_format = 'duckdb')")
            computeActual("INSERT INTO $table VALUES (1, CAST(ROW(ROW(1)) AS ROW(m ROW(a INTEGER))))")
            computeActual("ALTER TABLE $table ALTER COLUMN s.m.a SET DATA TYPE BIGINT")
            computeActual(
                    "INSERT INTO $table VALUES (2, CAST(ROW(ROW(3000000000)) AS ROW(m ROW(a BIGINT))))")

            assertThat(computeScalar("SELECT typeof(s) FROM $table LIMIT 1"))
                    .isEqualTo("row(\"m\" row(\"a\" bigint))")
            val values = computeActual("SELECT id, s.m.a FROM $table ORDER BY id").materializedRows
                    .map { (it.getField(1) as Number).toLong() }
            assertThat(values).containsExactly(1L, 3_000_000_000L)
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun narrowingIsRejected() {
        val table = "test_schema.promote_field_narrow"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, s ROW(a BIGINT))")
            assertThatThrownBy {
                computeActual("ALTER TABLE $table ALTER COLUMN s.a SET DATA TYPE INTEGER")
            }.hasMessageContaining("widening")
        }
        finally {
            tryDropTable(table)
        }
    }

    @Test
    fun incompatiblePromotionIsRejected() {
        val table = "test_schema.promote_field_incompatible"
        try {
            computeActual("CREATE TABLE $table (id INTEGER, s ROW(a INTEGER))")
            assertThatThrownBy {
                computeActual("ALTER TABLE $table ALTER COLUMN s.a SET DATA TYPE VARCHAR")
            }.hasMessageContaining("widening")
        }
        finally {
            tryDropTable(table)
        }
    }
}
