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
package dev.brikk.ducklake.catalog

import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_COLUMN
import dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SCHEMA_VERSIONS
import dev.brikk.ducklake.catalog.schema.tables.DucklakeColumnTable
import dev.brikk.ducklake.catalog.schema.tables.DucklakeSchemaVersionsTable
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Pins the documented spec-vs-reality differences between the published
 * DuckLake markdown spec and the columns DuckDB's ducklake extension
 * actually creates on first attach. Each method here corresponds to one
 * entry in `TODO-WRITE-MODE.md § Reality Check: Spec vs Actual Catalog Shape`.
 *
 * If a future DuckLake version changes the catalog DDL — drops one of
 * these columns, renames it, moves it to a different table, or adds new
 * required columns we don't currently mirror — the codegen
 * (in `build-logic/.../DucklakePostgresService.kt`) will produce
 * jOOQ classes whose static fields shift. These tests will then fail with
 * a clear "missing field" compile error or `NullPointerException`,
 * making the breakage immediately obvious before any runtime corruption.
 *
 * The reality of the connector working depends on these columns being
 * present; if upstream ever removes them, our writers / readers need
 * updating in lock-step.
 */
class TestCatalogShapeRegression {
    /**
     * Reality #1: `ducklake_schema_versions` has `table_id` in
     * practice (DuckLake's CREATE-IF-NOT-EXISTS path emits it; the
     * markdown spec describes only `(begin_snapshot, schema_version)`).
     * Upstream itself diverges — the duplicate `CREATE TABLE` at
     * `ducklake_metadata_manager.cpp:281` omits `table_id` for
     * a fallback path. Our writer always populates it, so the column must
     * be present in the catalog we generate against.
     */
    @Test
    fun schemaVersionsHasTableIdColumn() {
        val schver: DucklakeSchemaVersionsTable = DUCKLAKE_SCHEMA_VERSIONS
        assertThat(schver.TABLE_ID)
            .`as`(
                "ducklake_schema_versions.table_id must exist — JdbcDucklakeCatalog.attemptWriteTransaction " +
                    "writes it on every DDL commit and DuckDB reads it for per-table schema versioning",
            )
            .isNotNull()
        assertThat(schver.TABLE_ID.name).isEqualTo("table_id")
        assertThat(schver.BEGIN_SNAPSHOT).isNotNull()
        assertThat(schver.SCHEMA_VERSION).isNotNull()
    }

    /**
     * Reality #2: `ducklake_column` has `default_value_type` in
     * practice. The markdown spec pre-1.0 didn't mention it. DuckDB writes
     * `'literal'` for column defaults and we mirror that contract in
     * `JdbcDucklakeCatalog.insertColumnTree` / `renameColumn`.
     */
    @Test
    fun columnHasDefaultValueTypeColumn() {
        val col: DucklakeColumnTable = DUCKLAKE_COLUMN
        assertThat(col.DEFAULT_VALUE_TYPE)
            .`as`(
                "ducklake_column.default_value_type must exist — our writers populate it with 'literal'; " +
                    "if upstream renames or removes it, JdbcDucklakeCatalog.insertColumnTree breaks",
            )
            .isNotNull()
        assertThat(col.DEFAULT_VALUE_TYPE.name).isEqualTo("default_value_type")
    }

    /**
     * Reality #3: `ducklake_column` has `default_value_dialect`
     * in practice. We currently leave it SQL NULL on writes (the "no real
     * default" sentinel — see `TestDucklakeCrossEngineCatalogMetadata.testDuckdbReadsTrinoTableWithNullDefaultValueDialect`
     * for the cross-engine pin). When we eventually surface user-defined
     * DEFAULTs, we'll write `'trino'` here. Either way the column
     * must exist.
     */
    @Test
    fun columnHasDefaultValueDialectColumn() {
        val col: DucklakeColumnTable = DUCKLAKE_COLUMN
        assertThat(col.DEFAULT_VALUE_DIALECT)
            .`as`(
                "ducklake_column.default_value_dialect must exist — pinned by cross-engine NULL-dialect " +
                    "test, and the home for future 'trino' values when we ship user-defined DEFAULTs",
            )
            .isNotNull()
        assertThat(col.DEFAULT_VALUE_DIALECT.name).isEqualTo("default_value_dialect")
    }

    /**
     * Bonus: confirm the spec'd `default_value` sentinel column also
     * exists. Our writers always write the four-character string `'NULL'`
     * (DuckDB's "no default" sentinel) here. If this column is ever renamed
     * to e.g. `default_expr`, our INSERTs collapse.
     */
    @Test
    fun columnHasDefaultValueColumn() {
        val col: DucklakeColumnTable = DUCKLAKE_COLUMN
        assertThat(col.DEFAULT_VALUE)
            .`as`("ducklake_column.default_value (the 'NULL'-string sentinel) must exist")
            .isNotNull()
        assertThat(col.DEFAULT_VALUE.name).isEqualTo("default_value")
    }
}
