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
package dev.brikk.ducklake.catalog;

import dev.brikk.ducklake.catalog.schema.tables.DucklakeColumnTable;
import dev.brikk.ducklake.catalog.schema.tables.DucklakeSchemaVersionsTable;
import org.junit.jupiter.api.Test;

import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_COLUMN;
import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_SCHEMA_VERSIONS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the documented spec-vs-reality differences between the published
 * DuckLake markdown spec and the columns DuckDB's ducklake extension
 * actually creates on first attach. Each method here corresponds to one
 * entry in {@code TODO-WRITE-MODE.md § Reality Check: Spec vs Actual
 * Catalog Shape}.
 *
 * <p>If a future DuckLake version changes the catalog DDL — drops one of
 * these columns, renames it, moves it to a different table, or adds new
 * required columns we don't currently mirror — the codegen
 * (in {@code build-logic/.../DucklakePostgresService.kt}) will produce
 * jOOQ classes whose static fields shift. These tests will then fail with
 * a clear "missing field" compile error or {@code NullPointerException},
 * making the breakage immediately obvious before any runtime corruption.
 *
 * <p>The reality of the connector working depends on these columns being
 * present; if upstream ever removes them, our writers / readers need
 * updating in lock-step.
 */
public class TestCatalogShapeRegression
{
    /**
     * Reality #1: {@code ducklake_schema_versions} has {@code table_id} in
     * practice (DuckLake's CREATE-IF-NOT-EXISTS path emits it; the
     * markdown spec describes only {@code (begin_snapshot, schema_version)}).
     * Upstream itself diverges — the duplicate {@code CREATE TABLE} at
     * {@code ducklake_metadata_manager.cpp:281} omits {@code table_id} for
     * a fallback path. Our writer always populates it, so the column must
     * be present in the catalog we generate against.
     */
    @Test
    public void schemaVersionsHasTableIdColumn()
    {
        DucklakeSchemaVersionsTable schver = DUCKLAKE_SCHEMA_VERSIONS;
        assertThat(schver.TABLE_ID)
                .as("ducklake_schema_versions.table_id must exist — JdbcDucklakeCatalog.attemptWriteTransaction "
                        + "writes it on every DDL commit and DuckDB reads it for per-table schema versioning")
                .isNotNull();
        assertThat(schver.TABLE_ID.getName()).isEqualTo("table_id");
        assertThat(schver.BEGIN_SNAPSHOT).isNotNull();
        assertThat(schver.SCHEMA_VERSION).isNotNull();
    }

    /**
     * Reality #2: {@code ducklake_column} has {@code default_value_type} in
     * practice. The markdown spec pre-1.0 didn't mention it. DuckDB writes
     * {@code 'literal'} for column defaults and we mirror that contract in
     * {@code JdbcDucklakeCatalog.insertColumnTree} / {@code renameColumn}.
     */
    @Test
    public void columnHasDefaultValueTypeColumn()
    {
        DucklakeColumnTable col = DUCKLAKE_COLUMN;
        assertThat(col.DEFAULT_VALUE_TYPE)
                .as("ducklake_column.default_value_type must exist — our writers populate it with 'literal'; "
                        + "if upstream renames or removes it, JdbcDucklakeCatalog.insertColumnTree breaks")
                .isNotNull();
        assertThat(col.DEFAULT_VALUE_TYPE.getName()).isEqualTo("default_value_type");
    }

    /**
     * Reality #3: {@code ducklake_column} has {@code default_value_dialect}
     * in practice. We currently leave it SQL NULL on writes (the "no real
     * default" sentinel — see {@code TestDucklakeCrossEngineCatalogMetadata.testDuckdbReadsTrinoTableWithNullDefaultValueDialect}
     * for the cross-engine pin). When we eventually surface user-defined
     * DEFAULTs, we'll write {@code 'trino'} here. Either way the column
     * must exist.
     */
    @Test
    public void columnHasDefaultValueDialectColumn()
    {
        DucklakeColumnTable col = DUCKLAKE_COLUMN;
        assertThat(col.DEFAULT_VALUE_DIALECT)
                .as("ducklake_column.default_value_dialect must exist — pinned by cross-engine NULL-dialect "
                        + "test, and the home for future 'trino' values when we ship user-defined DEFAULTs")
                .isNotNull();
        assertThat(col.DEFAULT_VALUE_DIALECT.getName()).isEqualTo("default_value_dialect");
    }

    /**
     * Bonus: confirm the spec'd {@code default_value} sentinel column also
     * exists. Our writers always write the four-character string {@code 'NULL'}
     * (DuckDB's "no default" sentinel) here. If this column is ever renamed
     * to e.g. {@code default_expr}, our INSERTs collapse.
     */
    @Test
    public void columnHasDefaultValueColumn()
    {
        DucklakeColumnTable col = DUCKLAKE_COLUMN;
        assertThat(col.DEFAULT_VALUE)
                .as("ducklake_column.default_value (the 'NULL'-string sentinel) must exist")
                .isNotNull();
        assertThat(col.DEFAULT_VALUE.getName()).isEqualTo("default_value");
    }
}
