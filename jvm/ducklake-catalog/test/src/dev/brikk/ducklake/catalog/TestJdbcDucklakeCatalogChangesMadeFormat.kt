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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Exercises the `ducklake_snapshot_changes.changes_made` serializer:
 * [WriteChange.formatChangesMade] (joiner) plus the per-variant
 * `toChangesMadeEntry` renderers.
 *
 * Upstream DuckDB parses this column via `ParseChangesList` in
 * `third_party/ducklake/src/storage/ducklake_transaction_changes.cpp`.
 * `created_table` / `created_view` values are read by
 * `DuckLakeUtil::ParseCatalogEntry` which requires the fully qualified
 * `"schema"."name"` form; `created_schema` is a single quoted value
 * read by `DuckLakeUtil::ParseQuotedValue`. Both forms wrap the value in
 * `"..."` and escape embedded `"` by doubling (`""`).
 */
class TestJdbcDucklakeCatalogChangesMadeFormat {
    // ==================== formatChangesMade: plain joiner ====================

    @Test
    fun testEmptyListProducesEmptyString() {
        assertThat(WriteChange.formatChangesMade(listOf<WriteChange>())).isEmpty()
    }

    @Test
    fun testSingleEntryIsPassedThrough() {
        assertThat(WriteChange.formatChangesMade(listOf(WriteChange.DroppedTable(7))))
            .isEqualTo("dropped_table:7")
    }

    @Test
    fun testMultipleEntriesJoinedWithComma() {
        assertThat(
            WriteChange.formatChangesMade(
                listOf(
                    WriteChange.CreatedSchema("sales"),
                    WriteChange.CreatedTable(SCHEMA_ID, "sales", "orders"),
                    WriteChange.InsertedIntoTable(7, setOf<Long>()),
                ),
            ),
        )
            .isEqualTo("created_schema:\"sales\",created_table:\"sales\".\"orders\",inserted_into_table:7")
    }

    @Test
    fun testNumericIdEntriesStayUnquoted() {
        // Spec: `dropped_*`, `altered_*`, `inserted_into_table`, `deleted_from_table` use raw
        // integers (parsed by StringUtil::ToUnsigned upstream), never quoted.
        assertThat(
            WriteChange.formatChangesMade(
                listOf(
                    WriteChange.DroppedSchema(3, "sales"),
                    WriteChange.DroppedTable(12),
                    WriteChange.AlteredTable(42),
                    WriteChange.InsertedIntoTable(100, setOf<Long>()),
                    WriteChange.DeletedFromTable(100, setOf<Long>()),
                ),
            ),
        )
            .isEqualTo("dropped_schema:3,dropped_table:12,altered_table:42,inserted_into_table:100,deleted_from_table:100")
    }

    // ==================== CreatedTable: "schema"."name" ====================

    @Test
    fun testCreatedTableProducesFullyQualifiedQuotedForm() {
        assertThat(WriteChange.CreatedTable(SCHEMA_ID, "sales", "orders").toChangesMadeEntry())
            .isEqualTo("created_table:\"sales\".\"orders\"")
    }

    @Test
    fun testCreatedTableQuotesCommaInName() {
        // Without quoting, ParseChangesList would see three entries instead of one.
        assertThat(WriteChange.CreatedTable(SCHEMA_ID, "sales", "bad,name").toChangesMadeEntry())
            .isEqualTo("created_table:\"sales\".\"bad,name\"")
    }

    @Test
    fun testCreatedTableEscapesEmbeddedQuoteByDoubling() {
        assertThat(WriteChange.CreatedTable(SCHEMA_ID, "sales", "weird\"name").toChangesMadeEntry())
            .isEqualTo("created_table:\"sales\".\"weird\"\"name\"")
    }

    @Test
    fun testCreatedTableQuotesBothSchemaAndTable() {
        assertThat(WriteChange.CreatedTable(SCHEMA_ID, "odd.schema", "odd.table").toChangesMadeEntry())
            .isEqualTo("created_table:\"odd.schema\".\"odd.table\"")
    }

    // ==================== CreatedView: "schema"."name" ====================

    @Test
    fun testCreatedViewProducesFullyQualifiedQuotedForm() {
        assertThat(WriteChange.CreatedView(SCHEMA_ID, "analytics", "daily_summary").toChangesMadeEntry())
            .isEqualTo("created_view:\"analytics\".\"daily_summary\"")
    }

    @Test
    fun testCreatedViewEscapesEmbeddedQuoteByDoubling() {
        assertThat(WriteChange.CreatedView(SCHEMA_ID, "a\"b", "c\"d").toChangesMadeEntry())
            .isEqualTo("created_view:\"a\"\"b\".\"c\"\"d\"")
    }

    // ==================== CreatedSchema: single quoted value ====================

    @Test
    fun testCreatedSchemaWrapsNameInQuotes() {
        assertThat(WriteChange.CreatedSchema("sales").toChangesMadeEntry())
            .isEqualTo("created_schema:\"sales\"")
    }

    @Test
    fun testCreatedSchemaEscapesEmbeddedQuoteByDoubling() {
        assertThat(WriteChange.CreatedSchema("weird\"name").toChangesMadeEntry())
            .isEqualTo("created_schema:\"weird\"\"name\"")
    }

    @Test
    fun testCreatedSchemaQuotesCommaInName() {
        assertThat(WriteChange.CreatedSchema("bad,name").toChangesMadeEntry())
            .isEqualTo("created_schema:\"bad,name\"")
    }

    companion object {
        // schemaId is carried on CreatedTable / CreatedView for matrix cross-checks
        // but is NOT serialized to the changes_made text — these format tests don't
        // care which value we pass.
        private const val SCHEMA_ID = 1L
    }
}
