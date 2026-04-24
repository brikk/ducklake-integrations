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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link JdbcDucklakeCatalog#formatChangesMade(List)} — the serializer for the
 * {@code ducklake_snapshot_changes.changes_made} column.
 * <p>
 * Upstream DuckDB parses this column as a comma-separated list of {@code <kind>:<value>} entries.
 * Values that embed the delimiters (comma, double quote) in a schema/table/view name would
 * produce ambiguous output today — see {@code TODO-compatibility.md} and
 * {@code COMPARE-pg_ducklake.md} (B1). The tests below pin current behavior; the
 * {@code @Disabled} test describes what we want after the fix lands.
 */
public class TestJdbcDucklakeCatalogChangesMadeFormat
{
    @Test
    public void testEmptyListProducesEmptyString()
    {
        assertThat(JdbcDucklakeCatalog.formatChangesMade(List.of())).isEmpty();
    }

    @Test
    public void testSingleEntry()
    {
        assertThat(JdbcDucklakeCatalog.formatChangesMade(List.of("created_table:foo")))
                .isEqualTo("created_table:foo");
    }

    @Test
    public void testMultipleEntriesJoinedWithComma()
    {
        assertThat(JdbcDucklakeCatalog.formatChangesMade(List.of(
                "created_schema:sales",
                "created_table:orders",
                "inserted_into_table:7")))
                .isEqualTo("created_schema:sales,created_table:orders,inserted_into_table:7");
    }

    // ==================== Current behavior with pathological names (the timebomb) ====================

    @Test
    public void testCurrentBehaviorUnescapedCommaInTableName()
    {
        // Current behavior: the comma in the table name is emitted raw, which makes the output
        // indistinguishable from two separate changes ("created_table:bad" and "name"). A DuckDB
        // reader parsing this column with a comma-split will see phantom entries.
        //
        // This test documents the bug; it does not fail. When the fix lands (quoting with
        // upstream's `created_table:"..."` form), update this expectation and drop this test in
        // favor of {@link #testFixedBehaviorQuotesValuesWithCommas}.
        String out = JdbcDucklakeCatalog.formatChangesMade(List.of("created_table:bad,name"));
        assertThat(out).isEqualTo("created_table:bad,name");
    }

    @Test
    public void testCurrentBehaviorUnescapedQuoteInSchemaName()
    {
        // Same shape: a double quote in the name would break any reader that uses double quotes
        // as value delimiters (which upstream does — KeywordHelper::WriteQuoted).
        String out = JdbcDucklakeCatalog.formatChangesMade(List.of("created_schema:weird\"name"));
        assertThat(out).isEqualTo("created_schema:weird\"name");
    }

    // ==================== Target behavior after the fix (Disabled until fixed) ====================

    @Test
    @Disabled("TODO (TODO-compatibility.md B1): quote values with upstream's KeywordHelper::WriteQuoted form")
    public void testFixedBehaviorQuotesValuesWithCommas()
    {
        // After the fix: emit upstream's quoted form so round-tripping through DuckDB works
        // regardless of embedded commas / quotes in names.
        String out = JdbcDucklakeCatalog.formatChangesMade(List.of("created_table:bad,name"));
        assertThat(out).isEqualTo("created_table:\"bad,name\"");
    }

    @Test
    @Disabled("TODO (TODO-compatibility.md B1): escape embedded double quotes by doubling them")
    public void testFixedBehaviorEscapesEmbeddedQuotes()
    {
        String out = JdbcDucklakeCatalog.formatChangesMade(List.of("created_schema:weird\"name"));
        assertThat(out).isEqualTo("created_schema:\"weird\"\"name\"");
    }
}
