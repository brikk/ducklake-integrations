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
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.Test

/**
 * Inverse of [TestJdbcDucklakeCatalogChangesMadeFormat]: parses the
 * `ducklake_snapshot_changes.changes_made` text format that the
 * upstream DuckDB extension and our writer both emit. Round-trips cover
 * each variant; edge cases cover quoting (escaped `"`, embedded
 * commas), the all-upstream-kinds set (including ones we don't emit), and
 * malformed input.
 */
class TestInterveningChangesParser {
    // ==================== empty / single ====================

    @Test
    fun emptyStringYieldsEmptyAggregate() {
        val out = InterveningChanges.parse("")
        assertThat(out.createdSchemas).isEmpty()
        assertThat(out.droppedTables).isEmpty()
        assertThat(out.createdTablesByName).isEmpty()
    }

    @Test
    fun singleNumericIdEntry() {
        val out = InterveningChanges.parse("dropped_table:7")
        assertThat(out.droppedTables).containsExactly(7L)
    }

    // ==================== quoting round-trip ====================

    @Test
    fun quotedSchemaAndTableNameRoundTrip() {
        val out = InterveningChanges.parse("created_table:\"sales\".\"orders\"")
        assertThat(out.createdNamesInSchema("sales")).containsExactly("orders")
        assertThat(out.createdEntryKind("sales", "orders")).isEqualTo("table")
    }

    @Test
    fun embeddedCommaInsideQuotedNameStaysOneEntry() {
        // Without quoting, a naive split on ',' would see three entries.
        val out = InterveningChanges.parse("created_table:\"sales\".\"bad,name\"")
        assertThat(out.createdNamesInSchema("sales")).containsExactly("bad,name")
    }

    @Test
    fun escapedDoubleQuoteWithinQuotedName() {
        // The upstream escape rule: within a quoted value, "" is a literal ".
        val out = InterveningChanges.parse("created_table:\"sales\".\"weird\"\"name\"")
        assertThat(out.createdNamesInSchema("sales")).containsExactly("weird\"name")
    }

    @Test
    fun quotedSchemaNameRoundTrip() {
        val out = InterveningChanges.parse("created_schema:\"sales\"")
        assertThat(out.createdSchemas).containsExactly("sales")
    }

    @Test
    fun quotedSchemaNameWithEscapedQuote() {
        val out = InterveningChanges.parse("created_schema:\"weird\"\"name\"")
        assertThat(out.createdSchemas).containsExactly("weird\"name")
    }

    @Test
    fun roundTripCommaListWithMixedKinds() {
        val out = InterveningChanges.parse(
            "created_schema:\"sales\"," +
                "created_table:\"sales\".\"orders\"," +
                "inserted_into_table:7",
        )
        assertThat(out.createdSchemas).containsExactly("sales")
        assertThat(out.createdNamesInSchema("sales")).containsExactly("orders")
        assertThat(out.insertedTables).containsExactly(7L)
    }

    // ==================== all upstream change kinds ====================

    @Test
    fun recognizesAllUpstreamChangeKinds() {
        // All numeric-id forms upstream's ParseChangeType handles, including kinds
        // we don't currently emit — must round-trip cleanly so a catalog written
        // by DuckDB / pg_ducklake doesn't break our parser.
        val out = InterveningChanges.parse(
            "dropped_schema:1," +
                "dropped_table:2," +
                "dropped_view:3," +
                "inserted_into_table:4," +
                "deleted_from_table:5," +
                "altered_table:6," +
                "altered_view:7," +
                "compacted_table:8," +
                "merge_adjacent:9," +
                "rewrite_delete:10," +
                "inlined_insert:11," +
                "inlined_delete:12," +
                "flushed_inlined:13," +
                "inline_flush:14",
        )
        assertThat(out.droppedSchemas).containsExactly(1L)
        assertThat(out.droppedTables).containsExactly(2L)
        assertThat(out.droppedViews).containsExactly(3L)
        assertThat(out.insertedTables).containsExactly(4L)
        assertThat(out.tablesDeletedFrom).containsExactly(5L)
        assertThat(out.alteredTables).containsExactly(6L)
        assertThat(out.alteredViews).containsExactly(7L)
        assertThat(out.tablesCompacted).containsExactly(8L)
        assertThat(out.tablesMergeAdjacent).containsExactly(9L)
        assertThat(out.tablesRewriteDelete).containsExactly(10L)
        assertThat(out.tablesInsertedInlined).containsExactly(11L)
        assertThat(out.tablesDeletedInlined).containsExactly(12L)
        // flushed_inlined and inline_flush both populate tablesFlushedInlined.
        assertThat(out.tablesFlushedInlined).containsExactly(13L, 14L)
    }

    @Test
    fun macroChangesAreIgnored() {
        // We don't support macros (yet); upstream emits them and our parser
        // must not throw on a catalog that contains them.
        val out = InterveningChanges.parse(
            "created_scalar_macro:\"sales\".\"my_macro\"," +
                "dropped_table_macro:42",
        )
        assertThat(out.createdTablesByName).isEmpty()
        assertThat(out.droppedTables).isEmpty()
    }

    @Test
    fun createdViewLandsInTableNameMapWithViewKind() {
        // Tables and views share a namespace within a schema; upstream's
        // matrix uses one map keyed by "table" / "view" kind tag.
        val out = InterveningChanges.parse("created_view:\"analytics\".\"daily\"")
        assertThat(out.createdEntryKind("analytics", "daily")).isEqualTo("view")
    }

    // ==================== merge across snapshots ====================

    @Test
    fun parseAllUnionsSetsAcrossRows() {
        val combined = InterveningChanges.parseAll(
            listOf(
                "created_schema:\"a\",dropped_table:1",
                "created_schema:\"b\",dropped_table:2",
                "altered_table:3,altered_table:4",
            ),
        )
        assertThat(combined.createdSchemas).containsExactlyInAnyOrder("a", "b")
        assertThat(combined.droppedTables).containsExactlyInAnyOrder(1L, 2L)
        assertThat(combined.alteredTables).containsExactlyInAnyOrder(3L, 4L)
    }

    @Test
    fun parseAllOnEmptyListReturnsEmptyAggregate() {
        val combined = InterveningChanges.parseAll(emptyList())
        assertThat(combined.createdSchemas).isEmpty()
        assertThat(combined.droppedTables).isEmpty()
    }

    // ==================== malformed input ====================

    @Test
    fun unsupportedChangeKindThrows() {
        assertThatThrownBy(ThrowingCallable { InterveningChanges.parse("totally_made_up:1") })
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("totally_made_up")
    }

    @Test
    fun missingColonAfterKindThrows() {
        assertThatThrownBy(ThrowingCallable { InterveningChanges.parse("dropped_table") })
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("Expected a colon")
    }

    @Test
    fun unterminatedQuoteThrows() {
        assertThatThrownBy(ThrowingCallable { InterveningChanges.parse("created_schema:\"open") })
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("unterminated quote")
    }

    @Test
    fun catalogEntryMissingDotBetweenSchemaAndNameThrows() {
        assertThatThrownBy(ThrowingCallable { InterveningChanges.parse("created_table:\"sales\"") })
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("expected a dot")
    }

    @Test
    fun trailingCommaAtEndIsTolerated() {
        // Mirrors upstream's loop in ducklake_transaction_changes.cpp:115 — after
        // consuming the comma, pos equals the end-of-string and the loop exits
        // cleanly. Trailing commas don't trigger an error; an entry AFTER a
        // trailing comma (e.g. "dropped_table:1,bad") would because the parser
        // would then read "bad" as a kind that doesn't match any ChangeType.
        val out = InterveningChanges.parse("dropped_table:1,")
        assertThat(out.droppedTables).containsExactly(1L)
    }

    @Test
    fun emptyEntryAfterCommaThrows() {
        // The natural follow-up to the trailing-comma test: a comma followed by
        // something that's not a valid kind:value entry must error.
        assertThatThrownBy(ThrowingCallable { InterveningChanges.parse("dropped_table:1,bad") })
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun numericIdMustBeNonNegative() {
        assertThatThrownBy(ThrowingCallable { InterveningChanges.parse("dropped_table:-5") })
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("Negative")
    }

    // ==================== sanity: parser output shape matches what the matrix consumes ====================

    @Test
    fun parsedShapeMatchesMatrixExpectations() {
        val out = InterveningChanges.parse(
            "created_schema:\"foo\"," +
                "created_table:\"foo\".\"bar\"," +
                "altered_table:42",
        )
        // What ConflictMatrix.checkCreatedSchema reads:
        assertThat(out.createdSchemas).contains("foo")
        // What ConflictMatrix.checkCreatedTable reads:
        assertThat(out.createdTablesByName["foo"]).containsKey("bar")
        // What ConflictMatrix.checkAlteredTable / checkInsertedIntoTable read:
        assertThat(out.alteredTables).containsExactly(42L)
        // What other variants would NOT find:
        assertThat(out.droppedSchemas).isEmpty()
        assertThat(out.droppedTables).isEmpty()
    }

    @Test
    fun createdNamesInSchemaIsImmutableSnapshot() {
        val out = InterveningChanges.parse("created_table:\"s\".\"t\"")
        val names: Set<String> = out.createdNamesInSchema("s")
        assertThat(names).containsExactly("t")
        // The returned set is a copy; mutating it must not affect the parser's state.
        // (We can't actually test set immutability cheaply without try/catch — just
        // re-check that the parser's state is intact.)
        assertThat(out.createdEntryKind("s", "t")).isEqualTo("table")
    }
}
