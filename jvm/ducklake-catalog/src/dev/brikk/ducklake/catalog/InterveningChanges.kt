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

// Visibility note: Java was package-private `final class`. Kotlin has no
// package-private; widening to default `public` since `ConflictMatrix.check`
// (also default `public` in Kotlin) exposes this type in its signature.
// `internal` would fix the visibility but the parameter type is referenced
// from JdbcDucklakeCatalog (same module) where it's also called from Kotlin.

/**
 * Aggregated view of catalog changes committed in some snapshot range,
 * mirroring upstream's `SnapshotChangeInformation`
 * (see `temp/pg_ducklake/third_party/ducklake/src/storage/ducklake_transaction_changes.cpp:131`).
 * Built by [parse] from one or more
 * `ducklake_snapshot_changes.changes_made` text values, or by
 * [mergeFrom] across a range of intervening snapshots.
 *
 * The [ConflictMatrix] consumes one of these as the "other" side
 * when checking if my-changes conflict with what intervening transactions
 * committed.
 *
 * Includes upstream's full `ChangeType` enum even where we don't
 * emit a particular kind, so a catalog written by DuckDB / pg_ducklake
 * (which may emit `compacted_table`, `merge_adjacent`,
 * `rewrite_delete`, `inlined_insert`, `inlined_delete`,
 * `flushed_inlined` / `inline_flush`, scalar/table macros) round-trips
 * cleanly through the matrix when our connector is the in-flight writer.
 */
class InterveningChanges {
    /**
     * `(schemaName, entryName) → "table"|"view"`. Keyed by schema name
     * because that's the form upstream's matrix uses for the
     * created-vs-created name-collision check; tables and views share the
     * namespace within a schema, so they live in one map.
     */
    internal val createdTablesByName: MutableMap<String, MutableMap<String, String>> = HashMap()

    @JvmField
    internal val createdSchemas: MutableSet<String> = HashSet()

    @JvmField
    internal val droppedSchemas: MutableSet<Long> = HashSet()

    @JvmField
    internal val droppedTables: MutableSet<Long> = HashSet()

    @JvmField
    internal val droppedViews: MutableSet<Long> = HashSet()

    @JvmField
    internal val insertedTables: MutableSet<Long> = HashSet()

    @JvmField
    internal val tablesDeletedFrom: MutableSet<Long> = HashSet()

    @JvmField
    internal val alteredTables: MutableSet<Long> = HashSet()

    @JvmField
    internal val alteredViews: MutableSet<Long> = HashSet()

    @JvmField
    internal val tablesCompacted: MutableSet<Long> = HashSet()

    @JvmField
    internal val tablesMergeAdjacent: MutableSet<Long> = HashSet()

    @JvmField
    internal val tablesRewriteDelete: MutableSet<Long> = HashSet()

    @JvmField
    internal val tablesInsertedInlined: MutableSet<Long> = HashSet()

    @JvmField
    internal val tablesDeletedInlined: MutableSet<Long> = HashSet()

    @JvmField
    internal val tablesFlushedInlined: MutableSet<Long> = HashSet()

    /**
     * Merge `other` into `this`; cumulative across multiple
     * intervening snapshots' rows.
     */
    internal fun mergeFrom(other: InterveningChanges) {
        for (e in other.createdTablesByName.entries) {
            createdTablesByName
                .getOrPut(e.key) { HashMap() }
                .putAll(e.value)
        }
        createdSchemas.addAll(other.createdSchemas)
        droppedSchemas.addAll(other.droppedSchemas)
        droppedTables.addAll(other.droppedTables)
        droppedViews.addAll(other.droppedViews)
        insertedTables.addAll(other.insertedTables)
        tablesDeletedFrom.addAll(other.tablesDeletedFrom)
        alteredTables.addAll(other.alteredTables)
        alteredViews.addAll(other.alteredViews)
        tablesCompacted.addAll(other.tablesCompacted)
        tablesMergeAdjacent.addAll(other.tablesMergeAdjacent)
        tablesRewriteDelete.addAll(other.tablesRewriteDelete)
        tablesInsertedInlined.addAll(other.tablesInsertedInlined)
        tablesDeletedInlined.addAll(other.tablesDeletedInlined)
        tablesFlushedInlined.addAll(other.tablesFlushedInlined)
    }

    // Test helpers — let unit tests inspect the parsed shape without
    // exposing the mutable Maps/Sets directly.

    internal fun createdNamesInSchema(schemaName: String): Set<String> {
        val nestedMap = createdTablesByName[schemaName] ?: return emptySet()
        return nestedMap.keys.toList().toSet()
    }

    internal fun createdEntryKind(schemaName: String, entryName: String): String? {
        val nestedMap = createdTablesByName[schemaName] ?: return null
        return nestedMap[entryName]
    }

    private class Cursor(val input: String) {
        var pos: Int = 0
    }

    private data class ParsedQuotedValue(val value: String, val endPos: Int)

    private data class ParsedCatalogEntry(val schema: String, val name: String)

    companion object {
        /**
         * Parse one `ducklake_snapshot_changes.changes_made` text value
         * into an [InterveningChanges]. Mirrors upstream
         * `SnapshotChangeInformation::ParseChangesMade`
         * (ducklake_transaction_changes.cpp:131).
         */
        fun parse(changesMade: String?): InterveningChanges {
            val result = InterveningChanges()
            if (changesMade == null || changesMade.isEmpty()) {
                return result
            }
            val cur = Cursor(changesMade)
            while (cur.pos < cur.input.length) {
                val kind = parseChangeKind(cur)
                if (cur.pos >= cur.input.length || cur.input[cur.pos] != ':') {
                    throw IllegalArgumentException(
                        "Expected a colon after the change type at position ${cur.pos} in: $changesMade",
                    )
                }
                cur.pos++
                val value = parseChangeValue(cur)
                applyEntry(result, kind, value)
                if (cur.pos >= cur.input.length) {
                    break
                }
                if (cur.input[cur.pos] != ',') {
                    throw IllegalArgumentException(
                        "Expected a comma separating change entries at position ${cur.pos} in: $changesMade",
                    )
                }
                cur.pos++
            }
            return result
        }

        /**
         * Cross-snapshot helper: parse and merge each row's `changes_made`
         * into one [InterveningChanges]. Order doesn't matter — all merges
         * are set unions / map merges.
         */
        fun parseAll(changesMadeRows: List<String>): InterveningChanges {
            val combined = InterveningChanges()
            for (row in changesMadeRows) {
                combined.mergeFrom(parse(row))
            }
            return combined
        }

        private fun applyEntry(result: InterveningChanges, kind: String, value: String) {
            when (kind) {
                "created_table" -> {
                    val e = parseCatalogEntry(value)
                    result.createdTablesByName
                        .getOrPut(e.schema) { HashMap() }
                        .put(e.name, "table")
                }
                "created_view" -> {
                    val e = parseCatalogEntry(value)
                    result.createdTablesByName
                        .getOrPut(e.schema) { HashMap() }
                        .put(e.name, "view")
                }
                "created_schema" -> result.createdSchemas.add(parseQuotedValueOnly(value))
                "dropped_schema" -> result.droppedSchemas.add(parseUnsignedLong(value))
                "dropped_table" -> result.droppedTables.add(parseUnsignedLong(value))
                "dropped_view" -> result.droppedViews.add(parseUnsignedLong(value))
                "inserted_into_table" -> result.insertedTables.add(parseUnsignedLong(value))
                "deleted_from_table" -> result.tablesDeletedFrom.add(parseUnsignedLong(value))
                "altered_table" -> result.alteredTables.add(parseUnsignedLong(value))
                "altered_view" -> result.alteredViews.add(parseUnsignedLong(value))
                "compacted_table" -> result.tablesCompacted.add(parseUnsignedLong(value))
                "merge_adjacent" -> result.tablesMergeAdjacent.add(parseUnsignedLong(value))
                "rewrite_delete" -> result.tablesRewriteDelete.add(parseUnsignedLong(value))
                "inlined_insert" -> result.tablesInsertedInlined.add(parseUnsignedLong(value))
                "inlined_delete" -> result.tablesDeletedInlined.add(parseUnsignedLong(value))
                "flushed_inlined", "inline_flush" ->
                    result.tablesFlushedInlined.add(parseUnsignedLong(value))
                // Scalar / table macros — upstream supports them but we don't emit them and
                // none of our matrix entries reference them. Silently ignore so a catalog
                // that contains macro changes (e.g. written by DuckDB) still parses.
                "created_scalar_macro", "created_table_macro",
                "dropped_scalar_macro", "dropped_table_macro" -> { /* ignored */ }
                else -> throw IllegalArgumentException(
                    "Unsupported change type \"$kind\" in changes_made",
                )
            }
        }

        private fun parseChangeKind(cur: Cursor): String {
            val start = cur.pos
            while (cur.pos < cur.input.length && cur.input[cur.pos] != ':') {
                cur.pos++
            }
            return cur.input.substring(start, cur.pos)
        }

        /**
         * Scan to the next unquoted comma. Quoting is double-quote, escaped by
         * doubling. Mirrors upstream's `ParseChangeValue`
         * (ducklake_transaction_changes.cpp:88).
         */
        private fun parseChangeValue(cur: Cursor): String {
            val start = cur.pos
            var inQuotes = false
            while (cur.pos < cur.input.length) {
                val c = cur.input[cur.pos]
                if (!inQuotes && c == ',') {
                    break
                }
                if (c == '"') {
                    inQuotes = !inQuotes
                }
                cur.pos++
            }
            return cur.input.substring(start, cur.pos)
        }

        /**
         * Mirrors upstream's `DuckLakeUtil::ParseQuotedValue`
         * (common/ducklake_util.cpp:16): expects opening `"`, scans until
         * matching `"`, treats `""` as an escaped literal `"`.
         */
        private fun parseQuotedValue(input: String, startPos: Int): ParsedQuotedValue {
            if (startPos >= input.length || input[startPos] != '"') {
                throw IllegalArgumentException(
                    "Failed to parse quoted value - expected opening quote at position $startPos in: $input",
                )
            }
            val sb = StringBuilder()
            var p = startPos + 1
            while (p < input.length) {
                val c = input[p]
                if (c == '"') {
                    p++
                    if (p < input.length && input[p] == '"') {
                        sb.append('"')
                        p++
                        continue
                    }
                    return ParsedQuotedValue(sb.toString(), p)
                }
                sb.append(c)
                p++
            }
            throw IllegalArgumentException(
                "Failed to parse quoted value - unterminated quote in: $input",
            )
        }

        /**
         * Parses `"schema"."name"` mirroring upstream's
         * `DuckLakeUtil::ParseCatalogEntry` (common/ducklake_util.cpp:68).
         */
        private fun parseCatalogEntry(value: String): ParsedCatalogEntry {
            val schema = parseQuotedValue(value, 0)
            if (schema.endPos >= value.length || value[schema.endPos] != '.') {
                throw IllegalArgumentException(
                    "Failed to parse catalog entry - expected a dot in: $value",
                )
            }
            val name = parseQuotedValue(value, schema.endPos + 1)
            if (name.endPos < value.length) {
                throw IllegalArgumentException(
                    "Failed to parse catalog entry - trailing data after quoted value in: $value",
                )
            }
            return ParsedCatalogEntry(schema.value, name.value)
        }

        private fun parseQuotedValueOnly(value: String): String {
            val parsed = parseQuotedValue(value, 0)
            if (parsed.endPos < value.length) {
                throw IllegalArgumentException(
                    "Failed to parse single quoted value - trailing data in: $value",
                )
            }
            return parsed.value
        }

        private fun parseUnsignedLong(value: String): Long {
            try {
                val parsed = value.toLong()
                if (parsed < 0) {
                    throw IllegalArgumentException(
                        "Negative IDs are not valid in changes_made: $value",
                    )
                }
                return parsed
            } catch (e: NumberFormatException) {
                throw IllegalArgumentException(
                    "Expected an unsigned integer change value, got: $value", e,
                )
            }
        }
    }
}
