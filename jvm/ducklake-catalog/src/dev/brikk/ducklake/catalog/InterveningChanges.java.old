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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Aggregated view of catalog changes committed in some snapshot range,
 * mirroring upstream's {@code SnapshotChangeInformation}
 * (see {@code temp/pg_ducklake/third_party/ducklake/src/storage/ducklake_transaction_changes.cpp:131}).
 * Built by {@link #parse(String)} from one or more
 * {@code ducklake_snapshot_changes.changes_made} text values, or by
 * {@link #merge(InterveningChanges)} across a range of intervening snapshots.
 *
 * <p>The {@link ConflictMatrix} consumes one of these as the "other" side
 * when checking if my-changes conflict with what intervening transactions
 * committed.
 *
 * <p>Includes upstream's full {@code ChangeType} enum even where we don't
 * emit a particular kind, so a catalog written by DuckDB / pg_ducklake
 * (which may emit {@code compacted_table}, {@code merge_adjacent},
 * {@code rewrite_delete}, {@code inlined_insert}, {@code inlined_delete},
 * {@code flushed_inlined} / {@code inline_flush}, scalar/table macros) round-trips
 * cleanly through the matrix when our connector is the in-flight writer.
 */
final class InterveningChanges
{
    /**
     * {@code (schemaName, entryName) → "table"|"view"}. Keyed by schema name
     * because that's the form upstream's matrix uses for the
     * created-vs-created name-collision check; tables and views share the
     * namespace within a schema, so they live in one map.
     */
    final Map<String, Map<String, String>> createdTablesByName = new HashMap<>();
    final Set<String> createdSchemas = new HashSet<>();
    final Set<Long> droppedSchemas = new HashSet<>();
    final Set<Long> droppedTables = new HashSet<>();
    final Set<Long> droppedViews = new HashSet<>();
    final Set<Long> insertedTables = new HashSet<>();
    final Set<Long> tablesDeletedFrom = new HashSet<>();
    final Set<Long> alteredTables = new HashSet<>();
    final Set<Long> alteredViews = new HashSet<>();
    final Set<Long> tablesCompacted = new HashSet<>();
    final Set<Long> tablesMergeAdjacent = new HashSet<>();
    final Set<Long> tablesRewriteDelete = new HashSet<>();
    final Set<Long> tablesInsertedInlined = new HashSet<>();
    final Set<Long> tablesDeletedInlined = new HashSet<>();
    final Set<Long> tablesFlushedInlined = new HashSet<>();

    InterveningChanges() {}

    /**
     * Parse one {@code ducklake_snapshot_changes.changes_made} text value
     * into an {@link InterveningChanges}. Mirrors upstream
     * {@code SnapshotChangeInformation::ParseChangesMade}
     * (ducklake_transaction_changes.cpp:131).
     */
    static InterveningChanges parse(String changesMade)
    {
        InterveningChanges result = new InterveningChanges();
        if (changesMade == null || changesMade.isEmpty()) {
            return result;
        }
        Cursor cur = new Cursor(changesMade);
        while (cur.pos < cur.input.length()) {
            String kind = parseChangeKind(cur);
            if (cur.pos >= cur.input.length() || cur.input.charAt(cur.pos) != ':') {
                throw new IllegalArgumentException(
                        "Expected a colon after the change type at position " + cur.pos
                                + " in: " + changesMade);
            }
            cur.pos++;
            String value = parseChangeValue(cur);
            applyEntry(result, kind, value);
            if (cur.pos >= cur.input.length()) {
                break;
            }
            if (cur.input.charAt(cur.pos) != ',') {
                throw new IllegalArgumentException(
                        "Expected a comma separating change entries at position " + cur.pos
                                + " in: " + changesMade);
            }
            cur.pos++;
        }
        return result;
    }

    /**
     * Merge {@code other} into {@code this}; cumulative across multiple
     * intervening snapshots' rows.
     */
    void mergeFrom(InterveningChanges other)
    {
        for (var e : other.createdTablesByName.entrySet()) {
            createdTablesByName
                    .computeIfAbsent(e.getKey(), k -> new HashMap<>())
                    .putAll(e.getValue());
        }
        createdSchemas.addAll(other.createdSchemas);
        droppedSchemas.addAll(other.droppedSchemas);
        droppedTables.addAll(other.droppedTables);
        droppedViews.addAll(other.droppedViews);
        insertedTables.addAll(other.insertedTables);
        tablesDeletedFrom.addAll(other.tablesDeletedFrom);
        alteredTables.addAll(other.alteredTables);
        alteredViews.addAll(other.alteredViews);
        tablesCompacted.addAll(other.tablesCompacted);
        tablesMergeAdjacent.addAll(other.tablesMergeAdjacent);
        tablesRewriteDelete.addAll(other.tablesRewriteDelete);
        tablesInsertedInlined.addAll(other.tablesInsertedInlined);
        tablesDeletedInlined.addAll(other.tablesDeletedInlined);
        tablesFlushedInlined.addAll(other.tablesFlushedInlined);
    }

    private static void applyEntry(InterveningChanges result, String kind, String value)
    {
        switch (kind) {
            case "created_table" -> {
                ParsedCatalogEntry e = parseCatalogEntry(value);
                result.createdTablesByName
                        .computeIfAbsent(e.schema(), k -> new HashMap<>())
                        .put(e.name(), "table");
            }
            case "created_view" -> {
                ParsedCatalogEntry e = parseCatalogEntry(value);
                result.createdTablesByName
                        .computeIfAbsent(e.schema(), k -> new HashMap<>())
                        .put(e.name(), "view");
            }
            case "created_schema" -> result.createdSchemas.add(parseQuotedValueOnly(value));
            case "dropped_schema" -> result.droppedSchemas.add(parseUnsignedLong(value));
            case "dropped_table" -> result.droppedTables.add(parseUnsignedLong(value));
            case "dropped_view" -> result.droppedViews.add(parseUnsignedLong(value));
            case "inserted_into_table" -> result.insertedTables.add(parseUnsignedLong(value));
            case "deleted_from_table" -> result.tablesDeletedFrom.add(parseUnsignedLong(value));
            case "altered_table" -> result.alteredTables.add(parseUnsignedLong(value));
            case "altered_view" -> result.alteredViews.add(parseUnsignedLong(value));
            case "compacted_table" -> result.tablesCompacted.add(parseUnsignedLong(value));
            case "merge_adjacent" -> result.tablesMergeAdjacent.add(parseUnsignedLong(value));
            case "rewrite_delete" -> result.tablesRewriteDelete.add(parseUnsignedLong(value));
            case "inlined_insert" -> result.tablesInsertedInlined.add(parseUnsignedLong(value));
            case "inlined_delete" -> result.tablesDeletedInlined.add(parseUnsignedLong(value));
            case "flushed_inlined", "inline_flush" ->
                    result.tablesFlushedInlined.add(parseUnsignedLong(value));
            // Scalar / table macros — upstream supports them but we don't emit them and
            // none of our matrix entries reference them. Silently ignore so a catalog
            // that contains macro changes (e.g. written by DuckDB) still parses.
            case "created_scalar_macro", "created_table_macro",
                    "dropped_scalar_macro", "dropped_table_macro" -> { /* ignored */ }
            default -> throw new IllegalArgumentException(
                    "Unsupported change type \"" + kind + "\" in changes_made");
        }
    }

    private static String parseChangeKind(Cursor cur)
    {
        int start = cur.pos;
        while (cur.pos < cur.input.length() && cur.input.charAt(cur.pos) != ':') {
            cur.pos++;
        }
        return cur.input.substring(start, cur.pos);
    }

    /**
     * Scan to the next unquoted comma. Quoting is double-quote, escaped by
     * doubling. Mirrors upstream's {@code ParseChangeValue}
     * (ducklake_transaction_changes.cpp:88).
     */
    private static String parseChangeValue(Cursor cur)
    {
        int start = cur.pos;
        boolean inQuotes = false;
        while (cur.pos < cur.input.length()) {
            char c = cur.input.charAt(cur.pos);
            if (!inQuotes && c == ',') {
                break;
            }
            if (c == '"') {
                inQuotes = !inQuotes;
            }
            cur.pos++;
        }
        return cur.input.substring(start, cur.pos);
    }

    /**
     * Mirrors upstream's {@code DuckLakeUtil::ParseQuotedValue}
     * (common/ducklake_util.cpp:16): expects opening {@code "}, scans until
     * matching {@code "}, treats {@code ""} as an escaped literal {@code "}.
     */
    private static ParsedQuotedValue parseQuotedValue(String input, int startPos)
    {
        if (startPos >= input.length() || input.charAt(startPos) != '"') {
            throw new IllegalArgumentException(
                    "Failed to parse quoted value - expected opening quote at position " + startPos
                            + " in: " + input);
        }
        StringBuilder sb = new StringBuilder();
        int p = startPos + 1;
        while (p < input.length()) {
            char c = input.charAt(p);
            if (c == '"') {
                p++;
                if (p < input.length() && input.charAt(p) == '"') {
                    sb.append('"');
                    p++;
                    continue;
                }
                return new ParsedQuotedValue(sb.toString(), p);
            }
            sb.append(c);
            p++;
        }
        throw new IllegalArgumentException(
                "Failed to parse quoted value - unterminated quote in: " + input);
    }

    /**
     * Parses {@code "schema"."name"} mirroring upstream's
     * {@code DuckLakeUtil::ParseCatalogEntry} (common/ducklake_util.cpp:68).
     */
    private static ParsedCatalogEntry parseCatalogEntry(String value)
    {
        ParsedQuotedValue schema = parseQuotedValue(value, 0);
        if (schema.endPos() >= value.length() || value.charAt(schema.endPos()) != '.') {
            throw new IllegalArgumentException(
                    "Failed to parse catalog entry - expected a dot in: " + value);
        }
        ParsedQuotedValue name = parseQuotedValue(value, schema.endPos() + 1);
        if (name.endPos() < value.length()) {
            throw new IllegalArgumentException(
                    "Failed to parse catalog entry - trailing data after quoted value in: " + value);
        }
        return new ParsedCatalogEntry(schema.value(), name.value());
    }

    private static String parseQuotedValueOnly(String value)
    {
        ParsedQuotedValue parsed = parseQuotedValue(value, 0);
        if (parsed.endPos() < value.length()) {
            throw new IllegalArgumentException(
                    "Failed to parse single quoted value - trailing data in: " + value);
        }
        return parsed.value();
    }

    private static long parseUnsignedLong(String value)
    {
        try {
            long parsed = Long.parseLong(value);
            if (parsed < 0) {
                throw new IllegalArgumentException(
                        "Negative IDs are not valid in changes_made: " + value);
            }
            return parsed;
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Expected an unsigned integer change value, got: " + value, e);
        }
    }

    private static final class Cursor
    {
        final String input;
        int pos;

        Cursor(String input)
        {
            this.input = input;
            this.pos = 0;
        }
    }

    private record ParsedQuotedValue(String value, int endPos) {}

    private record ParsedCatalogEntry(String schema, String name) {}

    /**
     * Cross-snapshot helper: parse and merge each row's {@code changes_made}
     * into one {@link InterveningChanges}. Order doesn't matter — all merges
     * are set unions / map merges.
     */
    static InterveningChanges parseAll(List<String> changesMadeRows)
    {
        InterveningChanges combined = new InterveningChanges();
        for (String row : changesMadeRows) {
            combined.mergeFrom(parse(row));
        }
        return combined;
    }

    // Test helpers — let unit tests inspect the parsed shape without
    // exposing the mutable Maps/Sets directly.

    Set<String> createdNamesInSchema(String schemaName)
    {
        Map<String, String> nestedMap = createdTablesByName.get(schemaName);
        if (nestedMap == null) {
            return Set.of();
        }
        return new LinkedHashSet<>(nestedMap.keySet());
    }

    String createdEntryKind(String schemaName, String entryName)
    {
        Map<String, String> nestedMap = createdTablesByName.get(schemaName);
        return nestedMap == null ? null : nestedMap.get(entryName);
    }
}
