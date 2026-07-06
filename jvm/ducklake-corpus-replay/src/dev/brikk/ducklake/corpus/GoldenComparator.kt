package dev.brikk.ducklake.corpus

import org.duckdb.DuckDBArray
import org.duckdb.DuckDBStruct
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset

/**
 * Converts JDBC results to sqllogictest golden form and compares against a
 * query record's expected block.
 *
 * Golden-form rules (DuckDB's sqllogictest conventions):
 *  - SQL NULL renders as `NULL`
 *  - empty string renders as `(empty)`
 *  - booleans render `true`/`false`
 *  - everything else uses DuckDB's own string rendering (JDBC `getString`)
 *  - expected block: one row per line, columns tab-separated; a legacy
 *    value-per-line layout (rows*cols single-column lines) is regrouped
 *  - `rowsort` sorts rows lexicographically on both sides; `valuesort` sorts
 *    all cells individually
 *  - an expected block of exactly `N values hashing to <md5>` is a hash
 *    result we don't support → surfaces as [Comparison.Unsupported]
 */
object GoldenComparator {

    sealed interface Comparison {
        data object Match : Comparison

        data class Mismatch(val detail: String) : Comparison

        data class Unsupported(val reason: String) : Comparison
    }

    private val HASHING = Regex("^\\d+ values hashing to [0-9a-f]+$")

    fun readRows(rs: ResultSet): List<List<String?>> {
        val cols = rs.metaData.columnCount
        val rows = mutableListOf<List<String?>>()
        while (rs.next()) {
            rows +=
                (1..cols).map { i ->
                    try {
                        renderCell(rs.getObject(i))
                    } catch (_: Exception) {
                        // e.g. the JDBC driver cannot represent TIME '24:00:00'
                        // as java.time.LocalTime; keep the record comparable.
                        "<jdbc-unreadable>"
                    }
                }
        }
        return rows
    }

    /**
     * Renders a JDBC/engine value the way DuckDB's own test runner renders it
     * (the golden-text dialect), instead of Java's default toString forms.
     * Public so [ReplayReadEngine] adapters produce comparable cells without
     * re-deriving the rules.
     */
    fun renderCell(value: Any?): String? =
        when (value) {
            null -> null
            // DuckDB golden text escapes embedded NUL bytes.
            is String -> value.replace("\u0000", "\\0")
            is Timestamp -> renderDateTime(value.toLocalDateTime())
            is LocalDateTime -> renderDateTime(value)
            is OffsetDateTime ->
                renderDateTime(value.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime()) + "+00"
            is java.time.ZonedDateTime ->
                renderDateTime(value.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime()) + "+00"
            is LocalTime -> renderTime(value)
            is LocalDate -> value.toString()
            is Double -> renderFloating(value)
            is Float -> renderFloating(value.toDouble())
            is List<*> -> value.joinToString(", ", prefix = "[", postfix = "]") { renderNested(it) }
            is DuckDBStruct ->
                value.map.entries.joinToString(", ", prefix = "{", postfix = "}") { (k, v) ->
                    "'$k': ${renderNested(v)}"
                }
            is DuckDBArray -> renderArray(value)
            is Map<*, *> ->
                value.entries.joinToString(", ", prefix = "{", postfix = "}") { (k, v) ->
                    "${renderNested(k)}=${renderNested(v)}"
                }
            is java.sql.Blob -> renderBlob(value.getBytes(1, value.length().toInt()))
            else -> value.toString()
        }

    /** DuckDB blob rendering: printable ASCII as-is, backslash escaped, else \xHH. */
    private fun renderBlob(bytes: ByteArray): String =
        buildString {
            for (b in bytes) {
                val i = b.toInt() and 0xFF
                when {
                    i == '\\'.code -> append("\\\\")
                    i in 0x20..0x7E -> append(i.toChar())
                    else -> append("\\x%02X".format(i))
                }
            }
        }

    private fun renderArray(value: DuckDBArray): String =
        (value.array as Array<*>).joinToString(", ", prefix = "[", postfix = "]") { renderNested(it) }

    /**
     * Values nested in LIST/STRUCT/MAP: DuckDB single-quotes a string only
     * when it needs it (empty, NULL-lookalike, or special characters);
     * simple strings render bare (e.g. `[main, hello]`, `{'k': FIX}`).
     */
    private fun renderNested(value: Any?): String =
        when (value) {
            is String -> {
                val s = value.replace("\u0000", "\\0")
                if (needsQuotes(s)) "'${s.replace("'", "''")}'" else s
            }
            else -> renderCell(value) ?: "NULL"
        }

    private val SPECIAL = "'\"{}[](),=: \t\n".toCharArray().toSet()

    private fun needsQuotes(s: String): Boolean =
        s.isEmpty() || s.equals("NULL", ignoreCase = true) || s.any { it in SPECIAL }

    private fun renderDateTime(dt: LocalDateTime): String {
        val base = "%04d-%02d-%02d %02d:%02d:%02d".format(dt.year, dt.monthValue, dt.dayOfMonth, dt.hour, dt.minute, dt.second)
        return base + fraction(dt.nano)
    }

    private fun renderTime(t: LocalTime): String =
        "%02d:%02d:%02d".format(t.hour, t.minute, t.second) + fraction(t.nano)

    /** DuckDB renders special floats as inf/-inf/nan, not Java's Infinity/NaN. */
    private fun renderFloating(d: Double): String =
        when {
            d.isNaN() -> "nan"
            d == Double.POSITIVE_INFINITY -> "inf"
            d == Double.NEGATIVE_INFINITY -> "-inf"
            else -> d.toString()
        }

    /** Microsecond fraction, trailing zeros trimmed, omitted when zero (DuckDB style). */
    private fun fraction(nano: Int): String {
        if (nano == 0) return ""
        val micros = "%06d".format(nano / 1000).trimEnd('0')
        return if (micros.isEmpty()) "" else ".$micros"
    }

    fun toGoldenCell(value: String?): String =
        when {
            value == null -> "NULL"
            value.isEmpty() -> "(empty)"
            else -> value
        }

    fun compare(query: SltQuery, actual: List<List<String?>>): Comparison {
        if (query.expected.size == 1 && HASHING.matches(query.expected[0].trim())) {
            return Comparison.Unsupported("hash-format expected result")
        }
        val colCount = query.types.length
        val actualRows = actual.map { row -> row.map(::toGoldenCell) }

        var expectedRows = query.expected.map { it.split('\t').map(String::trim) }
        // Legacy value-per-line layout: regroup when tabs are absent and counts line up.
        if (colCount > 1 &&
            expectedRows.all { it.size == 1 } &&
            query.expected.size == actualRows.size * colCount
        ) {
            expectedRows = query.expected.map(String::trim).chunked(colCount)
        }

        val a = sortRows(actualRows, query.sortMode)
        val e = sortRows(expectedRows.map { it.map(String::trim) }, query.sortMode)

        if (a.size != e.size) {
            return Comparison.Mismatch("row count: expected ${e.size}, got ${a.size}\n${diffPreview(e, a)}")
        }
        for (i in a.indices) {
            if (!rowsEqual(e[i], a[i], query.types)) {
                return Comparison.Mismatch("row ${i + 1}: expected ${e[i]}, got ${a[i]}")
            }
        }
        return Comparison.Match
    }

    private fun sortRows(rows: List<List<String>>, mode: SortMode): List<List<String>> =
        when (mode) {
            SortMode.NOSORT -> rows
            SortMode.ROWSORT -> rows.sortedBy { it.joinToString("\u0001") }
            SortMode.VALUESORT -> {
                val flat = rows.flatten().sorted()
                if (rows.isEmpty()) rows else flat.chunked(rows[0].size)
            }
        }

    private fun rowsEqual(expected: List<String>, actual: List<String>, types: String): Boolean {
        if (expected.size != actual.size) return false
        for (i in expected.indices) {
            val t = types.getOrNull(i) ?: 'T'
            if (!cellEqual(expected[i], actual[i], t)) return false
        }
        return true
    }

    private const val REGEX_PREFIX = "<REGEX>:"
    private const val NOT_REGEX_PREFIX = "<!REGEX>:"

    private fun cellEqual(expected: String, actual: String, type: Char): Boolean {
        if (expected == actual) return true
        // Upstream cell-level regex escape hatches (analyzed_plan assertions etc.).
        if (expected.startsWith(REGEX_PREFIX)) {
            return Regex(expected.removePrefix(REGEX_PREFIX), RegexOption.DOT_MATCHES_ALL).matches(actual)
        }
        if (expected.startsWith(NOT_REGEX_PREFIX)) {
            return !Regex(expected.removePrefix(NOT_REGEX_PREFIX), RegexOption.DOT_MATCHES_ALL).matches(actual)
        }
        // Classic sqllogictest: booleans under an I column render as 1/0; and
        // golden files are inconsistent about True/true capitalization.
        if (actual == "true" || actual == "false") {
            if (expected.equals(actual, ignoreCase = true)) return true
            if (type == 'I') return expected == (if (actual == "true") "1" else "0")
        }
        // Numeric tolerance: golden files write integers where engines may emit
        // decimal forms (and vice versa); R columns compare as doubles.
        if (type == 'R' || type == 'I') {
            val de = expected.toDoubleOrNull()
            val da = actual.toDoubleOrNull()
            if (de != null && da != null) {
                return de == da || (de != 0.0 && Math.abs(de - da) / Math.abs(de) < 1e-9)
            }
        }
        return false
    }

    private fun diffPreview(expected: List<List<String>>, actual: List<List<String>>): String {
        val e = expected.take(5).joinToString("\n") { it.joinToString("\t") }
        val a = actual.take(5).joinToString("\n") { it.joinToString("\t") }
        return "expected (head):\n$e\nactual (head):\n$a"
    }
}
