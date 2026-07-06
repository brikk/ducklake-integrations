package dev.brikk.ducklake.corpus

/**
 * Line-based parser for the DuckDB sqllogictest dialect (see [SltRecord] for
 * the modeled subset). Parsing never throws on unknown constructs — they
 * surface as [SltUnsupported] so the driver can skip with a reason and the
 * corpus report can count the frontier.
 */
object SltParser {

    private val KNOWN_UNSUPPORTED =
        setOf("concurrentloop", "restart", "sleep", "mode", "load", "hash-threshold", "set", "reconnect", "unzip")

    private val CONNECTION_TOKEN = Regex("con\\d+")

    fun parse(path: String, content: String): SltFile {
        val lines = content.lines()
        val records = mutableListOf<SltRecord>()
        parseInto(lines, 0, lines.size, records, path)
        return SltFile(path, records)
    }

    /** Parses [from, until) into [out]; returns index after the region. */
    @Suppress("CyclomaticComplexMethod", "LongMethod")
    private fun parseInto(lines: List<String>, from: Int, until: Int, out: MutableList<SltRecord>, path: String): Int {
        var i = from
        while (i < until) {
            val raw = lines[i]
            val line = raw.trim()
            if (line.isEmpty() || line.startsWith("#")) {
                i++
                continue
            }
            val lineNo = i + 1
            val tokens = line.split(Regex("\\s+"))
            when (tokens[0]) {
                "require" -> {
                    out += SltRequire(lineNo, tokens.drop(1).joinToString(" "))
                    i++
                }
                "require-env" -> {
                    out += SltUnsupported(lineNo, "require-env", line)
                    i++
                }
                "test-env" -> {
                    val name = tokens.getOrNull(1) ?: ""
                    val value = tokens.drop(2).joinToString(" ")
                    out += SltTestEnv(lineNo, name, value)
                    i++
                }
                "statement" -> i = parseStatement(lines, i, until, tokens, out)
                "query" -> i = parseQuery(lines, i, until, tokens, out)
                "loop", "foreach" -> i = parseLoop(lines, i, until, tokens, out, path)
                "endloop" -> return i // handled by the caller (parseLoop)
                "skipif", "onlyif" -> {
                    val engine = tokens.getOrNull(1) ?: ""
                    val inner = mutableListOf<SltRecord>()
                    i = parseInto(lines, i + 1, minOf(i + 1 + guardSpan(lines, i + 1, until), until), inner, path)
                    val guarded = inner.firstOrNull()
                    if (guarded == null) {
                        out += SltUnsupported(lineNo, tokens[0], line)
                    } else {
                        out += SltConditional(lineNo, tokens[0] == "skipif", engine, guarded)
                        inner.drop(1).forEach { out += it }
                    }
                }
                in KNOWN_UNSUPPORTED -> {
                    out += SltUnsupported(lineNo, tokens[0], line)
                    i++
                }
                else -> {
                    out += SltUnsupported(lineNo, tokens[0], line)
                    i++
                }
            }
        }
        return i
    }

    /** A conditional guards exactly the next record: span until the blank line after it. */
    private fun guardSpan(lines: List<String>, from: Int, until: Int): Int {
        var i = from
        var seenContent = false
        while (i < until) {
            val t = lines[i].trim()
            if (t.isEmpty()) {
                if (seenContent) return i - from + 1
            } else if (!t.startsWith("#")) {
                seenContent = true
            }
            i++
        }
        return until - from
    }

    private fun parseStatement(
        lines: List<String>,
        start: Int,
        until: Int,
        tokens: List<String>,
        out: MutableList<SltRecord>,
    ): Int {
        val lineNo = start + 1
        val kind = tokens.getOrNull(1)
        if (kind != "ok" && kind != "error" && kind != "maybe") {
            out += SltUnsupported(lineNo, "statement ${kind ?: ""}", lines[start].trim())
            return start + 1
        }
        val connection = tokens.getOrNull(2)
        val (sql, afterSql) = readSqlBlock(lines, start + 1, until)
        val (block, i) = readResultBlock(lines, afterSql, until, trimLines = true)
        val expectedError: String? = block.joinToString("\n").ifEmpty { null }
        // `statement maybe` = may or may not error: expectError with a null
        // expectation means "an error, if any, is acceptable".
        if (kind == "maybe") {
            out += SltStatement(lineNo, sql, expectError = true, expectedError = null, connection = connection)
        } else {
            out += SltStatement(lineNo, sql, kind == "error", expectedError, connection)
        }
        return i
    }

    private fun parseQuery(
        lines: List<String>,
        start: Int,
        until: Int,
        tokens: List<String>,
        out: MutableList<SltRecord>,
    ): Int {
        val lineNo = start + 1
        val types = tokens.getOrNull(1) ?: ""
        val modifiers = parseQueryModifiers(tokens.drop(2))
        val (sql, afterSql) = readSqlBlock(lines, start + 1, until)
        val (expected, i) = readResultBlock(lines, afterSql, until, trimLines = false)
        out += SltQuery(lineNo, sql, types, modifiers.sortMode, modifiers.connection, modifiers.label, expected)
        return i
    }

    private data class QueryModifiers(val sortMode: SortMode, val connection: String?, val label: String?)

    private fun parseQueryModifiers(tokens: List<String>): QueryModifiers {
        var sortMode = SortMode.NOSORT
        var connection: String? = null
        var label: String? = null
        for (t in tokens) {
            when {
                t == "rowsort" -> sortMode = SortMode.ROWSORT
                t == "valuesort" -> sortMode = SortMode.VALUESORT
                t == "nosort" -> sortMode = SortMode.NOSORT
                t.matches(CONNECTION_TOKEN) -> connection = t
                else -> label = t // result-sharing label
            }
        }
        return QueryModifiers(sortMode, connection, label)
    }

    /** Multi-line SQL body, ending at a blank line or the `----` separator. */
    private fun readSqlBlock(lines: List<String>, from: Int, until: Int): Pair<String, Int> {
        var i = from
        val sql = StringBuilder()
        while (i < until && lines[i].trim().let { it.isNotEmpty() && it != "----" }) {
            if (sql.isNotEmpty()) sql.append('\n')
            sql.append(lines[i])
            i++
        }
        return sql.toString() to i
    }

    /** Optional `----`-introduced block, consumed until the blank terminator. */
    private fun readResultBlock(
        lines: List<String>,
        from: Int,
        until: Int,
        trimLines: Boolean,
    ): Pair<List<String>, Int> {
        var i = from
        if (i >= until || lines[i].trim() != "----") {
            return emptyList<String>() to i
        }
        i++
        val block = mutableListOf<String>()
        while (i < until && lines[i].isNotBlank()) {
            block += if (trimLines) lines[i].trim() else lines[i]
            i++
        }
        return block to i
    }

    private fun parseLoop(
        lines: List<String>,
        start: Int,
        until: Int,
        tokens: List<String>,
        out: MutableList<SltRecord>,
        path: String,
    ): Int {
        val lineNo = start + 1
        val variable = tokens.getOrNull(1) ?: ""
        val values: List<String> =
            if (tokens[0] == "loop") {
                val lo = tokens.getOrNull(2)?.toLongOrNull()
                val hi = tokens.getOrNull(3)?.toLongOrNull()
                if (lo == null || hi == null) {
                    out += SltUnsupported(lineNo, "loop", lines[start].trim())
                    return start + 1
                }
                (lo until hi).map { it.toString() }
            } else {
                tokens.drop(2)
            }
        val body = mutableListOf<SltRecord>()
        val after = parseInto(lines, start + 1, until, body, path)
        // parseInto returns at the `endloop` line (or region end)
        val end = if (after < until && lines[after].trim() == "endloop") after + 1 else after
        out += SltLoop(lineNo, variable, values, body)
        return end
    }
}
