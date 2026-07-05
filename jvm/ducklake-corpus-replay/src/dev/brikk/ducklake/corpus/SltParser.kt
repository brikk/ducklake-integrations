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
        var i = start + 1
        val sql = StringBuilder()
        while (i < until && lines[i].trim().let { it.isNotEmpty() && it != "----" }) {
            if (sql.isNotEmpty()) sql.append('\n')
            sql.append(lines[i])
            i++
        }
        var expectedError: String? = null
        if (i < until && lines[i].trim() == "----") {
            i++
            val msg = StringBuilder()
            while (i < until && lines[i].trim().isNotEmpty()) {
                if (msg.isNotEmpty()) msg.append('\n')
                msg.append(lines[i].trim())
                i++
            }
            expectedError = msg.toString().ifEmpty { null }
        }
        // `statement maybe` = may or may not error; model as ok-with-tolerance via expectError+null? v1: treat
        // as unsupported only if it has no error expectation semantics we can honor. maybe == "error allowed".
        if (kind == "maybe") {
            out += SltStatement(lineNo, sql.toString(), expectError = true, expectedError = null, connection = connection)
        } else {
            out += SltStatement(lineNo, sql.toString(), kind == "error", expectedError, connection)
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
        var sortMode = SortMode.NOSORT
        var connection: String? = null
        var label: String? = null
        for (t in tokens.drop(2)) {
            when {
                t == "rowsort" -> sortMode = SortMode.ROWSORT
                t == "valuesort" -> sortMode = SortMode.VALUESORT
                t == "nosort" -> sortMode = SortMode.NOSORT
                t.matches(Regex("con\\d+")) -> connection = t
                else -> label = t // result-sharing label
            }
        }
        var i = start + 1
        val sql = StringBuilder()
        while (i < until && lines[i].trim().let { it.isNotEmpty() && it != "----" }) {
            if (sql.isNotEmpty()) sql.append('\n')
            sql.append(lines[i])
            i++
        }
        val expected = mutableListOf<String>()
        if (i < until && lines[i].trim() == "----") {
            i++
            while (i < until && lines[i].isNotBlank()) {
                expected += lines[i]
                i++
            }
        }
        out += SltQuery(lineNo, sql.toString(), types, sortMode, connection, label, expected)
        return i
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
