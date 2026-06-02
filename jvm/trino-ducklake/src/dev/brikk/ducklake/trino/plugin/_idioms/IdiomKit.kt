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
package dev.brikk.ducklake.trino.plugin._idioms

import java.sql.SQLException
import java.util.Locale

/**
 * Idiom kit extracted from the trino-main faithful Kotlin port. These helpers
 * exist to make recurring Java-isms (locale-naive case folding, NPE-prone SQL
 * error messages, etc.) self-documenting at the call site without making every
 * caller restate the rationale.
 *
 * Kit policy (per .ai/kotlin-port/PLAN.md):
 *   - Locale-aware lowercase for catalog identifiers, type-name normalisation,
 *     SQL dialect comparisons → use [lowercaseRoot] / [Locale.ROOT].
 *   - SQLException.getMessage() can be null → use [safeMessage] / [firstLine]
 *     so warn/log paths never NPE.
 *   - Use Kotlin `.use {}` on every AutoCloseable. Manual paired try/close
 *     blocks are reserved for compound resources where one close depends on
 *     state set up by another (see DuckDbFileWriter.init / extractColumnStats).
 *
 * See `_idioms/kit-notes.json` next to this file for the catalog of decisions
 * (handle-class style, Java-interop visibility, Optional<T> vs T?) that were
 * intentionally *not* applied in this slice and the reasoning.
 */
internal object IdiomKit {

    /** Locale-stable lowercase. Use anywhere a string is keyed/compared/normalised. */
    fun String.lowercaseRoot(): String = lowercase(Locale.ROOT)

    /**
     * Null-safe access to [SQLException.getMessage]. DuckDB's JDBC driver can
     * return null here on a handful of internal error paths; callers that log
     * `e.message!!` will NPE under load. Use this instead.
     */
    fun safeMessage(e: Throwable, fallback: String = "(no message)"): String =
            e.message ?: fallback

    /**
     * First newline-delimited line of a possibly-multiline string. DuckDB SQL
     * errors are frequently long stack-trace-like blocks; we typically only
     * want the first line in a log.warn so it stays readable.
     */
    fun firstLine(message: String): String =
            message.lineSequence().firstOrNull() ?: message

    /** Convenience: null-safe + first-line, as one call. */
    fun firstLineOf(e: Throwable, fallback: String = "(no message)"): String =
            firstLine(safeMessage(e, fallback))
}
