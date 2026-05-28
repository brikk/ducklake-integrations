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
package dev.brikk.ducklake.trino.plugin;

import com.google.common.io.Resources;
import io.airlift.log.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Loader for the {@code trino-function-aliases.sql} classpath resource, which
 * defines {@code trino_<name>(...)} macros and a {@code trino_meta()} table
 * macro on whatever DuckDB instance the executors attach to.
 *
 * <p>The macros are the interpretation layer between the Trino pushdown
 * translator and DuckDB: the translator only ever emits {@code trino_*} calls,
 * and this file decides how each one is implemented in DuckDB. Future semantic
 * fixes (NULL handling, collation, edge cases) happen here, not in plugin code.
 *
 * <p>Statements are split on {@code ;} from the resource and exposed via
 * {@link #statements()}. {@code INSTALL} / {@code LOAD} statements are flagged
 * best-effort via {@link #isBestEffort(String)} so a sandboxed environment
 * without network access still gets working macros — the round 1 macros do not
 * yet depend on the ICU extension.
 */
final class TrinoFunctionAliases
{
    private static final Logger log = Logger.get(TrinoFunctionAliases.class);

    private static final String RESOURCE = "trino-function-aliases.sql";

    private static final List<String> STATEMENTS = loadStatements();

    /**
     * Names harvested from {@code -- @placeholder ...} lines in the SQL resource.
     * These macros are installed (so direct use in test / dev sessions works for
     * ASCII input) but the translator must NOT push them — the Trino-equivalent
     * implementation is pending a native DuckDB extension. Logged once per JVM.
     */
    private static final List<String> PLACEHOLDER_MACROS = loadPlaceholderMacros();

    private static final AtomicBoolean PLACEHOLDER_WARNED = new AtomicBoolean(false);

    private TrinoFunctionAliases() {}

    /**
     * Ordered list of SQL statements to apply on every DuckDB attach. Each
     * element is a single statement with no trailing semicolon. Apply in order.
     */
    static List<String> statements()
    {
        return STATEMENTS;
    }

    /**
     * Returns true for statements that may legitimately fail in a sandboxed
     * environment (currently {@code INSTALL} / {@code LOAD} for extensions
     * that need to be downloaded). Callers should log and continue rather than
     * abort the attach if such a statement fails.
     */
    static boolean isBestEffort(String statement)
    {
        String head = statement.stripLeading().toUpperCase(Locale.ROOT);
        return head.startsWith("INSTALL ") || head.startsWith("LOAD ");
    }

    /**
     * Apply all statements directly to {@code stmt}. Best-effort statements log
     * a warning on failure and continue; other failures propagate.
     */
    static void applyDirect(Statement stmt) throws SQLException
    {
        for (String sql : STATEMENTS) {
            try {
                stmt.execute(sql);
            }
            catch (SQLException e) {
                if (isBestEffort(sql)) {
                    log.warn("trino-function-aliases best-effort statement failed: %s — %s",
                            firstLine(sql), e.getMessage());
                    continue;
                }
                throw e;
            }
        }
        warnPlaceholdersOnce();
    }

    /** Names of macros flagged with {@code -- @placeholder} in the SQL resource. */
    static List<String> placeholderMacros()
    {
        return PLACEHOLDER_MACROS;
    }

    private static void warnPlaceholdersOnce()
    {
        if (PLACEHOLDER_MACROS.isEmpty()) {
            return;
        }
        if (PLACEHOLDER_WARNED.compareAndSet(false, true)) {
            log.warn("trino-function-aliases includes %d placeholder macro(s): %s. "
                            + "These do not match Trino's semantics on non-ASCII input and are "
                            + "intentionally excluded from the pushdown catalog (trino_meta + "
                            + "DuckDbExpressionTranslator.PUSHABLE_FUNCTIONS). They are installed "
                            + "for ASCII-safe testing only; native-extension replacement is the "
                            + "real fix. See dev-docs/TODO-pushdown-duckdb.md.",
                    PLACEHOLDER_MACROS.size(), String.join(", ", PLACEHOLDER_MACROS));
        }
    }

    private static List<String> loadStatements()
    {
        return splitStatements(readResource());
    }

    private static List<String> loadPlaceholderMacros()
    {
        // Scan lines of the form: -- @placeholder name1 name2 ...
        // Names are the un-prefixed macro names (e.g. `trino_lower`); whitespace-separated.
        List<String> out = new ArrayList<>();
        for (String line : readResource().split("\\R")) {
            String trimmed = line.stripLeading();
            if (!trimmed.startsWith("-- @placeholder")) {
                continue;
            }
            String tail = trimmed.substring("-- @placeholder".length()).strip();
            if (tail.isEmpty()) {
                continue;
            }
            for (String name : tail.split("\\s+")) {
                if (!name.isEmpty()) {
                    out.add(name);
                }
            }
        }
        return List.copyOf(out);
    }

    private static String readResource()
    {
        try {
            return Resources.toString(
                    Resources.getResource(TrinoFunctionAliases.class, RESOURCE),
                    StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new IllegalStateException("Failed to read classpath resource " + RESOURCE, e);
        }
    }

    /**
     * Strip {@code --} line comments, then split on {@code ;}. The curated SQL
     * file does not use {@code ;} inside string literals or block comments, so
     * a character-level splitter is unnecessary.
     */
    private static List<String> splitStatements(String body)
    {
        List<String> out = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (String rawLine : body.split("\\R")) {
            String line = rawLine;
            int comment = line.indexOf("--");
            if (comment >= 0) {
                line = line.substring(0, comment);
            }
            current.append(line).append('\n');
            int semi;
            while ((semi = current.indexOf(";")) >= 0) {
                String stmt = current.substring(0, semi).trim();
                if (!stmt.isEmpty()) {
                    out.add(stmt);
                }
                current.delete(0, semi + 1);
            }
        }
        String tail = current.toString().trim();
        if (!tail.isEmpty()) {
            out.add(tail);
        }
        return List.copyOf(out);
    }

    private static String firstLine(String sql)
    {
        int nl = sql.indexOf('\n');
        return nl < 0 ? sql : sql.substring(0, nl);
    }
}
