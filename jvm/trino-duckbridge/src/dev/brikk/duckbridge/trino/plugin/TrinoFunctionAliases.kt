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
package dev.brikk.duckbridge.trino.plugin

import java.sql.Connection
import java.sql.SQLException

/**
 * Loader + prober for the `trino_parity` DuckDB extension, which registers the
 * `trino_<name>(...)` scalar functions/macros and the `trino_meta()` table macro that the
 * connector's pushdown translator relies on.
 *
 * The extension is a hard dependency of parity pushdown: the plugin jar bundles a host-built
 * binary (see [TrinoParityExtensionResolver]), and operators can override with the
 * `duckbridge.parity-extension-path` catalog property. The connection must have been opened with
 * `allow_unsigned_extensions=true` (a Properties entry — immutable once the DB is running) for the
 * LOAD to succeed on a locally-built, unsigned binary.
 *
 * P3 note: this in-process LOAD assumes the extension binary lives on the same filesystem as the
 * DuckDB instance. A remote/server-side DuckDB (Quack) needs the LOAD forwarded server-side and
 * the binary present on the server — see [TrinoParityExtensionResolver]'s P3 note. That
 * server-side forwarder is intentionally NOT ported here (no Quack in this module yet).
 */
object TrinoFunctionAliases {
    /**
     * LOAD the trino_parity extension into an in-process JDBC DuckDB connection. Uses a scratch
     * [java.sql.Statement] so a failure propagates without closing the caller's main statement
     * (DuckDB JDBC auto-closes a Statement on any SQLException).
     */
    @Throws(SQLException::class)
    fun loadInProcess(connection: Connection, path: String) {
        connection.createStatement().use { scratch ->
            scratch.execute("LOAD '" + path.replace("'", "''") + "'")
        }
    }

    /**
     * Probe `trino_meta()` on a connection that has already LOADed the extension, returning the
     * row count. Throws [SQLException] if the table macro isn't resolvable (i.e. the extension
     * didn't actually register), so the caller can fail loud rather than silently pushing SQL the
     * remote DuckDB can't resolve.
     */
    @Throws(SQLException::class)
    fun probeMetaRowCount(connection: Connection): Int {
        connection.createStatement().use { scratch ->
            scratch.executeQuery("SELECT count(*) FROM trino_meta()").use { rs ->
                check(rs.next()) { "trino_meta() returned no rows" }
                return rs.getInt(1)
            }
        }
    }
}
