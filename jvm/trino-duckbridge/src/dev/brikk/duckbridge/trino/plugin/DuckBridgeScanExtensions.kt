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

import com.google.inject.Inject
import io.airlift.log.Logger
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import java.sql.Connection
import java.sql.SQLException

/**
 * Loads the DuckDB `lance` / `vortex` extensions that back the scan + search PTFs, when enabled.
 *
 * "Probed, never managed" — the extension is INSTALLed (floating latest, matching the DuckLake
 * connector's stance; a hard version pin isn't possible because extensions.duckdb.org serves only
 * the latest build per (DuckDB version, platform)) + LOADed on each connection. If the extension
 * can't be installed/loaded, PTF use fails loud with install instructions rather than silently
 * returning empty.
 *
 * On a remote (Quack) server the extension is a server-side concern: the connector still issues
 * INSTALL/LOAD over the pass-through connection, but if the server lacks network/repo access the
 * failure surfaces here with a server-side hint. There is no bundled binary for these (unlike
 * trino_parity) — they come from DuckDB's extension repositories.
 */
class DuckBridgeScanExtensions
    @Inject
    constructor(private val config: DuckBridgeConfig) {
        /** INSTALL + LOAD the enabled scan extensions on [connection]. Idempotent; no-op when both off. */
        fun ensureLoaded(connection: Connection) {
            if (config.isLanceEnabled) {
                install(connection, LANCE)
            }
            if (config.isVortexEnabled) {
                install(connection, VORTEX)
            }
        }

        /**
         * Fail loud if [extension] isn't enabled, so a PTF whose extension is off gives a clear error
         * at analyze time rather than a cryptic "function not found" from DuckDB.
         */
        fun requireEnabled(extension: String) {
            val enabled =
                when (extension) {
                    LANCE -> config.isLanceEnabled
                    VORTEX -> config.isVortexEnabled
                    else -> false
                }
            if (!enabled) {
                throw TrinoException(
                    NOT_SUPPORTED,
                    "The DuckDB '$extension' extension is not enabled. Set duckbridge.$extension.enabled=true " +
                        "on the catalog to use its scan/search table functions.",
                )
            }
        }

        private fun install(connection: Connection, extension: String) {
            try {
                connection.createStatement().use { stmt ->
                    // Floating latest from the default extension repo (matches the DuckLake connector).
                    stmt.execute("INSTALL $extension")
                    stmt.execute("LOAD $extension")
                }
            } catch (e: SQLException) {
                throw TrinoException(
                    NOT_SUPPORTED,
                    "Failed to INSTALL/LOAD the DuckDB '$extension' extension: ${e.message}. " +
                        "The connector installs it from DuckDB's extension repository (floating latest) — the " +
                        "executing DuckDB must have network access to extensions.duckdb.org, and the extension must " +
                        "be published for this DuckDB version + platform. For a remote Quack server, install it " +
                        "server-side.",
                    e,
                )
            }
        }

        companion object {
            const val LANCE: String = "lance"
            const val VORTEX: String = "vortex"

            @Suppress("unused")
            private val log: Logger = Logger.get(DuckBridgeScanExtensions::class.java)
        }
    }
