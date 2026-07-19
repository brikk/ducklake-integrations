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

import io.airlift.configuration.Config
import io.airlift.configuration.ConfigDescription

/**
 * Connector-level configuration for the DuckBridge connector.
 *
 * The DuckDB connection URL comes from the standard base-jdbc `connection-url` catalog
 * property (handled by [io.trino.plugin.jdbc.BaseJdbcConfig]); this class holds the
 * DuckBridge-specific knobs.
 */
class DuckBridgeConfig {
    /**
     * When true, the in-process DuckDB connection is opened with
     * `allow_unsigned_extensions=true`, which is required later to `LOAD` the
     * locally-built (unsigned) `trino_parity.duckdb_extension`. Defaults to true so the
     * parity extension path works out of the box; operators can flip it off to harden a
     * deployment that never loads the extension.
     */
    var isAllowUnsignedExtensions: Boolean = true
        private set

    @Config("duckbridge.allow-unsigned-extensions")
    @ConfigDescription("Allow the in-process DuckDB to LOAD unsigned extensions (needed for the trino_parity extension)")
    fun setAllowUnsignedExtensions(allowUnsignedExtensions: Boolean): DuckBridgeConfig {
        this.isAllowUnsignedExtensions = allowUnsignedExtensions
        return this
    }
}
