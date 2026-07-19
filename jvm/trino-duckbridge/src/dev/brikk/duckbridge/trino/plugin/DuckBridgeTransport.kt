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

/**
 * Which JDBC transport the connector talks over, derived from the `connection-url` prefix:
 *
 *  - `jdbc:duckdb:...`  → [EMBEDDED] (T1): an in-process DuckDB opened by the DuckDB JDBC driver.
 *  - `jdbc:quack://...` → [QUACK]    (T3): a remote DuckDB reached over the Quack RPC protocol via
 *    gizmo's `quack-jdbc` driver (pass-through — every statement runs directly server-side).
 *
 * The prefix fully determines transport, so there is no separate transport config knob (URL-first,
 * per the plan). Auth/tuning that a given transport needs comes from the `duckbridge.quack.*`
 * config; the URL carries host/port.
 */
enum class DuckBridgeTransport {
    EMBEDDED,
    QUACK,
    ;

    val isRemote: Boolean get() = this == QUACK

    companion object {
        private const val EMBEDDED_PREFIX = "jdbc:duckdb:"
        private const val QUACK_PREFIX = "jdbc:quack:"

        /**
         * Classify a base-jdbc `connection-url`. Throws [IllegalArgumentException] for anything the
         * connector can't route — fail loud rather than silently defaulting a typo'd URL to embedded.
         */
        fun fromConnectionUrl(connectionUrl: String): DuckBridgeTransport =
            when {
                connectionUrl.startsWith(EMBEDDED_PREFIX) -> EMBEDDED
                connectionUrl.startsWith(QUACK_PREFIX) -> QUACK
                else ->
                    throw IllegalArgumentException(
                        "Unsupported connection-url '$connectionUrl' for the duckbridge connector. " +
                            "Use 'jdbc:duckdb:...' (embedded) or 'jdbc:quack://host:port/...' (remote Quack).",
                    )
            }
    }
}
