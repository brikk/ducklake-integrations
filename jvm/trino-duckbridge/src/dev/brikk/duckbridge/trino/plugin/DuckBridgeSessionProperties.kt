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

import com.google.common.collect.ImmutableList
import com.google.inject.Inject
import io.trino.plugin.base.session.SessionPropertiesProvider
import io.trino.spi.connector.ConnectorSession
import io.trino.spi.session.PropertyMetadata
import io.trino.spi.session.PropertyMetadata.booleanProperty

/**
 * Connector session properties for DuckBridge. Ported from the DuckLake connector's
 * `DucklakeSessionProperties`, trimmed to the single property the pushdown translator
 * consults: [PUSHDOWN_TIMESTAMP_WITH_TIME_ZONE]. The DuckLake read/write knobs
 * (snapshot pinning, file format, writer/read mode, deletion vectors, row lineage) are
 * DuckLake-table specific and stay behind in that module.
 */
class DuckBridgeSessionProperties
    @Inject
    constructor() : SessionPropertiesProvider {
        private val sessionProperties: List<PropertyMetadata<*>> =
            ImmutableList.of(
                booleanProperty(
                    PUSHDOWN_TIMESTAMP_WITH_TIME_ZONE,
                    "Enable pushdown of date/time function predicates over TIMESTAMP WITH TIME ZONE " +
                        "columns to DuckDB. On by default. Requires a successful SET TimeZone on the " +
                        "session connection (automatic for all named IANA zones and integer-hour offsets; " +
                        "fractional bare offsets like '+05:30' degrade to Trino-side evaluation). Set to " +
                        "false to keep these predicates above the scan.",
                    true,
                    false,
                ),
            )

        override fun getSessionProperties(): List<PropertyMetadata<*>> = sessionProperties

        companion object {
            const val PUSHDOWN_TIMESTAMP_WITH_TIME_ZONE: String = "pushdown_timestamp_with_timezone"

            /**
             * @return true iff this session has opted into Tier C pushdown (date/time predicates
             * over TIMESTAMP WITH TIME ZONE columns). Default true. A null session (the test overload
             * of the translator that doesn't have one) reads as off, keeping the safer "don't push
             * WTZ" behaviour for unit tests that synthesize calls without a session.
             */
            fun isPushdownTimestampWithTimeZone(session: ConnectorSession?): Boolean {
                if (session == null) {
                    return false
                }
                val v = session.getProperty(PUSHDOWN_TIMESTAMP_WITH_TIME_ZONE, Boolean::class.javaObjectType)
                return v != null && v
            }
        }
    }
