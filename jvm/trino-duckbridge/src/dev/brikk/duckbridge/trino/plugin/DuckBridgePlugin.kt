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

import io.trino.plugin.jdbc.JdbcPlugin

/**
 * DuckBridge: a generic Trino -> DuckDB connector, seed for later parity-extension pushdown phases.
 *
 * Built on Trino's base-jdbc framework. The `duckbridge` connector name is the string users put
 * in a catalog properties file (`connector.name=duckbridge`).
 *
 * [JdbcPlugin] wires the `query` passthrough table function for free.
 */
class DuckBridgePlugin : JdbcPlugin("duckbridge", { DuckBridgeClientModule() })
