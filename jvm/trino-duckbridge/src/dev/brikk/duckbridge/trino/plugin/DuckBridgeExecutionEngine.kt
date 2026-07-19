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
 * The data-plane read strategy, selected via the `duckbridge.execution-engine` catalog property.
 *
 *  - [JDBC] (default): the plain base-jdbc row-by-row path (`JdbcRecordSetProvider`). Works over both
 *    transports (embedded DuckDB and remote Quack via quack-jdbc). This is the production default.
 *  - [DUCKDB_LOCAL] (T2): a custom Arrow page source that runs the split's SQL through an in-process
 *    DuckDB and decodes `arrowExportStream` batches to Trino Pages. Benchmark channel.
 *  - [QUACK] (T2): the same Arrow page source but reaching a remote DuckDB via the `quack` DuckDB
 *    extension + `quack_query_by_name` wrapper (NOT quack-jdbc — that driver has no Arrow surface).
 *    Benchmark channel. **Gated:** Quack 1.5.4's fixed server-side connection pool exhausts under
 *    per-split churn, so this is a benchmark channel, not the default, until the pool rework lands.
 */
enum class DuckBridgeExecutionEngine {
    JDBC,
    DUCKDB_LOCAL,
    QUACK,
    ;

    /** True for the T2 Arrow page-source engines (as opposed to the default JDBC record-set path). */
    val isArrowPageSource: Boolean get() = this != JDBC
}
