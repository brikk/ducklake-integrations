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

/**
 * Execution engine for the DuckDB-format read path. Selected via the
 * {@code ducklake.execution-engine} catalog property; consumed by
 * {@link DucklakeDuckDbExecutorFactory} to pick the concrete
 * {@link DucklakeDuckDbExecutor} implementation per split.
 */
public enum DucklakeExecutionEngine
{
    /** Embedded DuckDB in the Trino worker JVM. */
    DUCKDB_LOCAL,
    /** Out-of-process DuckDB reached over Quack RPC. */
    QUACK,
    /** Out-of-process DuckDB reached over Arrow Flight SQL. Reserved; not yet implemented. */
    SWANLAKE
}
