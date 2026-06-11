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
package dev.brikk.ducklake.trino.plugin

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import io.trino.spi.connector.ConnectorTableHandle
import io.trino.spi.function.table.ConnectorTableFunctionHandle
import io.trino.spi.predicate.TupleDomain

/**
 * The *scan-shaped* form of a lance search: `DucklakeMetadata.applyTableFunction` maps the
 * analyzed [LanceSearchHandle] to this [ConnectorTableHandle], so the engine plans an ordinary
 * table scan over the function's output and the connector's pushdown hooks compose on top of it
 * (the table-function *processor* path has no pushdown surface — HANDOFF O2):
 *
 * - `applyFilter` intersects predicates into [pushedPredicate]; the page source renders them as
 *   a `WHERE` over the `lance_*` call, where DuckDB applies them after the search — or *before*
 *   it when the user passed `prefilter => true` (DuckDB pushes the WHERE into lance, which then
 *   filters-then-searches). All predicates stay in the engine's remaining filter, so semantics
 *   are correct either way.
 * - `applyTopN` recognizes `ORDER BY <natural score column> LIMIT n` and trims the per-fragment
 *   `k` to [limitK] = n (the union of per-fragment top-n is a superset of the global top-n; the
 *   engine still sorts and limits, so this is purely row-volume reduction).
 *
 * [searchHandle] is declared as bare [ConnectorTableFunctionHandle] — not [LanceSearchHandle] —
 * on purpose: the engine's typed Jackson module serializes any value *assignable* to that base
 * (by runtime type) with its own type envelope, but resolves the deserializer by the *declared*
 * type, which must therefore be exactly the base. Use [search] for the typed view.
 */
@JvmRecord
data class LanceSearchTableHandle @JsonCreator constructor(
        @get:JvmName("searchHandle")
        @param:JsonProperty("searchHandle") val searchHandle: ConnectorTableFunctionHandle,
        @get:JvmName("pushedPredicate")
        @param:JsonProperty("pushedPredicate") val pushedPredicate: TupleDomain<DucklakeColumnHandle>,
        @get:JvmName("limitK")
        @param:JsonProperty("limitK") val limitK: Long?) : ConnectorTableHandle
{
    init {
        require(searchHandle is LanceSearchHandle) {
            "searchHandle must be a lance search handle, got ${searchHandle.javaClass.name}"
        }
    }

    /** The typed view of [searchHandle]. */
    fun search(): LanceSearchHandle = searchHandle as LanceSearchHandle

    /** The search handle with [limitK] folded into its per-fragment `k`. */
    fun effectiveSearch(): LanceSearchHandle =
        limitK?.let { search().withK(minOf(search().k, it)) } ?: search()
}
