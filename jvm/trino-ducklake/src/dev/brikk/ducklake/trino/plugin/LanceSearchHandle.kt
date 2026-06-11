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

/**
 * Common shape of the lance search table-function handles (`lance_vector_search`, `lance_fts`,
 * `lance_hybrid_search`), produced at analyze time by the [AbstractLanceSearchTableFunction]
 * subclasses with everything execution needs fully resolved:
 *
 * - [datasetPaths]: the table's lance dataset *directories* (catalog paths resolved through
 *   [DucklakePathResolver]). [DucklakeSplitManager] emits one [LanceSearchSplit] per entry; each
 *   split computes its own local top-[k], so a multi-fragment table yields up to `k * fragments`
 *   rows (a superset of the global top-k — `ORDER BY <score column> LIMIT k` recovers exact
 *   global semantics, and [DucklakeMetadata.applyTopN] trims the per-fragment `k` for it).
 * - [outputColumns]: the produced page layout — every table column followed by the function's
 *   synthetic score column(s), in the exact order of the descriptor returned to the engine.
 *
 * Execution goes through `DucklakeMetadata.applyTableFunction`, which maps the handle to a
 * [LanceSearchTableHandle] scan so `applyFilter` / `applyTopN` compose (the engine's
 * table-function *processor* path has no pushdown hooks; it remains wired as a fallback).
 * [LanceSearchSplitProcessor] / the page source dispatch on the concrete type to render the
 * DuckDB call.
 *
 * Deliberately NOT a `ConnectorTableFunctionHandle` subtype: the engine's typed Jackson module
 * registers a polymorphic serializer for that interface and hijacks any *nested* field whose
 * declared type is assignable to it (failing with "Type id handling not implemented"). The
 * concrete handles implement both interfaces; [LanceSearchTableHandle.search] declares THIS
 * type plus field-level `@JsonTypeInfo`, keeping the engine's module out of the loop.
 */
interface LanceSearchHandle {
    val datasetPaths: List<String>
    val outputColumns: List<DucklakeColumnHandle>

    /** The per-fragment top-k forwarded to the DuckDB function (best-effort for FTS). */
    val k: Long

    /**
     * The user's filter-then-search flag, forwarded to the DuckDB function. When true, lance
     * REQUIRES any WHERE over the call to be pushable into the function (single-range
     * conjuncts; OR-of-ranges and IN-lists are not) and errors otherwise — so the page source
     * only renders prefilter-pushable domains into the WHERE and leaves the rest to the engine.
     */
    val prefilter: Boolean

    /** A copy of this handle with [k] replaced — used by `applyTopN` to trim the fragment k. */
    fun withK(newK: Long): LanceSearchHandle

    /**
     * The score column the DuckDB function naturally orders its per-fragment output by, and the
     * direction: `_distance` ascending (vector), `_score` descending (fts), `_hybrid_score`
     * descending (hybrid). A Trino `ORDER BY <this> LIMIT n` is the exact-global-top-k recipe,
     * which `applyTopN` recognizes to trim the per-fragment `k` down to `n`.
     */
    fun scoreOrderColumn(): String

    fun scoreOrderAscending(): Boolean
}
