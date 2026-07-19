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
 * Renders the DuckDB scan + lance-search SQL the P5 PTFs run. Ported from the DuckLake connector's
 * `LanceSearchSplitProcessor.renderExtraArgsSql`, adapted to the path-based (no-catalog) surface.
 *
 * All string arguments are single-quote escaped; vector elements are finite doubles (validated at
 * analyze time) so `Double.toString` always yields valid SQL literals; `k`/`alpha`/`prefilter` are
 * numeric/boolean. `path`/`column` names are connector-validated (never raw user SQL) but still
 * escaped defensively.
 */
object LanceSearchSql {
    fun sqlString(value: String): String = "'${value.replace("'", "''")}'"

    private fun sqlVector(vector: List<Double>): String = "[${vector.joinToString(", ")}]::DOUBLE[]"

    /** `SELECT * FROM __lance_scan('<dir>')` — plain lance dataset scan. */
    fun lanceScan(path: String): String = "SELECT * FROM __lance_scan(${sqlString(path)})"

    /** `SELECT * FROM read_vortex('<path>')` — plain vortex file scan. */
    fun vortexScan(path: String): String = "SELECT * FROM read_vortex(${sqlString(path)})"

    /** `lance_vector_search('<dir>', '<col>', [..]::DOUBLE[], k := n, prefilter := b)`. */
    fun lanceVectorSearch(path: String, column: String, queryVector: List<Double>, k: Long, prefilter: Boolean): String =
        "SELECT * FROM lance_vector_search(${sqlString(path)}, ${sqlString(column)}, ${sqlVector(queryVector)}, " +
            "k := $k, prefilter := $prefilter)"

    /** `lance_fts('<dir>', '<col>', '<query>', k := n, prefilter := b)`. */
    fun lanceFts(path: String, column: String, query: String, k: Long, prefilter: Boolean): String =
        "SELECT * FROM lance_fts(${sqlString(path)}, ${sqlString(column)}, ${sqlString(query)}, " +
            "k := $k, prefilter := $prefilter)"

    /**
     * `lance_hybrid_search('<dir>', '<vecCol>', [..]::DOUBLE[], '<textCol>', '<query>', k := n,
     * [alpha := a,] prefilter := b)`.
     */
    @Suppress("LongParameterList")
    fun lanceHybridSearch(
        path: String,
        vectorColumn: String,
        queryVector: List<Double>,
        textColumn: String,
        query: String,
        k: Long,
        alpha: Double?,
        prefilter: Boolean,
    ): String =
        buildString {
            append("SELECT * FROM lance_hybrid_search(")
            append(sqlString(path)).append(", ")
            append(sqlString(vectorColumn)).append(", ")
            append(sqlVector(queryVector)).append(", ")
            append(sqlString(textColumn)).append(", ")
            append(sqlString(query))
            append(", k := ").append(k)
            alpha?.let { append(", alpha := ").append(it) }
            append(", prefilter := ").append(prefilter)
            append(")")
        }
}
