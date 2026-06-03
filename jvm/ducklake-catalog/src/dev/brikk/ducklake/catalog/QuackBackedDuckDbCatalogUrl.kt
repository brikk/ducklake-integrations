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
package dev.brikk.ducklake.catalog

import java.net.URI
import java.net.URISyntaxException
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.util.LinkedHashMap

/**
 * Parses the synthetic `jdbc:duckdb:quack://host:port[?metadata_catalog=name]`
 * URL used to address a DuckLake catalog hosted on a remote DuckDB over the
 * Quack RPC protocol. Builds the per-connection init-SQL script that lets a
 * plain `jdbc:duckdb:` pool connection talk to that remote.
 *
 * The DuckDB JDBC driver doesn't natively understand this URL — we strip the
 * `jdbc:duckdb:` prefix, parse the remainder as a `quack://host:port`
 * URI, and translate the result into a `SET`/`INSTALL`/`LOAD`/
 * `CREATE SECRET`/`ATTACH`/`USE` sequence executed by HikariCP's
 * `connectionInitSql` hook on each new pooled connection.
 *
 * After the init-SQL runs, bare references to `ducklake_*` metadata
 * tables resolve against the metadata catalog the ATTACH exposes (because of
 * the trailing `USE <metadata_catalog>.main`). That keeps
 * `JdbcDucklakeCatalog`'s jOOQ DSL unchanged — its queries don't need
 * any catalog/schema prefix.
 */
internal class QuackBackedDuckDbCatalogUrl private constructor(
        host: String,
        port: Int,
        metadataCatalog: String,
        token: String,
        dataPath: String,
) {
    private val host: String = host
    private val port: Int = port
    private val metadataCatalog: String = metadataCatalog
    private val token: String = token
    private val dataPath: String = dataPath

    fun host(): String = host

    fun port(): Int = port

    fun metadataCatalog(): String = metadataCatalog

    /**
     * Multi-statement init script run by HikariCP on every new pooled connection.
     * DuckDB's JDBC driver accepts semicolon-separated statements in a single
     * `Statement.execute()` call, which is what HikariCP does internally
     * for `connectionInitSql`.
     */
    fun connectionInitSql(): String {
        // host goes inside the single-quoted ATTACH literal below, so escape it for the same
        // reason token/dataPath are escaped. URI.getHost() rejects quote-bearing authorities
        // today, but escaping here keeps the literal safe regardless of how host is derived.
        val attachUri = "ducklake:quack:${escapeSqlString(host)}:$port"
        val sb = StringBuilder()
        // Quack ships in core as of DuckDB 1.5.3; ducklake's QuackMetadataManager
        // also rides along in the bundled extension. Plain INSTALL/LOAD against
        // the default (core) repo is sufficient — autoinstall would also work,
        // but explicit INSTALL surfaces failures at init time rather than at
        // first CREATE SECRET / ATTACH.
        sb.append("INSTALL quack;")
        sb.append("LOAD quack;")
        sb.append("INSTALL ducklake;")
        sb.append("LOAD ducklake;")
        sb.append("CREATE OR REPLACE SECRET (TYPE quack, TOKEN '").append(escapeSqlString(token)).append("');")
        sb.append("ATTACH '").append(attachUri).append("' AS lake ")
                .append("(DATA_PATH '").append(escapeSqlString(dataPath)).append("', ")
                .append("METADATA_CATALOG '").append(metadataCatalog).append("');")
        // Double-quote the catalog identifier in USE: validateIdentifier allows [A-Za-z0-9_]+,
        // which includes numeric-only ("123") and reserved-word names that are not legal as a
        // bare identifier (USE 123.main fails to parse). Quoting makes the statement valid for
        // any accepted name and future-proofs against keyword collisions; the [A-Za-z0-9_]
        // restriction guarantees no embedded quote, and DuckDB folds quoted identifiers
        // case-insensitively so letter-leading names behave exactly as before.
        sb.append("USE \"").append(metadataCatalog).append("\".main;")
        return sb.toString()
    }

    companion object {
        private const val SYNTHETIC_URL_PREFIX = "jdbc:duckdb:quack://"

        const val DEFAULT_METADATA_CATALOG: String = "ducklake_meta"

        /**
         * The real JDBC URL the HikariCP pool talks to — a plain in-memory DuckDB
         * driver instance. The Quack connection lives inside that instance.
         */
        const val UNDERLYING_JDBC_URL: String = "jdbc:duckdb:"

        @JvmStatic
        fun matches(url: String?): Boolean {
            return url != null && url.startsWith(SYNTHETIC_URL_PREFIX)
        }

        /**
         * Parses `jdbc:duckdb:quack://host:port[?metadata_catalog=name]` plus
         * the side-channel parameters that don't live in the URL.
         *
         * @param url      synthetic URL; must start with `jdbc:duckdb:quack://`
         * @param token    Quack auth token; min 4 chars per Quack server validation
         * @param dataPath where DuckLake parquet data files live (must be a path
         *                 the JVM can read/write — DuckLake resolves it on the client)
         */
        @JvmStatic
        fun parse(url: String, token: String?, dataPath: String?): QuackBackedDuckDbCatalogUrl {
            if (!matches(url)) {
                throw IllegalArgumentException("not a Quack-backed DuckDB catalog URL: $url")
            }
            // Strip the JDBC prefix to leave a parseable scheme://authority?query URI.
            val uriPart = url.substring("jdbc:duckdb:".length)
            val uri: URI = try {
                URI(uriPart)
            }
            catch (ex: URISyntaxException) {
                throw IllegalArgumentException("invalid Quack URL: $url", ex)
            }
            val host = uri.host
            val port = uri.port
            if (host == null || host.isEmpty()) {
                throw IllegalArgumentException("Quack URL missing host: $url")
            }
            if (port <= 0) {
                throw IllegalArgumentException("Quack URL missing port: $url")
            }
            val params = parseQuery(uri.rawQuery)
            val metadataCatalog = params.getOrDefault("metadata_catalog", DEFAULT_METADATA_CATALOG)
            validateIdentifier(metadataCatalog, "metadata_catalog")
            if (token == null || token.length < 4) {
                throw IllegalArgumentException(
                        "Quack auth token must be at least 4 characters (Quack server-side requirement)")
            }
            if (dataPath == null || dataPath.isEmpty()) {
                throw IllegalArgumentException(
                        "data path is required for Quack-backed DuckLake catalogs (set ducklake.data-path)")
            }
            return QuackBackedDuckDbCatalogUrl(host, port, metadataCatalog, token, dataPath)
        }

        private fun parseQuery(rawQuery: String?): Map<String, String> {
            val result = LinkedHashMap<String, String>()
            if (rawQuery == null || rawQuery.isEmpty()) {
                return result
            }
            for (pair in rawQuery.split("&")) {
                val eq = pair.indexOf('=')
                val key = if (eq < 0) pair else pair.substring(0, eq)
                val value = if (eq < 0) "" else pair.substring(eq + 1)
                result[urlDecode(key)] = urlDecode(value)
            }
            return result
        }

        private fun urlDecode(s: String): String {
            return URLDecoder.decode(s, StandardCharsets.UTF_8)
        }

        private fun validateIdentifier(value: String, fieldName: String) {
            if (value.isEmpty()) {
                throw IllegalArgumentException("$fieldName must not be empty")
            }
            // Conservative — the value is interpolated unquoted into the ATTACH and
            // USE statements, so anything that would let an attacker break out of
            // the identifier slot must be rejected. Standard SQL identifier chars
            // only.
            for (i in 0 until value.length) {
                val c = value[i]
                val ok = (c in 'a'..'z') || (c in 'A'..'Z') || (c in '0'..'9') || c == '_'
                if (!ok) {
                    throw IllegalArgumentException(
                            "$fieldName must match [A-Za-z0-9_]+; got: $value")
                }
            }
        }

        private fun escapeSqlString(value: String): String {
            return value.replace("'", "''")
        }
    }
}
