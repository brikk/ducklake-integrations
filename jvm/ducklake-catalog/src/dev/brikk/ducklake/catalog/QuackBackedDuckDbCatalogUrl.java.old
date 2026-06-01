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
package dev.brikk.ducklake.catalog;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Parses the synthetic {@code jdbc:duckdb:quack://host:port[?metadata_catalog=name]}
 * URL used to address a DuckLake catalog hosted on a remote DuckDB over the
 * Quack RPC protocol. Builds the per-connection init-SQL script that lets a
 * plain {@code jdbc:duckdb:} pool connection talk to that remote.
 *
 * <p>The DuckDB JDBC driver doesn't natively understand this URL — we strip the
 * {@code jdbc:duckdb:} prefix, parse the remainder as a {@code quack://host:port}
 * URI, and translate the result into a {@code SET}/{@code INSTALL}/{@code LOAD}/
 * {@code CREATE SECRET}/{@code ATTACH}/{@code USE} sequence executed by HikariCP's
 * {@code connectionInitSql} hook on each new pooled connection.
 *
 * <p>After the init-SQL runs, bare references to {@code ducklake_*} metadata
 * tables resolve against the metadata catalog the ATTACH exposes (because of
 * the trailing {@code USE <metadata_catalog>.main}). That keeps
 * {@code JdbcDucklakeCatalog}'s jOOQ DSL unchanged — its queries don't need
 * any catalog/schema prefix.
 */
final class QuackBackedDuckDbCatalogUrl
{
    private static final String SYNTHETIC_URL_PREFIX = "jdbc:duckdb:quack://";
    static final String DEFAULT_METADATA_CATALOG = "ducklake_meta";

    /**
     * The real JDBC URL the HikariCP pool talks to — a plain in-memory DuckDB
     * driver instance. The Quack connection lives inside that instance.
     */
    static final String UNDERLYING_JDBC_URL = "jdbc:duckdb:";

    private final String host;
    private final int port;
    private final String metadataCatalog;
    private final String token;
    private final String dataPath;

    private QuackBackedDuckDbCatalogUrl(String host, int port, String metadataCatalog, String token, String dataPath)
    {
        this.host = requireNonNull(host, "host is null");
        this.port = port;
        this.metadataCatalog = requireNonNull(metadataCatalog, "metadataCatalog is null");
        this.token = requireNonNull(token, "token is null");
        this.dataPath = requireNonNull(dataPath, "dataPath is null");
    }

    static boolean matches(String url)
    {
        return url != null && url.startsWith(SYNTHETIC_URL_PREFIX);
    }

    /**
     * Parses {@code jdbc:duckdb:quack://host:port[?metadata_catalog=name]} plus
     * the side-channel parameters that don't live in the URL.
     *
     * @param url      synthetic URL; must start with {@code jdbc:duckdb:quack://}
     * @param token    Quack auth token; min 4 chars per Quack server validation
     * @param dataPath where DuckLake parquet data files live (must be a path
     *                 the JVM can read/write — DuckLake resolves it on the client)
     */
    static QuackBackedDuckDbCatalogUrl parse(String url, String token, String dataPath)
    {
        if (!matches(url)) {
            throw new IllegalArgumentException("not a Quack-backed DuckDB catalog URL: " + url);
        }
        // Strip the JDBC prefix to leave a parseable scheme://authority?query URI.
        String uriPart = url.substring("jdbc:duckdb:".length());
        URI uri;
        try {
            uri = new URI(uriPart);
        }
        catch (URISyntaxException ex) {
            throw new IllegalArgumentException("invalid Quack URL: " + url, ex);
        }
        String host = uri.getHost();
        int port = uri.getPort();
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Quack URL missing host: " + url);
        }
        if (port <= 0) {
            throw new IllegalArgumentException("Quack URL missing port: " + url);
        }
        Map<String, String> params = parseQuery(uri.getRawQuery());
        String metadataCatalog = params.getOrDefault("metadata_catalog", DEFAULT_METADATA_CATALOG);
        validateIdentifier(metadataCatalog, "metadata_catalog");
        if (token == null || token.length() < 4) {
            throw new IllegalArgumentException(
                    "Quack auth token must be at least 4 characters (Quack server-side requirement)");
        }
        if (dataPath == null || dataPath.isEmpty()) {
            throw new IllegalArgumentException(
                    "data path is required for Quack-backed DuckLake catalogs (set ducklake.data-path)");
        }
        return new QuackBackedDuckDbCatalogUrl(host, port, metadataCatalog, token, dataPath);
    }

    String host()
    {
        return host;
    }

    int port()
    {
        return port;
    }

    String metadataCatalog()
    {
        return metadataCatalog;
    }

    /**
     * Multi-statement init script run by HikariCP on every new pooled connection.
     * DuckDB's JDBC driver accepts semicolon-separated statements in a single
     * {@code Statement.execute()} call, which is what HikariCP does internally
     * for {@code connectionInitSql}.
     */
    String connectionInitSql()
    {
        String attachUri = "ducklake:quack:" + host + ":" + port;
        StringBuilder sb = new StringBuilder();
        // Quack ships in core as of DuckDB 1.5.3; ducklake's QuackMetadataManager
        // also rides along in the bundled extension. Plain INSTALL/LOAD against
        // the default (core) repo is sufficient — autoinstall would also work,
        // but explicit INSTALL surfaces failures at init time rather than at
        // first CREATE SECRET / ATTACH.
        sb.append("INSTALL quack;");
        sb.append("LOAD quack;");
        sb.append("INSTALL ducklake;");
        sb.append("LOAD ducklake;");
        sb.append("CREATE OR REPLACE SECRET (TYPE quack, TOKEN '").append(escapeSqlString(token)).append("');");
        sb.append("ATTACH '").append(attachUri).append("' AS lake ")
                .append("(DATA_PATH '").append(escapeSqlString(dataPath)).append("', ")
                .append("METADATA_CATALOG '").append(metadataCatalog).append("');");
        sb.append("USE ").append(metadataCatalog).append(".main;");
        return sb.toString();
    }

    private static Map<String, String> parseQuery(String rawQuery)
    {
        Map<String, String> result = new LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return result;
        }
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            String key = (eq < 0 ? pair : pair.substring(0, eq));
            String value = (eq < 0 ? "" : pair.substring(eq + 1));
            result.put(urlDecode(key), urlDecode(value));
        }
        return result;
    }

    private static String urlDecode(String s)
    {
        return java.net.URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    private static void validateIdentifier(String value, String fieldName)
    {
        if (value.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be empty");
        }
        // Conservative — the value is interpolated unquoted into the ATTACH and
        // USE statements, so anything that would let an attacker break out of
        // the identifier slot must be rejected. Standard SQL identifier chars
        // only.
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
            if (!ok) {
                throw new IllegalArgumentException(
                        fieldName + " must match [A-Za-z0-9_]+; got: " + value);
            }
        }
    }

    private static String escapeSqlString(String value)
    {
        return value.replace("'", "''");
    }
}
