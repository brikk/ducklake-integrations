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

import java.util.Map;
import java.util.Optional;

/**
 * S3 settings copied from the catalog's connector-properties map for use by DuckDB's
 * httpfs extension on the read path. We deliberately reuse the same keys
 * ({@code s3.endpoint}, {@code s3.region}, {@code s3.aws-access-key},
 * {@code s3.aws-secret-key}, {@code s3.path-style-access}) that Trino's
 * {@code FileSystemModule} consumes for the parquet path — the user only configures
 * S3 once. Reading these directly from the config map avoids binding the same
 * keys to two airlift {@code @Config} classes (which the Bootstrap would treat as
 * a duplicate-binding error).
 *
 * <p>All fields are optional; when {@link #endpoint()} is absent the connector falls
 * back to AWS-default endpoint resolution. {@link #useSsl()} is derived from the
 * endpoint URL scheme (defaults to {@code true} when no endpoint is given).
 */
record DuckDbS3Config(
        Optional<String> endpoint,
        Optional<String> region,
        Optional<String> accessKey,
        Optional<String> secretKey,
        boolean pathStyleAccess,
        boolean useSsl)
{
    static DuckDbS3Config fromCatalogConfig(Map<String, String> config)
    {
        Optional<String> endpoint = trimmed(config.get("s3.endpoint"));
        boolean useSsl = endpoint.map(e -> !e.toLowerCase(java.util.Locale.ROOT).startsWith("http://"))
                .orElse(true);
        return new DuckDbS3Config(
                endpoint,
                trimmed(config.get("s3.region")),
                trimmed(config.get("s3.aws-access-key")),
                trimmed(config.get("s3.aws-secret-key")),
                Boolean.parseBoolean(config.getOrDefault("s3.path-style-access", "false")),
                useSsl);
    }

    private static Optional<String> trimmed(String value)
    {
        if (value == null) {
            return Optional.empty();
        }
        String t = value.trim();
        return t.isEmpty() ? Optional.empty() : Optional.of(t);
    }

    /**
     * Render a DuckDB {@code CREATE SECRET (TYPE S3, ...)} statement using these settings.
     * Caller is expected to run it once per attached connection before {@code ATTACH 's3://...'}.
     */
    String renderCreateSecretSql()
    {
        StringBuilder sql = new StringBuilder("CREATE SECRET ducklake_s3 (TYPE S3");
        endpoint.ifPresent(e -> {
            sql.append(", ENDPOINT '").append(stripScheme(e).replace("'", "''")).append("'");
        });
        region.ifPresent(r -> sql.append(", REGION '").append(r.replace("'", "''")).append("'"));
        accessKey.ifPresent(k -> sql.append(", KEY_ID '").append(k.replace("'", "''")).append("'"));
        secretKey.ifPresent(s -> sql.append(", SECRET '").append(s.replace("'", "''")).append("'"));
        sql.append(", URL_STYLE '").append(pathStyleAccess ? "path" : "vhost").append("'");
        sql.append(", USE_SSL ").append(useSsl);
        sql.append(")");
        return sql.toString();
    }

    private static String stripScheme(String url)
    {
        // DuckDB's S3 ENDPOINT field expects host:port, not a full URL. Strip http(s)://
        // if present so the user can paste the same value Trino's FileSystemModule accepts.
        int idx = url.indexOf("://");
        return idx < 0 ? url : url.substring(idx + 3);
    }
}
