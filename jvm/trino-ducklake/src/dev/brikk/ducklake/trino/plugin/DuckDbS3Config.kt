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

import java.util.Optional

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
@JvmRecord
data class DuckDbS3Config(
        @get:JvmName("endpoint") val endpoint: Optional<String>,
        @get:JvmName("region") val region: Optional<String>,
        @get:JvmName("accessKey") val accessKey: Optional<String>,
        @get:JvmName("secretKey") val secretKey: Optional<String>,
        @get:JvmName("pathStyleAccess") val pathStyleAccess: Boolean,
        @get:JvmName("useSsl") val useSsl: Boolean) {
    /**
     * Render a DuckDB {@code CREATE SECRET IF NOT EXISTS ducklake_s3 (TYPE S3, ...)} statement
     * using these settings. {@code IF NOT EXISTS} (NOT {@code OR REPLACE}) is load-bearing on the
     * Quack execution-engine path, where the secret is server-instance-scoped and shared across
     * concurrent sessions:
     *
     *  - {@code OR REPLACE} under concurrency is a proven failure (live 2026-06-10, pinned by
     *    `TestDucklakeQuackS3InitRace`): DuckDB 1.5.3 aborts overlapping replaces with
     *    `Catalog write-write conflict on create/alter with "ducklake_s3"`, and worse, the
     *    drop+recreate window can leave a concurrently-binding scan with NO matching secret —
     *    observed as vortex's object_store falling back to the EC2 metadata service
     *    (`PUT http://169.254.169.254/...`) and failing the query seconds later.
     *  - {@code IF NOT EXISTS} on an existing secret performs no catalog write at all (probed:
     *    400 concurrent creates, zero conflicts), so steady state is race-free and the secret
     *    never blinks out under a scan. First-contact creates can still conflict (probed: 7 of 8
     *    in a simultaneous bootstrap storm) — [QuackDuckDbExecutor] retries those.
     *
     * Consequences: the FIRST creator wins and later contents are silently ignored, so rotated
     * s3 credentials reach a long-lived Quack server only after a server restart (new DuckDB
     * instance). On the in-process path each split gets a fresh DuckDB, so IF NOT EXISTS always
     * creates and behaves identically to the old OR REPLACE there.
     */
    fun renderCreateSecretSql(): String {
        val sql = StringBuilder("CREATE SECRET IF NOT EXISTS ducklake_s3 (TYPE S3")
        endpoint.ifPresent { e ->
            sql.append(", ENDPOINT '").append(stripScheme(e).replace("'", "''")).append("'")
        }
        region.ifPresent { r -> sql.append(", REGION '").append(r.replace("'", "''")).append("'") }
        accessKey.ifPresent { k -> sql.append(", KEY_ID '").append(k.replace("'", "''")).append("'") }
        secretKey.ifPresent { s -> sql.append(", SECRET '").append(s.replace("'", "''")).append("'") }
        sql.append(", URL_STYLE '").append(if (pathStyleAccess) "path" else "vhost").append("'")
        sql.append(", USE_SSL ").append(useSsl)
        sql.append(")")
        return sql.toString()
    }

    /**
     * Render these settings as the **object_store `AWS_*` environment variables** that the
     * DuckDB `lance` extension's Rust object_store reads. Lance does NOT honor the DuckDB
     * httpfs secret ([renderCreateSecretSql] is a no-op for lance s3 paths — HANDOFF O1,
     * probed 2026-06-10 against MinIO), so any process running lance-over-s3 needs this env:
     * the Quack sidecar container at launch, or the Trino JVM itself for the in-process
     * engine (env is process-global — single s3 identity only).
     *
     * Keys are the probe-verified set: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
     * `AWS_REGION`, `AWS_ENDPOINT` + `AWS_ENDPOINT_URL` (both spellings, for object_store
     * version drift; value includes the scheme), and `AWS_ALLOW_HTTP=true` for http
     * endpoints. Mirrors the `s3.* → AWS_*` aliasing the Doris BE integration already does
     * for its native s3 client.
     */
    fun toObjectStoreEnv(): Map<String, String> {
        val env = LinkedHashMap<String, String>()
        accessKey.ifPresent { env["AWS_ACCESS_KEY_ID"] = it }
        secretKey.ifPresent { env["AWS_SECRET_ACCESS_KEY"] = it }
        region.ifPresent { env["AWS_REGION"] = it }
        endpoint.ifPresent { e ->
            // object_store wants a URL with scheme; catalog config may carry either form.
            val url = if (e.contains("://")) e else (if (useSsl) "https://" else "http://") + e
            env["AWS_ENDPOINT"] = url
            env["AWS_ENDPOINT_URL"] = url
        }
        if (!useSsl) {
            env["AWS_ALLOW_HTTP"] = "true"
        }
        return env
    }

    companion object {
        fun fromCatalogConfig(config: Map<String, String>): DuckDbS3Config {
            val endpoint = trimmed(config["s3.endpoint"])
            val useSsl = endpoint?.let { e -> !e.lowercase(java.util.Locale.ROOT).startsWith("http://") }
                    ?: true
            return DuckDbS3Config(
                    Optional.ofNullable(endpoint),
                    Optional.ofNullable(trimmed(config["s3.region"])),
                    Optional.ofNullable(trimmed(config["s3.aws-access-key"])),
                    Optional.ofNullable(trimmed(config["s3.aws-secret-key"])),
                    java.lang.Boolean.parseBoolean(config.getOrDefault("s3.path-style-access", "false")),
                    useSsl)
        }

        private fun trimmed(value: String?): String? {
            val t = (value ?: return null).trim()
            return t.ifEmpty { null }
        }

        private fun stripScheme(url: String): String {
            // DuckDB's S3 ENDPOINT field expects host:port, not a full URL. Strip http(s)://
            // if present so the user can paste the same value Trino's FileSystemModule accepts.
            val idx = url.indexOf("://")
            return if (idx < 0) url else url.substring(idx + 3)
        }
    }
}
