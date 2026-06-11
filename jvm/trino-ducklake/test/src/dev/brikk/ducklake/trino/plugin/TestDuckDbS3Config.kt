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

import com.google.common.collect.ImmutableMap
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Unit tests for [DuckDbS3Config] — the small adapter that copies the user's
 * `s3.*` catalog properties into a form DuckDB's httpfs extension can consume
 * via `CREATE SECRET`. The format of the rendered SQL is what the wire
 * eventually sees, so getting these strings right is load-bearing for httpfs reads.
 */
class TestDuckDbS3Config {
    @Test
    fun testFromCatalogConfigWithMinIoStyleSettings() {
        val raw: Map<String, String> = ImmutableMap.builder<String, String>()
                .put("s3.endpoint", "http://minio:9000")
                .put("s3.region", "us-east-1")
                .put("s3.aws-access-key", "minioadmin")
                .put("s3.aws-secret-key", "minioadmin")
                .put("s3.path-style-access", "true")
                .buildOrThrow()

        val config = DuckDbS3Config.fromCatalogConfig(raw)

        assertThat(config.endpoint).contains("http://minio:9000")
        assertThat(config.region).contains("us-east-1")
        assertThat(config.accessKey).contains("minioadmin")
        assertThat(config.secretKey).contains("minioadmin")
        assertThat(config.pathStyleAccess).isTrue()
        // http:// scheme implies useSsl=false — guards against the easy-to-make
        // mistake of always defaulting useSsl=true.
        assertThat(config.useSsl).isFalse()
    }

    @Test
    fun testFromCatalogConfigWithHttpsEndpointInfersSsl() {
        val config = DuckDbS3Config.fromCatalogConfig(mapOf(
                "s3.endpoint" to "https://s3.us-west-2.amazonaws.com",
                "s3.region" to "us-west-2"))

        assertThat(config.useSsl).isTrue()
    }

    @Test
    fun testFromCatalogConfigWithMissingEndpointDefaultsToSsl() {
        // No endpoint = AWS-default endpoint resolution; HTTPS is the right default.
        val config = DuckDbS3Config.fromCatalogConfig(mapOf("s3.region" to "us-east-1"))

        assertThat(config.endpoint).isEmpty
        assertThat(config.useSsl).isTrue()
    }

    @Test
    fun testFromCatalogConfigTreatsBlankValuesAsAbsent() {
        val config = DuckDbS3Config.fromCatalogConfig(mapOf(
                "s3.endpoint" to "",
                "s3.region" to "   ",
                "s3.aws-access-key" to "",
                "s3.aws-secret-key" to "  "))

        assertThat(config.endpoint).isEmpty
        assertThat(config.region).isEmpty
        assertThat(config.accessKey).isEmpty
        assertThat(config.secretKey).isEmpty
    }

    @Test
    fun testRenderCreateSecretSqlMinIo() {
        val config = DuckDbS3Config.fromCatalogConfig(mapOf(
                "s3.endpoint" to "http://minio:9000",
                "s3.region" to "us-east-1",
                "s3.aws-access-key" to "minioadmin",
                "s3.aws-secret-key" to "minioadmin",
                "s3.path-style-access" to "true"))

        val sql = config.renderCreateSecretSql()

        // ENDPOINT: scheme stripped (DuckDB's httpfs wants host:port, not URL)
        assertThat(sql).contains("ENDPOINT 'minio:9000'")
        assertThat(sql).contains("REGION 'us-east-1'")
        assertThat(sql).contains("KEY_ID 'minioadmin'")
        assertThat(sql).contains("SECRET 'minioadmin'")
        // path-style-access=true → URL_STYLE 'path' (DuckDB's enum, not Trino's)
        assertThat(sql).contains("URL_STYLE 'path'")
        // http:// inferred USE_SSL=false
        assertThat(sql).contains("USE_SSL false")
        // IF NOT EXISTS, NOT OR REPLACE: on the shared Quack server an existing secret must be
        // a catalog no-op — concurrent replaces conflict and blink the secret out under
        // concurrent scans (see renderCreateSecretSql's doc + TestDucklakeQuackS3InitRace).
        assertThat(sql).startsWith("CREATE SECRET IF NOT EXISTS ducklake_s3 (TYPE S3")
        assertThat(sql).endsWith(")")
    }

    @Test
    fun testRenderCreateSecretSqlAwsHosted() {
        val config = DuckDbS3Config.fromCatalogConfig(mapOf(
                "s3.region" to "us-west-2",
                "s3.aws-access-key" to "AKIAEXAMPLE",
                "s3.aws-secret-key" to "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "s3.path-style-access" to "false"))

        val sql = config.renderCreateSecretSql()

        // No endpoint emitted when the user didn't set one (AWS default resolution).
        assertThat(sql).doesNotContain("ENDPOINT")
        assertThat(sql).contains("REGION 'us-west-2'")
        assertThat(sql).contains("URL_STYLE 'vhost'")
        assertThat(sql).contains("USE_SSL true")
    }

    @Test
    fun testRenderCreateSecretSqlEscapesQuotesInValues() {
        // Single-quote in any of the SQL-emitted fields must be doubled or DuckDB
        // will reject the CREATE SECRET. Pin the escaping behavior — tested with
        // a key that legitimately could contain a tick character.
        val config = DuckDbS3Config.fromCatalogConfig(mapOf(
                "s3.aws-access-key" to "user's-key",
                "s3.region" to "us-east-1"))

        val sql = config.renderCreateSecretSql()

        assertThat(sql).contains("KEY_ID 'user''s-key'")
    }

    @Test
    fun testFromCatalogConfigEmptyMap() {
        // Defensive — empty map should not throw. All fields absent, useSsl=true.
        val config = DuckDbS3Config.fromCatalogConfig(emptyMap())

        assertThat(config.endpoint).isEmpty
        assertThat(config.region).isEmpty
        assertThat(config.accessKey).isEmpty
        assertThat(config.secretKey).isEmpty
        assertThat(config.pathStyleAccess).isFalse()
        assertThat(config.useSsl).isTrue()
    }

    /**
     * The object_store `AWS_*` env mapping for lance s3 (HANDOFF O1). This exact key/value
     * set was verified live against MinIO (2026-06-10, env-injected child process): lance
     * COPY-write, `__lance_scan`, `lance_vector_search`, and `lance_fts` over `s3://` all
     * succeed with it, and all fail with only the DuckDB httpfs secret present.
     */
    @Test
    fun testToObjectStoreEnvMinioStyle() {
        val config = DuckDbS3Config.fromCatalogConfig(mapOf(
                "s3.endpoint" to "http://localhost:9000",
                "s3.region" to "us-east-1",
                "s3.aws-access-key" to "minio-user",
                "s3.aws-secret-key" to "minio-secret",
                "s3.path-style-access" to "true"))

        assertThat(config.toObjectStoreEnv()).containsExactlyInAnyOrderEntriesOf(mapOf(
                "AWS_ACCESS_KEY_ID" to "minio-user",
                "AWS_SECRET_ACCESS_KEY" to "minio-secret",
                "AWS_REGION" to "us-east-1",
                // Scheme preserved; both endpoint spellings for object_store version drift.
                "AWS_ENDPOINT" to "http://localhost:9000",
                "AWS_ENDPOINT_URL" to "http://localhost:9000",
                "AWS_ALLOW_HTTP" to "true"))
    }

    @Test
    fun testToObjectStoreEnvAwsHosted() {
        val config = DuckDbS3Config.fromCatalogConfig(mapOf(
                "s3.region" to "us-west-2",
                "s3.aws-access-key" to "AKIAEXAMPLE",
                "s3.aws-secret-key" to "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"))

        // No endpoint configured → object_store's AWS default resolution; https → no ALLOW_HTTP.
        assertThat(config.toObjectStoreEnv()).containsExactlyInAnyOrderEntriesOf(mapOf(
                "AWS_ACCESS_KEY_ID" to "AKIAEXAMPLE",
                "AWS_SECRET_ACCESS_KEY" to "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "AWS_REGION" to "us-west-2"))
    }

    @Test
    fun testToObjectStoreEnvAddsSchemeToBareEndpoint() {
        // Catalog config may carry a scheme-less host:port (the DuckDB-secret form);
        // object_store wants a URL — the scheme is derived from useSsl.
        val httpsConfig = DuckDbS3Config(
                java.util.Optional.of("s3.example.com"),
                java.util.Optional.empty(),
                java.util.Optional.empty(),
                java.util.Optional.empty(),
                false,
                true)
        assertThat(httpsConfig.toObjectStoreEnv()["AWS_ENDPOINT"]).isEqualTo("https://s3.example.com")
        assertThat(httpsConfig.toObjectStoreEnv()).doesNotContainKey("AWS_ALLOW_HTTP")

        val httpConfig = httpsConfig.copy(useSsl = false)
        assertThat(httpConfig.toObjectStoreEnv()["AWS_ENDPOINT"]).isEqualTo("http://s3.example.com")
        assertThat(httpConfig.toObjectStoreEnv()["AWS_ALLOW_HTTP"]).isEqualTo("true")
    }

    @Test
    fun testToObjectStoreEnvEmptyConfig() {
        assertThat(DuckDbS3Config.fromCatalogConfig(emptyMap()).toObjectStoreEnv()).isEmpty()
    }
}
