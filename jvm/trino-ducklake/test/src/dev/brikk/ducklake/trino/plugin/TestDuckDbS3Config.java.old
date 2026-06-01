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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DuckDbS3Config} — the small adapter that copies the user's
 * {@code s3.*} catalog properties into a form DuckDB's httpfs extension can consume
 * via {@code CREATE SECRET}. The format of the rendered SQL is what the wire
 * eventually sees, so getting these strings right is load-bearing for httpfs reads.
 */
public class TestDuckDbS3Config
{
    @Test
    public void testFromCatalogConfigWithMinIoStyleSettings()
    {
        Map<String, String> raw = ImmutableMap.<String, String>builder()
                .put("s3.endpoint", "http://minio:9000")
                .put("s3.region", "us-east-1")
                .put("s3.aws-access-key", "minioadmin")
                .put("s3.aws-secret-key", "minioadmin")
                .put("s3.path-style-access", "true")
                .buildOrThrow();

        DuckDbS3Config config = DuckDbS3Config.fromCatalogConfig(raw);

        assertThat(config.endpoint()).contains("http://minio:9000");
        assertThat(config.region()).contains("us-east-1");
        assertThat(config.accessKey()).contains("minioadmin");
        assertThat(config.secretKey()).contains("minioadmin");
        assertThat(config.pathStyleAccess()).isTrue();
        // http:// scheme implies useSsl=false — guards against the easy-to-make
        // mistake of always defaulting useSsl=true.
        assertThat(config.useSsl()).isFalse();
    }

    @Test
    public void testFromCatalogConfigWithHttpsEndpointInfersSsl()
    {
        DuckDbS3Config config = DuckDbS3Config.fromCatalogConfig(Map.of(
                "s3.endpoint", "https://s3.us-west-2.amazonaws.com",
                "s3.region", "us-west-2"));

        assertThat(config.useSsl()).isTrue();
    }

    @Test
    public void testFromCatalogConfigWithMissingEndpointDefaultsToSsl()
    {
        // No endpoint = AWS-default endpoint resolution; HTTPS is the right default.
        DuckDbS3Config config = DuckDbS3Config.fromCatalogConfig(Map.of("s3.region", "us-east-1"));

        assertThat(config.endpoint()).isEmpty();
        assertThat(config.useSsl()).isTrue();
    }

    @Test
    public void testFromCatalogConfigTreatsBlankValuesAsAbsent()
    {
        DuckDbS3Config config = DuckDbS3Config.fromCatalogConfig(Map.of(
                "s3.endpoint", "",
                "s3.region", "   ",
                "s3.aws-access-key", "",
                "s3.aws-secret-key", "  "));

        assertThat(config.endpoint()).isEmpty();
        assertThat(config.region()).isEmpty();
        assertThat(config.accessKey()).isEmpty();
        assertThat(config.secretKey()).isEmpty();
    }

    @Test
    public void testRenderCreateSecretSqlMinIo()
    {
        DuckDbS3Config config = DuckDbS3Config.fromCatalogConfig(Map.of(
                "s3.endpoint", "http://minio:9000",
                "s3.region", "us-east-1",
                "s3.aws-access-key", "minioadmin",
                "s3.aws-secret-key", "minioadmin",
                "s3.path-style-access", "true"));

        String sql = config.renderCreateSecretSql();

        // ENDPOINT: scheme stripped (DuckDB's httpfs wants host:port, not URL)
        assertThat(sql).contains("ENDPOINT 'minio:9000'");
        assertThat(sql).contains("REGION 'us-east-1'");
        assertThat(sql).contains("KEY_ID 'minioadmin'");
        assertThat(sql).contains("SECRET 'minioadmin'");
        // path-style-access=true → URL_STYLE 'path' (DuckDB's enum, not Trino's)
        assertThat(sql).contains("URL_STYLE 'path'");
        // http:// inferred USE_SSL=false
        assertThat(sql).contains("USE_SSL false");
        assertThat(sql).startsWith("CREATE OR REPLACE SECRET ducklake_s3 (TYPE S3");
        assertThat(sql).endsWith(")");
    }

    @Test
    public void testRenderCreateSecretSqlAwsHosted()
    {
        DuckDbS3Config config = DuckDbS3Config.fromCatalogConfig(Map.of(
                "s3.region", "us-west-2",
                "s3.aws-access-key", "AKIAEXAMPLE",
                "s3.aws-secret-key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "s3.path-style-access", "false"));

        String sql = config.renderCreateSecretSql();

        // No endpoint emitted when the user didn't set one (AWS default resolution).
        assertThat(sql).doesNotContain("ENDPOINT");
        assertThat(sql).contains("REGION 'us-west-2'");
        assertThat(sql).contains("URL_STYLE 'vhost'");
        assertThat(sql).contains("USE_SSL true");
    }

    @Test
    public void testRenderCreateSecretSqlEscapesQuotesInValues()
    {
        // Single-quote in any of the SQL-emitted fields must be doubled or DuckDB
        // will reject the CREATE SECRET. Pin the escaping behavior — tested with
        // a key that legitimately could contain a tick character.
        DuckDbS3Config config = DuckDbS3Config.fromCatalogConfig(Map.of(
                "s3.aws-access-key", "user's-key",
                "s3.region", "us-east-1"));

        String sql = config.renderCreateSecretSql();

        assertThat(sql).contains("KEY_ID 'user''s-key'");
    }

    @Test
    public void testFromCatalogConfigEmptyMap()
    {
        // Defensive — empty map should not throw. All fields absent, useSsl=true.
        DuckDbS3Config config = DuckDbS3Config.fromCatalogConfig(Map.of());

        assertThat(config.endpoint()).isEmpty();
        assertThat(config.region()).isEmpty();
        assertThat(config.accessKey()).isEmpty();
        assertThat(config.secretKey()).isEmpty();
        assertThat(config.pathStyleAccess()).isFalse();
        assertThat(config.useSsl()).isTrue();
    }
}
