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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestQuackBackedDuckDbCatalogUrl
{
    @Test
    void matchesOnlyTheSyntheticPrefix()
    {
        assertThat(QuackBackedDuckDbCatalogUrl.matches("jdbc:duckdb:quack://127.0.0.1:9494")).isTrue();
        assertThat(QuackBackedDuckDbCatalogUrl.matches("jdbc:duckdb:quack://h:1?metadata_catalog=m")).isTrue();
        assertThat(QuackBackedDuckDbCatalogUrl.matches("jdbc:postgresql://h:5432/db")).isFalse();
        assertThat(QuackBackedDuckDbCatalogUrl.matches("jdbc:duckdb:")).isFalse();
        assertThat(QuackBackedDuckDbCatalogUrl.matches("jdbc:duckdb:/tmp/x.db")).isFalse();
        assertThat(QuackBackedDuckDbCatalogUrl.matches(null)).isFalse();
    }

    @Test
    void parsesHostPortAndDefaultsMetadataCatalog()
    {
        QuackBackedDuckDbCatalogUrl parsed = QuackBackedDuckDbCatalogUrl.parse(
                "jdbc:duckdb:quack://127.0.0.1:9494", "tokenvalue", "/tmp/data");
        assertThat(parsed.host()).isEqualTo("127.0.0.1");
        assertThat(parsed.port()).isEqualTo(9494);
        assertThat(parsed.metadataCatalog()).isEqualTo(QuackBackedDuckDbCatalogUrl.DEFAULT_METADATA_CATALOG);
    }

    @Test
    void parsesMetadataCatalogFromQueryString()
    {
        QuackBackedDuckDbCatalogUrl parsed = QuackBackedDuckDbCatalogUrl.parse(
                "jdbc:duckdb:quack://h:9494?metadata_catalog=isolated_42", "tokenvalue", "/tmp/data");
        assertThat(parsed.metadataCatalog()).isEqualTo("isolated_42");
    }

    @Test
    void rejectsTokensShorterThanQuackMinimum()
    {
        assertThatThrownBy(() -> QuackBackedDuckDbCatalogUrl.parse(
                "jdbc:duckdb:quack://h:1", "abc", "/tmp/data"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("4 characters");
    }

    @Test
    void rejectsMissingPort()
    {
        assertThatThrownBy(() -> QuackBackedDuckDbCatalogUrl.parse(
                "jdbc:duckdb:quack://h", "tokenvalue", "/tmp/data"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("port");
    }

    @Test
    void rejectsMissingDataPath()
    {
        assertThatThrownBy(() -> QuackBackedDuckDbCatalogUrl.parse(
                "jdbc:duckdb:quack://h:1", "tokenvalue", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("data path");
    }

    @Test
    void rejectsMetadataCatalogWithSqlMetacharacters()
    {
        assertThatThrownBy(() -> QuackBackedDuckDbCatalogUrl.parse(
                "jdbc:duckdb:quack://h:1?metadata_catalog=bad'name", "tokenvalue", "/tmp/data"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("[A-Za-z0-9_]");
    }

    @Test
    void buildsInitSqlWithAllRequiredSteps()
    {
        String sql = QuackBackedDuckDbCatalogUrl.parse(
                "jdbc:duckdb:quack://h:9494?metadata_catalog=meta42", "secrettoken", "/tmp/data")
                .connectionInitSql();
        assertThat(sql).contains("FORCE INSTALL quack FROM core_nightly");
        assertThat(sql).contains("LOAD quack");
        assertThat(sql).contains("FORCE INSTALL ducklake FROM core_nightly");
        assertThat(sql).contains("LOAD ducklake");
        assertThat(sql).contains("CREATE OR REPLACE SECRET (TYPE quack, TOKEN 'secrettoken')");
        assertThat(sql).contains("ATTACH 'ducklake:quack:h:9494' AS lake "
                + "(DATA_PATH '/tmp/data', METADATA_CATALOG 'meta42')");
        assertThat(sql).contains("USE meta42.main");
    }

    @Test
    void initSqlEscapesSingleQuotesInTokenAndDataPath()
    {
        String sql = QuackBackedDuckDbCatalogUrl.parse(
                "jdbc:duckdb:quack://h:9494", "tok'en'!", "/tmp/data'with'quotes")
                .connectionInitSql();
        assertThat(sql).contains("TOKEN 'tok''en''!'");
        assertThat(sql).contains("DATA_PATH '/tmp/data''with''quotes'");
    }
}
