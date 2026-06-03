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

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

internal class TestQuackBackedDuckDbCatalogUrl {
    @Test
    fun matchesOnlyTheSyntheticPrefix() {
        assertThat(QuackBackedDuckDbCatalogUrl.matches("jdbc:duckdb:quack://127.0.0.1:9494")).isTrue()
        assertThat(QuackBackedDuckDbCatalogUrl.matches("jdbc:duckdb:quack://h:1?metadata_catalog=m")).isTrue()
        assertThat(QuackBackedDuckDbCatalogUrl.matches("jdbc:postgresql://h:5432/db")).isFalse()
        assertThat(QuackBackedDuckDbCatalogUrl.matches("jdbc:duckdb:")).isFalse()
        assertThat(QuackBackedDuckDbCatalogUrl.matches("jdbc:duckdb:/tmp/x.db")).isFalse()
        assertThat(QuackBackedDuckDbCatalogUrl.matches(null)).isFalse()
    }

    @Test
    fun parsesHostPortAndDefaultsMetadataCatalog() {
        val parsed = QuackBackedDuckDbCatalogUrl.parse(
            "jdbc:duckdb:quack://127.0.0.1:9494", "tokenvalue", "/tmp/data"
        )
        assertThat(parsed.host()).isEqualTo("127.0.0.1")
        assertThat(parsed.port()).isEqualTo(9494)
        assertThat(parsed.metadataCatalog()).isEqualTo(QuackBackedDuckDbCatalogUrl.DEFAULT_METADATA_CATALOG)
    }

    @Test
    fun parsesMetadataCatalogFromQueryString() {
        val parsed = QuackBackedDuckDbCatalogUrl.parse(
            "jdbc:duckdb:quack://h:9494?metadata_catalog=isolated_42", "tokenvalue", "/tmp/data"
        )
        assertThat(parsed.metadataCatalog()).isEqualTo("isolated_42")
    }

    @Test
    fun rejectsTokensShorterThanQuackMinimum() {
        assertThatThrownBy {
            QuackBackedDuckDbCatalogUrl.parse(
                "jdbc:duckdb:quack://h:1", "abc", "/tmp/data"
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("4 characters")
    }

    @Test
    fun rejectsMissingPort() {
        assertThatThrownBy {
            QuackBackedDuckDbCatalogUrl.parse(
                "jdbc:duckdb:quack://h", "tokenvalue", "/tmp/data"
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("port")
    }

    @Test
    fun rejectsMissingDataPath() {
        assertThatThrownBy {
            QuackBackedDuckDbCatalogUrl.parse(
                "jdbc:duckdb:quack://h:1", "tokenvalue", ""
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("data path")
    }

    @Test
    fun rejectsMetadataCatalogWithSqlMetacharacters() {
        assertThatThrownBy {
            QuackBackedDuckDbCatalogUrl.parse(
                "jdbc:duckdb:quack://h:1?metadata_catalog=bad'name", "tokenvalue", "/tmp/data"
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("[A-Za-z0-9_]")
    }

    @Test
    fun buildsInitSqlWithAllRequiredSteps() {
        val sql = QuackBackedDuckDbCatalogUrl.parse(
            "jdbc:duckdb:quack://h:9494?metadata_catalog=meta42", "secrettoken", "/tmp/data"
        )
            .connectionInitSql()
        assertThat(sql).contains("INSTALL quack")
        assertThat(sql).contains("LOAD quack")
        assertThat(sql).contains("INSTALL ducklake")
        assertThat(sql).contains("LOAD ducklake")
        assertThat(sql).contains("CREATE OR REPLACE SECRET (TYPE quack, TOKEN 'secrettoken')")
        assertThat(sql).contains(
            "ATTACH 'ducklake:quack:h:9494' AS lake " +
                "(DATA_PATH '/tmp/data', METADATA_CATALOG 'meta42')"
        )
        assertThat(sql).contains("USE \"meta42\".main")
    }

    @Test
    fun initSqlQuotesMetadataCatalogInUseSoNumericNamesParse() {
        // validateIdentifier accepts [A-Za-z0-9_]+, which includes numeric-only and reserved-word
        // names. Those are not legal as a bare identifier (USE 123.main fails to parse), so the
        // USE statement double-quotes the catalog name. The name is restricted to [A-Za-z0-9_],
        // so it can never contain an embedded quote.
        val sql = QuackBackedDuckDbCatalogUrl.parse(
            "jdbc:duckdb:quack://h:9494?metadata_catalog=123", "secrettoken", "/tmp/data"
        )
            .connectionInitSql()
        assertThat(sql).contains("USE \"123\".main")
        assertThat(sql).doesNotContain("USE 123.main")
    }

    @Test
    fun initSqlEscapesSingleQuotesInTokenAndDataPath() {
        val sql = QuackBackedDuckDbCatalogUrl.parse(
            "jdbc:duckdb:quack://h:9494", "tok'en'!", "/tmp/data'with'quotes"
        )
            .connectionInitSql()
        assertThat(sql).contains("TOKEN 'tok''en''!'")
        assertThat(sql).contains("DATA_PATH '/tmp/data''with''quotes'")
    }
}
