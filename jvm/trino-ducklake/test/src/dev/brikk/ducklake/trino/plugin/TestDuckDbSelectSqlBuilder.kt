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

import io.airlift.slice.Slices
import io.trino.spi.predicate.Domain
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.VarcharType.VARCHAR
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Path

/**
 * Pure-string assertions on the `SELECT ... WHERE ...` the executors send
 * to DuckDB. Proves the `trino_<name>` aliases the translator produced
 * actually arrive at the executor's WHERE clause.
 */
class TestDuckDbSelectSqlBuilder {
    @Test
    fun testPushedExpressionAppearsInWhereClause() {
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(Path.of("/tmp/x.db")),
                listOf(NAME_COL),
                TupleDomain.all(),
                listOf("trino_lower(\"name\") = 'apple'"))

        val sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request)
        assertThat(sql).isEqualTo("SELECT \"name\" FROM ducklake_in.main.t WHERE (trino_lower(\"name\") = 'apple')")
    }

    @Test
    fun testDomainAndExpressionsAreAndedTogether() {
        // domain: name IN ('a', 'b')   +   expression: trino_lower(name) = 'apple'
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(Path.of("/tmp/x.db")),
                listOf(NAME_COL),
                TupleDomain.withColumnDomains(mapOf(
                        NAME_COL to
                                Domain.multipleValues(VARCHAR, listOf(
                                        Slices.utf8Slice("a"),
                                        Slices.utf8Slice("b"))))),
                listOf("trino_lower(\"name\") = 'apple'"))

        val sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request)
        // The domain clause comes first (existing translator output); the expression
        // clause is AND-ed after, wrapped in parens.
        assertThat(sql).contains(" WHERE ")
        assertThat(sql).contains(" AND ")
        assertThat(sql).contains("trino_lower(\"name\") = 'apple'")
        assertThat(sql).contains("\"name\"")
    }

    @Test
    fun testNoPredicateAtAllProducesPlainSelect() {
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(Path.of("/tmp/x.db")),
                listOf(NAME_COL),
                TupleDomain.all(),
                listOf())

        val sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request)
        assertThat(sql).isEqualTo("SELECT \"name\" FROM ducklake_in.main.t")
    }

    @Test
    fun testEmptyProjectionEmitsConstantOne() {
        // COUNT(*) and similar — empty projection emits "SELECT 1".
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(Path.of("/tmp/x.db")),
                listOf(),
                TupleDomain.all(),
                listOf("trino_lower(\"name\") = 'apple'"))

        val sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request)
        assertThat(sql).isEqualTo("SELECT 1 FROM ducklake_in.main.t WHERE (trino_lower(\"name\") = 'apple')")
    }

    @Test
    fun testMultipleExpressionsAreAnded() {
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(Path.of("/tmp/x.db")),
                listOf(NAME_COL),
                TupleDomain.all(),
                listOf(
                        "trino_lower(\"name\") = 'apple'",
                        "(\"id\" >= 42)"))

        val sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request)
        assertThat(sql).isEqualTo(
                "SELECT \"name\" FROM ducklake_in.main.t " +
                        "WHERE (trino_lower(\"name\") = 'apple') AND ((\"id\" >= 42))")
    }

    companion object {
        private val NAME_COL: DucklakeColumnHandle =
                DucklakeColumnHandle(101L, "name", VARCHAR, true)
    }
}
