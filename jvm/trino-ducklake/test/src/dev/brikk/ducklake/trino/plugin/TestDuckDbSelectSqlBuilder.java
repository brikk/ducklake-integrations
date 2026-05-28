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

import io.airlift.slice.Slices;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure-string assertions on the {@code SELECT ... WHERE ...} the executors send
 * to DuckDB. Proves the {@code trino_<name>} aliases the translator produced
 * actually arrive at the executor's WHERE clause.
 */
public class TestDuckDbSelectSqlBuilder
{
    private static final DucklakeColumnHandle NAME_COL =
            new DucklakeColumnHandle(101L, "name", VARCHAR, true);

    @Test
    public void testPushedExpressionAppearsInWhereClause()
    {
        DucklakeDuckDbExecutor.ExecutionRequest request = new DucklakeDuckDbExecutor.ExecutionRequest(
                new DuckDbAttachTarget.LocalPath(java.nio.file.Path.of("/tmp/x.db")),
                List.of(NAME_COL),
                TupleDomain.all(),
                List.of("trino_lower(\"name\") = 'apple'"));

        String sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request);
        assertThat(sql).isEqualTo("SELECT \"name\" FROM ducklake_in.main.t WHERE (trino_lower(\"name\") = 'apple')");
    }

    @Test
    public void testDomainAndExpressionsAreAndedTogether()
    {
        // domain: name IN ('a', 'b')   +   expression: trino_lower(name) = 'apple'
        DucklakeDuckDbExecutor.ExecutionRequest request = new DucklakeDuckDbExecutor.ExecutionRequest(
                new DuckDbAttachTarget.LocalPath(java.nio.file.Path.of("/tmp/x.db")),
                List.of(NAME_COL),
                TupleDomain.withColumnDomains(Map.of(
                        NAME_COL,
                        Domain.multipleValues(VARCHAR, List.of(
                                Slices.utf8Slice("a"),
                                Slices.utf8Slice("b"))))),
                List.of("trino_lower(\"name\") = 'apple'"));

        String sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request);
        // The domain clause comes first (existing translator output); the expression
        // clause is AND-ed after, wrapped in parens.
        assertThat(sql).contains(" WHERE ");
        assertThat(sql).contains(" AND ");
        assertThat(sql).contains("trino_lower(\"name\") = 'apple'");
        assertThat(sql).contains("\"name\"");
    }

    @Test
    public void testNoPredicateAtAllProducesPlainSelect()
    {
        DucklakeDuckDbExecutor.ExecutionRequest request = new DucklakeDuckDbExecutor.ExecutionRequest(
                new DuckDbAttachTarget.LocalPath(java.nio.file.Path.of("/tmp/x.db")),
                List.of(NAME_COL),
                TupleDomain.all(),
                List.of());

        String sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request);
        assertThat(sql).isEqualTo("SELECT \"name\" FROM ducklake_in.main.t");
    }

    @Test
    public void testEmptyProjectionEmitsConstantOne()
    {
        // COUNT(*) and similar — empty projection emits "SELECT 1".
        DucklakeDuckDbExecutor.ExecutionRequest request = new DucklakeDuckDbExecutor.ExecutionRequest(
                new DuckDbAttachTarget.LocalPath(java.nio.file.Path.of("/tmp/x.db")),
                List.of(),
                TupleDomain.all(),
                List.of("trino_lower(\"name\") = 'apple'"));

        String sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request);
        assertThat(sql).isEqualTo("SELECT 1 FROM ducklake_in.main.t WHERE (trino_lower(\"name\") = 'apple')");
    }

    @Test
    public void testMultipleExpressionsAreAnded()
    {
        DucklakeDuckDbExecutor.ExecutionRequest request = new DucklakeDuckDbExecutor.ExecutionRequest(
                new DuckDbAttachTarget.LocalPath(java.nio.file.Path.of("/tmp/x.db")),
                List.of(NAME_COL),
                TupleDomain.all(),
                List.of(
                        "trino_lower(\"name\") = 'apple'",
                        "(\"id\" >= 42)"));

        String sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request);
        assertThat(sql).isEqualTo(
                "SELECT \"name\" FROM ducklake_in.main.t "
                        + "WHERE (trino_lower(\"name\") = 'apple') AND ((\"id\" >= 42))");
    }
}
