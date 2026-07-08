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
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.RowType
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

    // ==================== Schema-evolution projection (fileColumnNamesById) ====================

    @Test
    fun testRenamedColumnIsProjectedUnderFileNameAliasedToCurrent() {
        // Current name "full_name" (id 101); the physical file still has it as "name".
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(Path.of("/tmp/x.db")),
                listOf(DucklakeColumnHandle(101L, "full_name", VARCHAR, true)),
                TupleDomain.all(),
                listOf(),
                null,
                mapOf(101L to "name"))

        val sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request)
        assertThat(sql).isEqualTo("SELECT \"name\" AS \"full_name\" FROM ducklake_in.main.t")
    }

    @Test
    fun testAddedColumnIsProjectedAsTypedNull() {
        // id 101 present in file; id 102 ("score", INTEGER) added after the file was written.
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(Path.of("/tmp/x.db")),
                listOf(
                        DucklakeColumnHandle(101L, "name", VARCHAR, true),
                        DucklakeColumnHandle(102L, "score", INTEGER, true)),
                TupleDomain.all(),
                listOf(),
                null,
                mapOf(101L to "name"))

        val sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request)
        assertThat(sql).isEqualTo(
                "SELECT \"name\", CAST(NULL AS INTEGER) AS \"score\" FROM ducklake_in.main.t")
    }

    @Test
    fun testPresentColumnWithUnchangedNameHasNoAlias() {
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(Path.of("/tmp/x.db")),
                listOf(NAME_COL),
                TupleDomain.all(),
                listOf(),
                null,
                mapOf(101L to "name"))

        val sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request)
        assertThat(sql).isEqualTo("SELECT \"name\" FROM ducklake_in.main.t")
    }

    @Test
    fun testEmptyFileNameMapProjectsCurrentNames() {
        // No resolution (no-evolution fast path): project the current name verbatim.
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(Path.of("/tmp/x.db")),
                listOf(NAME_COL),
                TupleDomain.all(),
                listOf(),
                null,
                emptyMap())

        val sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request)
        assertThat(sql).isEqualTo("SELECT \"name\" FROM ducklake_in.main.t")
    }

    // =============== Nested struct-evolution reshaping (structReshapePlans) ===============

    @Test
    fun testNestedAddedFieldIsStructPackedWithNullFill() {
        // current s = row(a integer, b integer); the file had only row(a) — b added later.
        val structType = RowType.rowType(RowType.field("a", INTEGER), RowType.field("b", INTEGER))
        val request = reshapeRequest(
                DucklakeColumnHandle(200L, "s", structType, true),
                listOf(
                        StructFieldPlan("a", INTEGER, "a", null),
                        StructFieldPlan("b", INTEGER, null, null)))

        val sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request)
        assertThat(sql).isEqualTo(
                "SELECT CASE WHEN \"s\" IS NULL THEN NULL ELSE " +
                        "struct_pack(\"a\" := (\"s\").\"a\", \"b\" := CAST(NULL AS INTEGER)) END AS \"s\" " +
                        "FROM ducklake_in.main.t")
    }

    @Test
    fun testNestedAddedFieldWithDefaultIsStructPackedWithDefault() {
        // current s = row(a integer, k integer); k added later with DEFAULT 42. The reshape projects
        // the default literal (rows predating ADD COLUMN s.k … DEFAULT 42), not NULL.
        val structType = RowType.rowType(RowType.field("a", INTEGER), RowType.field("k", INTEGER))
        val request = reshapeRequest(
                DucklakeColumnHandle(200L, "s", structType, true),
                listOf(
                        StructFieldPlan("a", INTEGER, "a", null),
                        StructFieldPlan("k", INTEGER, null, null, "42")))

        val sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request)
        assertThat(sql).isEqualTo(
                "SELECT CASE WHEN \"s\" IS NULL THEN NULL ELSE " +
                        "struct_pack(\"a\" := (\"s\").\"a\", \"k\" := CAST('42' AS INTEGER)) END AS \"s\" " +
                        "FROM ducklake_in.main.t")
    }

    @Test
    fun testNestedPromotedFieldIsStructPackedWithCast() {
        // current s = row(a bigint); the file stored a as integer (ALTER COLUMN s.a SET DATA TYPE
        // BIGINT since). The present field is CAST to the current type inside struct_pack — the file
        // holds the old physical type and the Arrow→page converter reads by the current type.
        val structType = RowType.rowType(RowType.field("a", BIGINT))
        val request = reshapeRequest(
                DucklakeColumnHandle(200L, "s", structType, true),
                listOf(StructFieldPlan("a", BIGINT, "a", null, null, true)))

        val sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request)
        assertThat(sql).isEqualTo(
                "SELECT CASE WHEN \"s\" IS NULL THEN NULL ELSE " +
                        "struct_pack(\"a\" := CAST((\"s\").\"a\" AS BIGINT)) END AS \"s\" " +
                        "FROM ducklake_in.main.t")
    }

    @Test
    fun testNestedDroppedNonTrailingFieldIsOmitted() {
        // current s = row(a, c); the file had row(a, b, c) — b dropped. struct_pack selects a, c by
        // file name, so the file's extra b is simply not read (no positional misbind).
        val structType = RowType.rowType(RowType.field("a", INTEGER), RowType.field("c", INTEGER))
        val request = reshapeRequest(
                DucklakeColumnHandle(200L, "s", structType, true),
                listOf(
                        StructFieldPlan("a", INTEGER, "a", null),
                        StructFieldPlan("c", INTEGER, "c", null)))

        val sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request)
        assertThat(sql).isEqualTo(
                "SELECT CASE WHEN \"s\" IS NULL THEN NULL ELSE " +
                        "struct_pack(\"a\" := (\"s\").\"a\", \"c\" := (\"s\").\"c\") END AS \"s\" " +
                        "FROM ducklake_in.main.t")
    }

    @Test
    fun testNestedRenamedFieldMapsCurrentNameToFileName() {
        // current s = row(a); the file stored the field as old_a (renamed since).
        val structType = RowType.rowType(RowType.field("a", INTEGER))
        val request = reshapeRequest(
                DucklakeColumnHandle(200L, "s", structType, true),
                listOf(StructFieldPlan("a", INTEGER, "old_a", null)))

        val sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request)
        assertThat(sql).isEqualTo(
                "SELECT CASE WHEN \"s\" IS NULL THEN NULL ELSE " +
                        "struct_pack(\"a\" := (\"s\").\"old_a\") END AS \"s\" FROM ducklake_in.main.t")
    }

    @Test
    fun testNestedStructInStructIsReshapedRecursively() {
        // current s = row(child row(a, b)); the file had row(child row(a)) — b added in the inner struct.
        val innerType = RowType.rowType(RowType.field("a", INTEGER), RowType.field("b", INTEGER))
        val structType = RowType.rowType(RowType.field("child", innerType))
        val request = reshapeRequest(
                DucklakeColumnHandle(200L, "s", structType, true),
                listOf(StructFieldPlan("child", innerType, "child", listOf(
                        StructFieldPlan("a", INTEGER, "a", null),
                        StructFieldPlan("b", INTEGER, null, null)))))

        val sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request)
        assertThat(sql).isEqualTo(
                "SELECT CASE WHEN \"s\" IS NULL THEN NULL ELSE struct_pack(\"child\" := " +
                        "CASE WHEN (\"s\").\"child\" IS NULL THEN NULL ELSE " +
                        "struct_pack(\"a\" := ((\"s\").\"child\").\"a\", \"b\" := CAST(NULL AS INTEGER)) END" +
                        ") END AS \"s\" FROM ducklake_in.main.t")
    }

    @Test
    fun testStructNotInReshapePlanIsProjectedPlain() {
        // A struct present in the file with an unchanged shape (no reshape plan) projects by name —
        // the common path stays byte-for-byte the plain projection, NOT a struct_pack rewrap.
        val structType = RowType.rowType(RowType.field("a", INTEGER))
        val request = DucklakeDuckDbExecutor.ExecutionRequest(
                DuckDbAttachTarget.LocalPath(Path.of("/tmp/x.db")),
                listOf(DucklakeColumnHandle(200L, "s", structType, true)),
                TupleDomain.all(),
                listOf(),
                null,
                mapOf(200L to "s"),
                emptyMap())

        val sql = DuckDbSelectSqlBuilder.buildSelectSql("ducklake_in.main.t", request)
        assertThat(sql).isEqualTo("SELECT \"s\" FROM ducklake_in.main.t")
    }

    companion object {
        private val NAME_COL: DucklakeColumnHandle =
                DucklakeColumnHandle(101L, "name", VARCHAR, true)

        /** A single-struct-column request whose file name equals the current name, with a reshape plan. */
        private fun reshapeRequest(
                column: DucklakeColumnHandle,
                plan: List<StructFieldPlan>): DucklakeDuckDbExecutor.ExecutionRequest =
                DucklakeDuckDbExecutor.ExecutionRequest(
                        DuckDbAttachTarget.LocalPath(Path.of("/tmp/x.db")),
                        listOf(column),
                        TupleDomain.all(),
                        listOf(),
                        null,
                        mapOf(column.columnId to column.columnName),
                        mapOf(column.columnId to plan))
    }
}
