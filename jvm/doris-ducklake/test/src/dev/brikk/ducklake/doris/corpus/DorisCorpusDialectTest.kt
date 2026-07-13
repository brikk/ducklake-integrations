package dev.brikk.ducklake.doris.corpus

import dev.brikk.ducklake.doris.corpus.DorisCorpusDialect.Run
import dev.brikk.ducklake.doris.corpus.DorisCorpusDialect.Skip
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Pins the transpile-first [DorisCorpusDialect.gate]: brikk-sql translates the
 * DuckDB corpus query to Doris and the gate runs it only when the transpiler
 * can do so faithfully, else engine-skips with a specific reason. These tests
 * exercise the DECISION + the emitted Doris SQL (not the live cluster).
 */
internal class DorisCorpusDialectTest {

    private fun run(sql: String): Run =
        DorisCorpusDialect.gate(sql).let {
            assertThat(it).`as`("expected Run for: $sql").isInstanceOf(Run::class.java)
            it as Run
        }

    private fun skip(sql: String): Skip =
        DorisCorpusDialect.gate(sql).let {
            assertThat(it).`as`("expected Skip for: $sql").isInstanceOf(Skip::class.java)
            it as Skip
        }

    // ---- Runs: transpiled to Doris ----

    @Test
    fun transpilesPlainSelect() {
        assertThat(run("SELECT a, b FROM lake.s.t WHERE a > 3").dorisSql)
            .contains("SELECT").contains("lake.s.t")
    }

    @Test
    fun rewritesCastShorthand() {
        // `::` is not Doris syntax; the transpiler must emit CAST(...).
        assertThat(run("SELECT o_orderkey::BIGINT FROM lake.s.orders").dorisSql)
            .contains("CAST(o_orderkey AS BIGINT)")
            .doesNotContain("::")
    }

    @Test
    fun rewritesFilterWhereToCase() {
        // Doris has no `agg(...) FILTER (WHERE …)` — must become CASE.
        val sql = run("SELECT COUNT(*) FILTER (WHERE id % 2 = 0) FROM lake.s.t").dorisSql
        assertThat(sql).containsIgnoringCase("CASE WHEN").doesNotContainIgnoringCase("FILTER")
    }

    @Test
    fun acceptsCtes() {
        assertThat(run("WITH x AS (SELECT a FROM lake.s.t) SELECT * FROM x").dorisSql)
            .containsIgnoringCase("WITH")
    }

    @Test
    fun rewritesLiteralInlineTimeTravel() {
        // brikk-sql passes `AT (VERSION => n)` through; the gate maps the literal
        // form to Doris FOR VERSION AS OF n.
        assertThat(run("SELECT * FROM lake.s.t AT (VERSION => 2)").dorisSql)
            .contains("FOR VERSION AS OF 2")
            .doesNotContainIgnoringCase("AT (")
    }

    @Test
    fun dropsTrailingOrderByAll() {
        // The mirror compares sorted rows, so a trailing ORDER BY ALL is dropped
        // (brikk-sql would otherwise mis-parse ALL as a column).
        val sql = run("SELECT a FROM lake.s.t ORDER BY ALL").dorisSql
        assertThat(sql).doesNotContainIgnoringCase("ORDER BY")
    }

    // ---- Skips: transpiler signals / residual divergences ----

    @Test
    fun skipsNonReads() {
        assertThat(skip("INSERT INTO lake.s.t VALUES (1)").reason).contains("read")
        assertThat(skip("DROP TABLE lake.s.t").reason).contains("read")
    }

    @Test
    fun skipsClass3FunctionHoles() {
        // Functions absent from Doris's catalog → certify UNMAPPABLE_FUNCTION.
        assertThat(skip("SELECT * FROM read_parquet('f.parquet')").reason)
            .contains("UNMAPPABLE_FUNCTION")
        assertThat(skip("SELECT * FROM ducklake_snapshots('lake')").reason)
            .contains("UNMAPPABLE_FUNCTION")
    }

    @Test
    fun skipsPragmaPassthrough() {
        assertThat(skip("PRAGMA database_size").reason).containsIgnoringCase("read")
        // (PRAGMA isn't a SELECT/WITH, so it's rejected at the read gate before transpile.)
    }

    @Test
    fun skipsScalarUnnestViaUnsupportedTranslation() {
        // certify REFUSAL: scalar UNNEST/EXPLODE has no Doris equivalent.
        assertThat(skip("SELECT unnest([1, 2, 3])").reason).contains("UNSUPPORTED_TRANSLATION")
    }

    @Test
    fun skipsInformationSchemaContentDivergence() {
        assertThat(skip("SELECT * FROM information_schema.columns").reason)
            .contains("information_schema")
    }

    @Test
    fun skipsNonLiteralInlineTimeTravel() {
        assertThat(skip("SELECT * FROM lake.s.t AT (TIMESTAMP => now())").reason)
            .contains("time travel")
    }

    @Test
    fun skipsOrderByAllGoverningLimit() {
        // Dropping ORDER BY ALL before a LIMIT would change which rows survive.
        assertThat(skip("SELECT a FROM lake.s.t ORDER BY ALL LIMIT 5").reason)
            .contains("LIMIT")
    }
}
