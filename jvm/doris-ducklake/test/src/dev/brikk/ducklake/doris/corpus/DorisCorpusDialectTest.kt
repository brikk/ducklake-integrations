package dev.brikk.ducklake.doris.corpus

import org.junit.jupiter.api.Test

import org.assertj.core.api.Assertions.assertThat

/**
 * Pins the v1 `accepts()` gate of the (backend-axis-gated) corpus adapter:
 * conservative SELECT-only admission with a deny-list of DuckDB-isms. Every
 * rejection is an engine-SKIP in the runner's report, so false negatives are
 * cheap and false positives (admitting SQL Doris then fails on) are the only
 * real bug class — hence the bias in these pins.
 */
internal class DorisCorpusDialectTest {

    // ---- admission: plain SELECTs, however they're dressed ----

    @Test
    fun acceptsPlainSelects() {
        assertThat(DorisCorpusDialect.accepts("SELECT 1")).isTrue()
        assertThat(DorisCorpusDialect.accepts("select a, b from t where a > 3")).isTrue()
        assertThat(DorisCorpusDialect.accepts("  \n\tSELECT count(*) FROM s.t")).isTrue()
        assertThat(DorisCorpusDialect.accepts("(SELECT 1)")).isTrue()
    }

    @Test
    fun acceptsSelectsBehindLeadingComments() {
        assertThat(DorisCorpusDialect.accepts("-- comment\nSELECT 1")).isTrue()
        assertThat(DorisCorpusDialect.accepts("/* block */ SELECT 1")).isTrue()
        assertThat(DorisCorpusDialect.accepts("-- a\n-- b\nSELECT 1")).isTrue()
    }

    // ---- tier 1: only reads are mirrored (the oracle owns everything else) ----

    @Test
    fun rejectsNonSelectStatements() {
        assertThat(DorisCorpusDialect.accepts("INSERT INTO t VALUES (1)")).isFalse()
        assertThat(DorisCorpusDialect.accepts("UPDATE t SET a = 1")).isFalse()
        assertThat(DorisCorpusDialect.accepts("DELETE FROM t")).isFalse()
        assertThat(DorisCorpusDialect.accepts("CREATE TABLE t (a INT)")).isFalse()
        assertThat(DorisCorpusDialect.accepts("DROP TABLE t")).isFalse()
        assertThat(DorisCorpusDialect.accepts("PRAGMA version")).isFalse()
        assertThat(DorisCorpusDialect.accepts("WITH x AS (SELECT 1) SELECT * FROM x")).isFalse()
        assertThat(DorisCorpusDialect.accepts("")).isFalse()
        assertThat(DorisCorpusDialect.accepts("-- only a comment")).isFalse()
    }

    // ---- tier 2: DuckDB-only syntax Doris's parser refuses ----

    @Test
    fun rejectsDuckDbSyntaxShorthands() {
        assertThat(DorisCorpusDialect.accepts("SELECT 1::BIGINT")).isFalse()
        assertThat(DorisCorpusDialect.accepts("SELECT [1, 2, 3]")).isFalse()
        assertThat(DorisCorpusDialect.accepts("SELECT {'a': 1}")).isFalse()
        assertThat(DorisCorpusDialect.accepts("SELECT list_transform(l, x -> x + 1) FROM t")).isFalse()
    }

    // ---- tier 3: DuckDB-internal state ----

    @Test
    fun rejectsDuckDbCatalogAndFileFunctions() {
        assertThat(DorisCorpusDialect.accepts("SELECT * FROM duckdb_tables()")).isFalse()
        assertThat(DorisCorpusDialect.accepts("SELECT * FROM read_parquet('f.parquet')")).isFalse()
        assertThat(DorisCorpusDialect.accepts("SELECT * FROM range(10)")).isFalse()
        assertThat(DorisCorpusDialect.accepts("SELECT current_setting('threads')")).isFalse()
        assertThat(DorisCorpusDialect.accepts("SELECT * FROM ducklake_snapshots('lake')")).isFalse()
    }

    // ---- tier 4: families the v1 type surface can't compare ----

    @Test
    fun rejectsDegradedTypeFamilies() {
        assertThat(DorisCorpusDialect.accepts("SELECT INTERVAL 1 DAY")).isFalse()
        assertThat(DorisCorpusDialect.accepts("SELECT unnest(l) FROM t")).isFalse()
        assertThat(DorisCorpusDialect.accepts("SELECT t.* FROM t USING SAMPLE 10%")).isFalse()
    }

    // ---- deny-list precision: word boundaries, not substrings ----

    @Test
    fun denyListMatchesWholeWordsOnly() {
        // RANGE is denied; a column called "oranges" or "range_x" is not.
        assertThat(DorisCorpusDialect.accepts("SELECT oranges FROM t")).isTrue()
        assertThat(DorisCorpusDialect.accepts("SELECT range_x FROM t")).isTrue()
        assertThat(DorisCorpusDialect.accepts("SELECT * FROM range(5)")).isFalse()
        // INTERVAL denied even mid-query, any case.
        assertThat(DorisCorpusDialect.accepts("SELECT a + interval 1 day FROM t")).isFalse()
    }
}
