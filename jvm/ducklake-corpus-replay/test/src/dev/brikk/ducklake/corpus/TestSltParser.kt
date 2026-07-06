package dev.brikk.ducklake.corpus

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TestSltParser {

    @Test
    fun `parses statement ok and error with expectation`() {
        val file =
            SltParser.parse(
                "t",
                """
                statement ok
                CREATE TABLE t(i INTEGER);

                statement error
                INSERT INTO nope VALUES (1);
                ----
                does not exist
                """.trimIndent(),
            )
        assertThat(file.records).hasSize(2)
        val ok = file.records[0] as SltStatement
        assertThat(ok.expectError).isFalse()
        assertThat(ok.sql).contains("CREATE TABLE")
        val err = file.records[1] as SltStatement
        assertThat(err.expectError).isTrue()
        assertThat(err.expectedError).isEqualTo("does not exist")
    }

    @Test
    fun `parses query with types sort mode connection and expected block`() {
        val file =
            SltParser.parse(
                "t",
                """
                query II rowsort con1
                SELECT i, j FROM t
                ----
                1	2
                3	4
                """.trimIndent(),
            )
        val q = file.records.single() as SltQuery
        assertThat(q.types).isEqualTo("II")
        assertThat(q.sortMode).isEqualTo(SortMode.ROWSORT)
        assertThat(q.connection).isEqualTo("con1")
        assertThat(q.expected).containsExactly("1\t2", "3\t4")
    }

    @Test
    fun `parses empty expected block as empty result`() {
        val file =
            SltParser.parse(
                "t",
                """
                query I
                SELECT i FROM t WHERE false
                ----

                statement ok
                SELECT 1
                """.trimIndent(),
            )
        val q = file.records[0] as SltQuery
        assertThat(q.expected).isEmpty()
        assertThat(file.records[1]).isInstanceOf(SltStatement::class.java)
    }

    @Test
    fun `parses loop and foreach with bodies`() {
        val file =
            SltParser.parse(
                "t",
                """
                loop i 0 3

                statement ok
                SELECT ${'$'}{i}

                endloop

                foreach v a b

                statement ok
                SELECT '${'$'}{v}'

                endloop
                """.trimIndent(),
            )
        val loop = file.records[0] as SltLoop
        assertThat(loop.values).containsExactly("0", "1", "2")
        assertThat(loop.body).hasSize(1)
        val foreach = file.records[1] as SltLoop
        assertThat(foreach.values).containsExactly("a", "b")
    }

    @Test
    fun `directives require and test-env`() {
        val file =
            SltParser.parse(
                "t",
                """
                require ducklake

                test-env DUCKLAKE_CONNECTION {TEST_DIR}/{UUID}.db
                """.trimIndent(),
            )
        assertThat((file.records[0] as SltRequire).requirement).isEqualTo("ducklake")
        val env = file.records[1] as SltTestEnv
        assertThat(env.name).isEqualTo("DUCKLAKE_CONNECTION")
        assertThat(env.value).isEqualTo("{TEST_DIR}/{UUID}.db")
    }

    @Test
    fun `unknown constructs surface as unsupported`() {
        val file =
            SltParser.parse(
                "t",
                """
                concurrentloop i 0 10

                statement ok
                SELECT 1

                endloop
                """.trimIndent(),
            )
        assertThat(file.records.filterIsInstance<SltUnsupported>()).isNotEmpty()
    }

    @Test
    fun `skipif guards the next record`() {
        val file =
            SltParser.parse(
                "t",
                """
                skipif duckdb
                query I
                SELECT 1
                ----
                1
                """.trimIndent(),
            )
        val cond = file.records.single() as SltConditional
        assertThat(cond.skipIf).isTrue()
        assertThat(cond.engine).isEqualTo("duckdb")
        assertThat(cond.record).isInstanceOf(SltQuery::class.java)
    }
}
