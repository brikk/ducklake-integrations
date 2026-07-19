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
package dev.brikk.duckbridge.trino.plugin

import io.trino.testing.QueryRunner
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * P1 smoke tests: prove the `duckbridge` connector attaches an embedded (`jdbc:duckdb:`)
 * DuckDB, round-trips the standard scalar types through a Trino [QueryRunner], and exposes the
 * base-jdbc `query` passthrough table function.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestDuckBridgeSmoke {
    private lateinit var queryRunner: QueryRunner

    @BeforeAll
    fun setUp() {
        queryRunner = DuckBridgeQueryRunner.create(DuckBridgeQueryRunner.freshDatabaseUrl())
        queryRunner.execute("CREATE SCHEMA ${DuckBridgeQueryRunner.CATALOG}.${DuckBridgeQueryRunner.SCHEMA}")
    }

    @AfterAll
    fun tearDown() {
        if (::queryRunner.isInitialized) {
            queryRunner.close()
        }
    }

    @Test
    fun showTables() {
        queryRunner.execute("CREATE TABLE show_tables_probe (id integer)")
        try {
            val result = queryRunner.execute("SHOW TABLES")
            val names = result.materializedRows.map { it.getField(0) as String }
            assertThat(names).contains("show_tables_probe")
        } finally {
            queryRunner.execute("DROP TABLE show_tables_probe")
        }
    }

    @Test
    fun scalarRoundTrip() {
        queryRunner.execute(
            """
            CREATE TABLE scalars (
                b boolean,
                i integer,
                l bigint,
                d double,
                dec decimal(18, 4),
                s varchar,
                dt date,
                ts timestamp(6)
            )
            """.trimIndent(),
        )
        try {
            queryRunner.execute(
                """
                INSERT INTO scalars VALUES (
                    true,
                    42,
                    9223372036854775807,
                    3.5,
                    DECIMAL '12345.6789',
                    'héllo wörld · 你好',
                    DATE '2024-02-29',
                    TIMESTAMP '2024-02-29 13:14:15.123456'
                )
                """.trimIndent(),
            )

            val row = queryRunner.execute("SELECT b, i, l, d, dec, s, dt, ts FROM scalars").materializedRows.single()
            assertThat(row.getField(0)).isEqualTo(true)
            assertThat(row.getField(1)).isEqualTo(42)
            assertThat(row.getField(2)).isEqualTo(9223372036854775807L)
            assertThat(row.getField(3)).isEqualTo(3.5)
            assertThat(row.getField(4).toString()).isEqualTo("12345.6789")
            assertThat(row.getField(5)).isEqualTo("héllo wörld · 你好")
            assertThat(row.getField(6).toString()).isEqualTo("2024-02-29")
            assertThat(row.getField(7).toString()).isEqualTo("2024-02-29T13:14:15.123456")

            val count = queryRunner.execute("SELECT count(*) FROM scalars").materializedRows.single()
            assertThat(count.getField(0)).isEqualTo(1L)
        } finally {
            queryRunner.execute("DROP TABLE scalars")
        }
    }

    @Test
    fun queryPassthroughTableFunction() {
        val result =
            queryRunner.execute(
                "SELECT * FROM TABLE(${DuckBridgeQueryRunner.CATALOG}.system.query(query => 'SELECT 42 AS answer'))",
            )
        val row = result.materializedRows.single()
        assertThat((row.getField(0) as Number).toLong()).isEqualTo(42L)
    }
}
