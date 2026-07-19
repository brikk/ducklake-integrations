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

import io.airlift.units.DataSize
import io.airlift.units.DataSize.Unit.GIGABYTE
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Server-free unit tests for the T2 executor machinery: tuning-SQL rendering, catalog-write-conflict
 * classification, the S3 secret SQL, and the execution-engine enum. These need no DuckDB/Quack.
 */
class TestDuckBridgeExecutorUnit {
    @Test
    fun tuningSqlDefaultsRenderMandatorySettings() {
        val sql = DuckDbTuningSql.statements(DuckDbTuning.defaults())
        assertThat(sql).containsExactly(
            "SET http_keep_alive = true",
            "SET http_retries = 5",
            "SET enable_object_cache = true",
        )
    }

    @Test
    fun tuningSqlRendersMemoryAndThreads() {
        val tuning = DuckDbTuning(DataSize.of(4, GIGABYTE), 8, null, null, false)
        val sql = DuckDbTuningSql.statements(tuning)
        assertThat(sql).contains("SET enable_object_cache = false")
        assertThat(sql).anyMatch { it.startsWith("SET memory_limit = '") && it.contains("4") }
        assertThat(sql).contains("SET threads = 8")
    }

    @Test
    fun catalogWriteRetryClassifiesConflicts() {
        assertThat(
            DuckDbCatalogWriteRetry.isWriteWriteConflict(
                java.sql.SQLException("Catalog write-write conflict on create with \"duckbridge_s3\""),
            ),
        ).isTrue()
        assertThat(DuckDbCatalogWriteRetry.isWriteWriteConflict(java.sql.SQLException("syntax error"))).isFalse()
    }

    @Test
    fun catalogWriteRetryRetriesThenSucceeds() {
        var attempts = 0
        val result =
            DuckDbCatalogWriteRetry.retryConflicts("test") {
                attempts++
                if (attempts < 2) {
                    throw java.sql.SQLException("Catalog write-write conflict on create with \"x\"")
                }
                "ok"
            }
        assertThat(result).isEqualTo("ok")
        assertThat(attempts).isEqualTo(2)
    }

    @Test
    fun catalogWriteRetryPropagatesNonConflict() {
        assertThat(
            runCatching {
                DuckDbCatalogWriteRetry.retryConflicts("test") { throw java.sql.SQLException("boom") }
            }.exceptionOrNull(),
        ).isInstanceOf(java.sql.SQLException::class.java)
    }

    @Test
    fun s3ConfigRendersCreateSecretIfNotExists() {
        val s3 = DuckDbS3Config.fromCatalogConfig(
            mapOf(
                "s3.endpoint" to "http://minio:9000",
                "s3.region" to "us-east-1",
                "s3.aws-access-key" to "ak",
                "s3.aws-secret-key" to "sk",
                "s3.path-style-access" to "true",
            ),
        )
        val sql = s3.renderCreateSecretSql()
        assertThat(sql).startsWith("CREATE SECRET IF NOT EXISTS duckbridge_s3 (TYPE S3")
        assertThat(sql).contains("ENDPOINT 'minio:9000'") // scheme stripped
        assertThat(sql).contains("URL_STYLE 'path'")
        assertThat(sql).contains("USE_SSL false") // http endpoint → no ssl
    }

    @Test
    fun executionEngineIsArrowPageSourceFlag() {
        assertThat(DuckBridgeExecutionEngine.JDBC.isArrowPageSource).isFalse()
        assertThat(DuckBridgeExecutionEngine.DUCKDB_LOCAL.isArrowPageSource).isTrue()
        assertThat(DuckBridgeExecutionEngine.QUACK.isArrowPageSource).isTrue()
    }

    @Test
    fun quackExecutorRejectsShortToken() {
        assertThat(
            runCatching {
                QuackDuckBridgeExecutor("h", 9494, "abc", DuckDbTuning.defaults(), null)
            }.exceptionOrNull(),
        ).isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun quackExecutionEngineIsRejectedByConfigValidation() {
        // Fail loud, never a config that lies: the QUACK T2 engine is gated on the upstream Quack
        // pool rework; airlift bootstrap enforces this bean-validation property at startup.
        assertThat(DuckBridgeConfig().setExecutionEngine(DuckBridgeExecutionEngine.QUACK).isExecutionEngineOperational)
            .isFalse()
        assertThat(DuckBridgeConfig().setExecutionEngine(DuckBridgeExecutionEngine.DUCKDB_LOCAL).isExecutionEngineOperational)
            .isTrue()
        assertThat(DuckBridgeConfig().isExecutionEngineOperational).isTrue()
    }
}
