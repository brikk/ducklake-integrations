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

import io.trino.spi.StandardErrorCode
import io.trino.spi.TrinoException
import io.trino.spi.predicate.TupleDomain
import io.trino.spi.type.Type
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.sql.SQLException
import java.util.Optional

/**
 * Unit tests for [DuckDbFilePageSource] error classification. A transient IO/engine
 * failure on the read path must surface as GENERIC_INTERNAL_ERROR, not NOT_SUPPORTED —
 * the latter is a USER_ERROR reserved for genuine type/feature gaps (which the
 * Arrow→Page converter raises separately) and misclassifying read failures as it
 * misleads operators and defeats error-type-keyed retry classification.
 */
class TestDuckDbFilePageSource {
    @Test
    fun testReadFailureIsGenericInternalErrorNotNotSupported() {
        val executor = object : DucklakeDuckDbExecutor {
            override fun execute(request: DucklakeDuckDbExecutor.ExecutionRequest): DucklakeDuckDbExecutor.ExecutionContext {
                throw SQLException("S3 httpfs read failed: connection reset")
            }
        }
        val pageSource = DuckDbFilePageSource(
                executor,
                DuckDbAttachTarget.LocalPath(Path.of("/tmp/unused.db")),
                emptyList(),
                emptyList<Type>(),
                TupleDomain.all(),
                emptyList(),
                Optional.empty())

        assertThatThrownBy { pageSource.getNextSourcePage() }
                .isInstanceOfSatisfying(TrinoException::class.java) { e ->
                    assertThat(e.errorCode).isEqualTo(StandardErrorCode.GENERIC_INTERNAL_ERROR.toErrorCode())
                    assertThat(e.cause).isInstanceOf(SQLException::class.java)
                }
    }
}
