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

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.sql.SQLException

/**
 * Retry policy of [DuckDbCatalogWriteRetry] in isolation (the live conflict is exercised
 * end-to-end by `TestDucklakeQuackS3InitRace`): conflicts retry until success, non-conflicts
 * propagate immediately, and exhaustion rethrows the last conflict.
 */
class TestDuckDbCatalogWriteRetry {

    // The shapes DuckDB 1.5.3 emits for overlapping catalog writes, as surfaced through the
    // Quack wrapper (conflict text embedded in the client statement's error).
    private val createConflict = SQLException("Invalid Input Error: Attempting to execute an "
            + "unsuccessful or closed pending query result\n"
            + "Error: Invalid Input Error: Catalog write-write conflict on create with \"ducklake_s3\"")
    private val alterConflict = SQLException(
            "Invalid Input Error: Catalog write-write conflict on alter with \"ducklake_s3\"")

    @Test
    fun retriesConflictsUntilSuccess() {
        var calls = 0
        val result = DuckDbCatalogWriteRetry.retryConflicts("CREATE SECRET") {
            calls++
            if (calls < 3) {
                throw if (calls == 1) createConflict else alterConflict
            }
            "done"
        }
        assertThat(result).isEqualTo("done")
        assertThat(calls).isEqualTo(3)
    }

    @Test
    fun nonConflictErrorPropagatesWithoutRetry() {
        var calls = 0
        val boom = SQLException("IO Error: Could not establish connection")
        assertThatThrownBy {
            DuckDbCatalogWriteRetry.retryConflicts<Unit>("ATTACH") {
                calls++
                throw boom
            }
        }.isSameAs(boom)
        assertThat(calls).isEqualTo(1)
    }

    @Test
    fun exhaustionRethrowsLastConflict() {
        var calls = 0
        assertThatThrownBy {
            DuckDbCatalogWriteRetry.retryConflicts<Unit>("CREATE SECRET") {
                calls++
                throw createConflict
            }
        }.isSameAs(createConflict)
        assertThat(calls).isEqualTo(DuckDbCatalogWriteRetry.MAX_ATTEMPTS)
    }

    @Test
    fun classifiesOnlyWriteWriteConflicts() {
        assertThat(DuckDbCatalogWriteRetry.isWriteWriteConflict(createConflict)).isTrue()
        assertThat(DuckDbCatalogWriteRetry.isWriteWriteConflict(alterConflict)).isTrue()
        assertThat(DuckDbCatalogWriteRetry.isWriteWriteConflict(
                SQLException("Catalog Error: Secret with name \"ducklake_s3\" already exists"))).isFalse()
        assertThat(DuckDbCatalogWriteRetry.isWriteWriteConflict(SQLException(null as String?))).isFalse()
    }
}
