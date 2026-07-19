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

import io.airlift.log.Logger
import java.sql.SQLException
import java.util.concurrent.ThreadLocalRandom

/**
 * Bounded retry for DuckDB **catalog write-write conflicts** on statements that are
 * idempotent-converging — re-running them after a competing transaction committed reaches the same
 * end state (`CREATE SECRET IF NOT EXISTS`, `ATTACH IF NOT EXISTS`, `INSTALL`).
 *
 * Needed by [QuackDuckBridgeExecutor] only: the Quack server is ONE long-lived DuckDB instance shared
 * by every concurrent client session, and DuckDB aborts overlapping catalog writes with
 * `Catalog write-write conflict on create/alter with "..."` even when both transactions would write
 * identical content. The in-process executor opens a private DuckDB per split, so it never conflicts
 * and does not use this. Ported from the DuckLake connector.
 */
internal object DuckDbCatalogWriteRetry {
    private val log: Logger = Logger.get(DuckDbCatalogWriteRetry::class.java)

    internal const val MAX_ATTEMPTS: Int = 6
    private const val BASE_BACKOFF_MS: Long = 10L

    /**
     * Run [action], retrying up to [MAX_ATTEMPTS] times total while it throws a [SQLException]
     * classified as a write-write conflict by [isWriteWriteConflict].
     */
    @Throws(SQLException::class)
    fun <T> retryConflicts(description: String, action: () -> T): T {
        var attempt = 1
        while (true) {
            try {
                return action()
            } catch (e: SQLException) {
                if (!isWriteWriteConflict(e) || attempt >= MAX_ATTEMPTS) {
                    throw e
                }
                log.debug(
                    "Catalog write-write conflict on %s (attempt %s/%s), retrying: %s",
                    description,
                    attempt,
                    MAX_ATTEMPTS,
                    e.message?.lineSequence()?.firstOrNull(),
                )
                backoff(attempt, e)
                attempt++
            }
        }
    }

    /**
     * DuckDB surfaces concurrent catalog writes as `Catalog write-write conflict on create with
     * "..."`; through the Quack wrapper the text arrives embedded in the client statement's error.
     * Substring match is the only classification channel — the JDBC driver carries no SQLState for it.
     */
    fun isWriteWriteConflict(e: SQLException): Boolean =
        e.message?.contains("write-write conflict", ignoreCase = true) ?: false

    private fun backoff(attempt: Int, conflict: SQLException) {
        val sleepMs = BASE_BACKOFF_MS * attempt + ThreadLocalRandom.current().nextLong(BASE_BACKOFF_MS)
        try {
            Thread.sleep(sleepMs)
        } catch (@Suppress("SwallowedException") ie: InterruptedException) {
            Thread.currentThread().interrupt()
            // The split is being cancelled — stop retrying and surface the conflict.
            throw conflict
        }
    }
}
