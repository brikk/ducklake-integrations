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
package dev.brikk.ducklake.catalog

import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

/**
 * Test fixture that interleaves two writers against the same catalog by parking
 * the "loser" attempt at [JdbcDucklakeCatalog.beforeWriteTransactionAction]
 * before any of its mutations run, then committing the "winner" on the main
 * thread, then releasing the loser whose first attempt fails the lineage check
 * and retries.
 *
 * Determinism comes from latches, not sleeps. The hook only parks the loser's
 * *first* attempt — retries proceed unimpeded so the loser can actually
 * commit (or fail logically) on its second pass.
 *
 * Usage:
 * ```
 * ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
 *         catalog,
 *         () -> catalog.createSchema("winner_schema"),
 *         () -> catalog.createSchema("loser_schema"));
 * assertThat(result.loserAttemptCount()).isEqualTo(2);
 * assertThat(result.loserException()).isNull();
 * ```
 */
object ConcurrentWriterHarness {
    @JvmField
    val LOSER_THREAD_NAME: String = "concurrent-writer-harness-loser"

    const val DEFAULT_PHASE_TIMEOUT_SECONDS: Long = 10

    @JvmRecord
    data class Result(val loserAttemptCount: Int, val loserException: Throwable?)

    @JvmStatic
    @JvmOverloads
    @Throws(InterruptedException::class)
    fun runWinnerWhileLoserParked(
        catalog: JdbcDucklakeCatalog,
        winnerOp: Runnable,
        loserOp: Runnable,
        phaseTimeoutSeconds: Long = DEFAULT_PHASE_TIMEOUT_SECONDS,
    ): Result {
        val loserHookCalls = AtomicInteger()
        val loserPaused = CountDownLatch(1)
        val winnerCommitted = CountDownLatch(1)

        catalog.beforeWriteTransactionAction = Runnable {
            if (Thread.currentThread().name != LOSER_THREAD_NAME) {
                return@Runnable
            }
            // Only park the first attempt; later retries proceed.
            if (loserHookCalls.getAndIncrement() != 0) {
                return@Runnable
            }
            loserPaused.countDown()
            try {
                if (!winnerCommitted.await(phaseTimeoutSeconds, TimeUnit.SECONDS)) {
                    throw IllegalStateException(
                        "winner did not signal commit within ${phaseTimeoutSeconds}s",
                    )
                }
            }
            catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                throw RuntimeException(e)
            }
        }

        val loserExecutor = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, LOSER_THREAD_NAME).apply { isDaemon = true }
        }
        var loserException: Throwable? = null
        try {
            val loser = loserExecutor.submit(loserOp)

            if (!loserPaused.await(phaseTimeoutSeconds, TimeUnit.SECONDS)) {
                throw AssertionError(
                    "loser thread did not reach the pre-write hook within ${phaseTimeoutSeconds}s",
                )
            }

            // Winner commits on the main thread. The hook is a no-op here because the
            // thread name doesn't match.
            winnerOp.run()

            // Release the loser so its first attempt can resume, fail the lineage
            // check, and retry.
            winnerCommitted.countDown()

            try {
                loser.get(phaseTimeoutSeconds, TimeUnit.SECONDS)
            }
            catch (e: ExecutionException) {
                loserException = e.cause
            }
            catch (e: TimeoutException) {
                throw AssertionError(
                    "loser thread did not complete within ${phaseTimeoutSeconds}s",
                    e,
                )
            }
        }
        finally {
            loserExecutor.shutdownNow()
            catalog.beforeWriteTransactionAction = Runnable {}
        }
        return Result(loserHookCalls.get(), loserException)
    }
}
