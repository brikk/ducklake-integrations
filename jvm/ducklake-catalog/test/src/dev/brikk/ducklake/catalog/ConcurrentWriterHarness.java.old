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
package dev.brikk.ducklake.catalog;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test fixture that interleaves two writers against the same catalog by parking
 * the "loser" attempt at {@link JdbcDucklakeCatalog#beforeWriteTransactionAction}
 * before any of its mutations run, then committing the "winner" on the main
 * thread, then releasing the loser whose first attempt fails the lineage check
 * and retries.
 *
 * <p>Determinism comes from latches, not sleeps. The hook only parks the loser's
 * <em>first</em> attempt — retries proceed unimpeded so the loser can actually
 * commit (or fail logically) on its second pass.
 *
 * <p>Usage:
 * <pre>{@code
 * ConcurrentWriterHarness.Result result = ConcurrentWriterHarness.runWinnerWhileLoserParked(
 *         catalog,
 *         () -> catalog.createSchema("winner_schema"),
 *         () -> catalog.createSchema("loser_schema"));
 * assertThat(result.loserAttemptCount()).isEqualTo(2);
 * assertThat(result.loserException()).isNull();
 * }</pre>
 */
final class ConcurrentWriterHarness
{
    static final String LOSER_THREAD_NAME = "concurrent-writer-harness-loser";
    static final long DEFAULT_PHASE_TIMEOUT_SECONDS = 10;

    private ConcurrentWriterHarness() {}

    record Result(int loserAttemptCount, Throwable loserException) {}

    static Result runWinnerWhileLoserParked(
            JdbcDucklakeCatalog catalog,
            Runnable winnerOp,
            Runnable loserOp)
            throws InterruptedException
    {
        return runWinnerWhileLoserParked(catalog, winnerOp, loserOp, DEFAULT_PHASE_TIMEOUT_SECONDS);
    }

    static Result runWinnerWhileLoserParked(
            JdbcDucklakeCatalog catalog,
            Runnable winnerOp,
            Runnable loserOp,
            long phaseTimeoutSeconds)
            throws InterruptedException
    {
        AtomicInteger loserHookCalls = new AtomicInteger();
        CountDownLatch loserPaused = new CountDownLatch(1);
        CountDownLatch winnerCommitted = new CountDownLatch(1);

        catalog.beforeWriteTransactionAction = () -> {
            if (!Thread.currentThread().getName().equals(LOSER_THREAD_NAME)) {
                return;
            }
            // Only park the first attempt; later retries proceed.
            if (loserHookCalls.getAndIncrement() != 0) {
                return;
            }
            loserPaused.countDown();
            try {
                if (!winnerCommitted.await(phaseTimeoutSeconds, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                            "winner did not signal commit within " + phaseTimeoutSeconds + "s");
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        };

        ExecutorService loserExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread t = new Thread(runnable, LOSER_THREAD_NAME);
            t.setDaemon(true);
            return t;
        });
        Throwable loserException = null;
        try {
            Future<?> loser = loserExecutor.submit(loserOp);

            if (!loserPaused.await(phaseTimeoutSeconds, TimeUnit.SECONDS)) {
                throw new AssertionError("loser thread did not reach the pre-write hook within "
                        + phaseTimeoutSeconds + "s");
            }

            // Winner commits on the main thread. The hook is a no-op here because the
            // thread name doesn't match.
            winnerOp.run();

            // Release the loser so its first attempt can resume, fail the lineage
            // check, and retry.
            winnerCommitted.countDown();

            try {
                loser.get(phaseTimeoutSeconds, TimeUnit.SECONDS);
            }
            catch (ExecutionException e) {
                loserException = e.getCause();
            }
            catch (java.util.concurrent.TimeoutException e) {
                throw new AssertionError("loser thread did not complete within "
                        + phaseTimeoutSeconds + "s", e);
            }
        }
        finally {
            loserExecutor.shutdownNow();
            catalog.beforeWriteTransactionAction = () -> {};
        }
        return new Result(loserHookCalls.get(), loserException);
    }
}
