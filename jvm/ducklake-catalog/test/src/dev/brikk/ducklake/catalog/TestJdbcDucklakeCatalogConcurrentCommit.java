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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test proving that the catalog's optimistic-retry loop actually
 * re-runs the transaction against a real Postgres-backed catalog when a
 * competing commit advances the snapshot lineage between this transaction's
 * read and its INSERT.
 *
 * <p>Determinism relies on the {@code beforeWriteTransactionAction} test seam in
 * {@link JdbcDucklakeCatalog}: the loser thread parks inside the hook on its
 * first attempt, the winner thread commits on the main thread, then a latch
 * releases the loser whose first attempt fails the lineage check and retries
 * to a successful commit. No sleep-based timing — the conflict is forced by
 * the latch, not raced.
 */
public class TestJdbcDucklakeCatalogConcurrentCommit
{
    private static final String LOSER_THREAD_NAME = "concurrent-commit-loser";
    private static final long PHASE_TIMEOUT_SECONDS = 10;

    private static TestingDucklakePostgreSqlCatalogServer server;
    private static JdbcDucklakeCatalog catalog;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        server = new TestingDucklakePostgreSqlCatalogServer();
        JdbcDucklakeCatalogTestDataGenerator.IsolatedCatalog isolated =
                JdbcDucklakeCatalogTestDataGenerator.generateIsolatedCatalog(server, "concurrent-commit");

        DucklakeCatalogConfig config = new DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(isolated.jdbcUrl())
                .setCatalogDatabaseUser(isolated.user())
                .setCatalogDatabasePassword(isolated.password())
                .setDataPath(isolated.dataDir().toAbsolutePath().toString())
                .setMaxCatalogConnections(5);
        catalog = new JdbcDucklakeCatalog(config);
    }

    @AfterAll
    public static void tearDownClass()
    {
        if (catalog != null) {
            catalog.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    public void concurrentCommitTriggersRetryAndBothSchemasLand()
            throws Exception
    {
        AtomicInteger loserHookCalls = new AtomicInteger();
        CountDownLatch loserPaused = new CountDownLatch(1);
        CountDownLatch winnerCommitted = new CountDownLatch(1);

        catalog.beforeWriteTransactionAction = () -> {
            if (!Thread.currentThread().getName().equals(LOSER_THREAD_NAME)) {
                return;
            }
            // Only block on the first attempt; retries proceed unimpeded so the
            // loser can actually commit after the winner.
            if (loserHookCalls.getAndIncrement() != 0) {
                return;
            }
            loserPaused.countDown();
            try {
                if (!winnerCommitted.await(PHASE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                            "winner did not signal commit within " + PHASE_TIMEOUT_SECONDS + "s");
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
        try {
            Future<?> loser = loserExecutor.submit(() -> catalog.createSchema("loser_schema"));

            assertThat(loserPaused.await(PHASE_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .as("loser thread must reach the pre-insert hook")
                    .isTrue();

            // Winner commits on the main thread. Hook is a no-op here because the
            // thread name doesn't match.
            catalog.createSchema("winner_schema");

            // Release the loser. Its first attempt's lineage check now fails (winner
            // advanced max(snapshot_id)); retry-with-backoff re-reads, re-inserts,
            // and commits.
            winnerCommitted.countDown();
            loser.get(PHASE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        finally {
            loserExecutor.shutdownNow();
            catalog.beforeWriteTransactionAction = () -> {};
        }

        long latestSnapshot = catalog.getCurrentSnapshotId();
        assertThat(catalog.getSchema("winner_schema", latestSnapshot))
                .as("winner_schema must be present at latest snapshot")
                .isPresent();
        assertThat(catalog.getSchema("loser_schema", latestSnapshot))
                .as("loser_schema must be present at latest snapshot (proves retry committed)")
                .isPresent();
        assertThat(loserHookCalls.get())
                .as("loser must have hit the hook twice: first attempt (paused) + retry (no-op)")
                .isEqualTo(2);
    }
}
