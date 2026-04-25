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

/**
 * Optimistic-retry policy for catalog write transactions. Re-runs an attempt
 * up to {@code maxRetries} times on {@link TransactionConflictException},
 * sleeping with exponential backoff between attempts. After exhaustion, the
 * latest conflict is rewrapped with an "exceeded the maximum retry count"
 * message and rethrown.
 *
 * <p>Extracted from {@code JdbcDucklakeCatalog.executeWriteTransaction} so
 * that the policy can be unit-tested without a database. The {@link Sleeper}
 * seam lets tests record intervals and skip real sleeps.
 */
final class WriteTransactionRetry
{
    private static final System.Logger log = System.getLogger(WriteTransactionRetry.class.getName());

    @FunctionalInterface
    interface Sleeper
    {
        void sleep(long millis)
                throws InterruptedException;
    }

    @FunctionalInterface
    interface Attempt
    {
        void run();
    }

    private WriteTransactionRetry() {}

    static void retryOnConflict(
            int maxRetries,
            long initialWaitMs,
            double backoffMultiplier,
            Sleeper sleeper,
            String operationDescription,
            Attempt attempt)
    {
        TransactionConflictException lastConflict = null;
        long waitMs = initialWaitMs;
        for (int i = 0; i <= maxRetries; i++) {
            try {
                attempt.run();
                return;
            }
            catch (TransactionConflictException e) {
                lastConflict = e;
                if (i == maxRetries) {
                    break;
                }
                log.log(System.Logger.Level.DEBUG,
                        "Retrying {0} after conflict (attempt {1}/{2}, waiting {3}ms): {4}",
                        operationDescription, i + 1, maxRetries, waitMs, e.getMessage());
                try {
                    sleeper.sleep(waitMs);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(
                            "Interrupted while waiting to retry " + operationDescription, ie);
                }
                waitMs = (long) Math.ceil(waitMs * backoffMultiplier);
            }
        }
        throw new TransactionConflictException(
                "Failed to " + operationDescription + ": exceeded the maximum retry count of "
                        + maxRetries + " due to concurrent commits. Last conflict: "
                        + lastConflict.getMessage(),
                lastConflict);
    }
}
