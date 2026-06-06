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

import kotlin.math.ceil

/**
 * Optimistic-retry policy for catalog write transactions. Re-runs an attempt
 * up to `maxRetries` times on [TransactionConflictException],
 * sleeping with exponential backoff between attempts. After exhaustion, the
 * latest conflict is rewrapped with an "exceeded the maximum retry count"
 * message and rethrown.
 *
 * Extracted from `JdbcDucklakeCatalog.executeWriteTransaction` so
 * that the policy can be unit-tested without a database. The [Sleeper]
 * seam lets tests record intervals and skip real sleeps.
 *
 * Visibility note: [Sleeper] and [Attempt] are package-private in Java. Kotlin
 * has no package-private; widening to `public` preserves JVM call-compatibility
 * with the existing tests (test-only-effective: same-package access remains
 * identical from production code).
 */
object WriteTransactionRetry {
    private val log: System.Logger = System.getLogger(WriteTransactionRetry::class.java.name)

    fun interface Sleeper {
        @Throws(InterruptedException::class)
        fun sleep(millis: Long)
    }

    fun interface Attempt {
        fun run()
    }

    fun retryOnConflict(
        maxRetries: Int,
        initialWaitMs: Long,
        backoffMultiplier: Double,
        sleeper: Sleeper,
        operationDescription: String,
        attempt: Attempt,
    ) {
        var lastConflict: TransactionConflictException? = null
        var waitMs: Long = initialWaitMs
        var i = 0
        while (i <= maxRetries) {
            try {
                attempt.run()
                return
            }
            catch (e: TransactionConflictException) {
                // Non-retryable conflicts (e.g. logical conflicts whose stale payload
                // would feed the same broken references into another attempt) bail
                // immediately rather than burning the retry budget on a guaranteed-fail.
                if (!e.retryable()) {
                    throw e
                }
                lastConflict = e
                if (i == maxRetries) {
                    break
                }
                log.log(
                    System.Logger.Level.DEBUG,
                    "Retrying {0} after conflict (attempt {1}/{2}, waiting {3}ms): {4}",
                    operationDescription, i + 1, maxRetries, waitMs, e.message,
                )
                try {
                    sleeper.sleep(waitMs)
                }
                catch (ie: InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw RuntimeException(
                        "Interrupted while waiting to retry $operationDescription", ie,
                    )
                }
                waitMs = ceil(waitMs * backoffMultiplier).toLong()
            }
            i++
        }
        throw TransactionConflictException(
            "Failed to " + operationDescription + ": exceeded the maximum retry count of " +
                maxRetries + " due to concurrent commits. Last conflict: " +
                lastConflict!!.message,
            lastConflict,
        )
    }
}
