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

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.ThrowableAssert.ThrowingCallable
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger

/**
 * Unit tests for [WriteTransactionRetry.retryOnConflict]. Covers every
 * branch of the retry policy without touching a database: a recording Sleeper
 * captures intervals, and a scripted Attempt throws conflicts on demand.
 */
class TestWriteTransactionRetry {
    /**
     * Expected backoff schedule given (initialWait=100ms, multiplier=1.5x,
     * ceil after each multiplication). Pinned here so a regression in the
     * formula shows up as a clear test failure rather than drifting silently.
     */
    private val expectedBackoffSchedule: List<Long> =
        listOf(100L, 150L, 225L, 338L, 507L, 761L, 1142L, 1713L, 2570L, 3855L)

    @Test
    fun firstAttemptSucceeds_doesNotSleepOrRetry() {
        val sleeper = RecordingSleeper()
        val calls = AtomicInteger()

        WriteTransactionRetry.retryOnConflict(
            MAX_RETRIES, INITIAL_WAIT_MS, BACKOFF, sleeper, "create schema",
        ) { calls.incrementAndGet() }

        assertThat(calls.get()).isEqualTo(1)
        assertThat(sleeper.intervals()).isEmpty()
    }

    @Test
    fun succeedsAfterTwoConflicts_callsAttemptThreeTimes_sleepsTwice() {
        val sleeper = RecordingSleeper()
        val attempt = ScriptedAttempt(2)

        WriteTransactionRetry.retryOnConflict(
            MAX_RETRIES, INITIAL_WAIT_MS, BACKOFF, sleeper, "create schema", attempt,
        )

        assertThat(attempt.calls()).isEqualTo(3)
        assertThat(sleeper.intervals()).containsExactly(100L, 150L)
    }

    @Test
    fun exhaustsRetries_throwsTransactionConflictWithLastConflictAsCause() {
        val sleeper = RecordingSleeper()
        val conflicts = arrayOfNulls<TransactionConflictException>(MAX_RETRIES + 1)
        val callIndex = AtomicInteger()
        val attempt = WriteTransactionRetry.Attempt {
            val i = callIndex.getAndIncrement()
            val e = TransactionConflictException("conflict #$i", null)
            conflicts[i] = e
            throw e
        }

        assertThatThrownBy(
            ThrowingCallable {
                WriteTransactionRetry.retryOnConflict(
                    MAX_RETRIES, INITIAL_WAIT_MS, BACKOFF, sleeper, "create schema", attempt,
                )
            },
        )
            .isInstanceOf(TransactionConflictException::class.java)
            .hasMessageContaining("Failed to create schema")
            .hasMessageContaining("exceeded the maximum retry count of $MAX_RETRIES")
            .hasMessageContaining("conflict #$MAX_RETRIES")
            .hasCauseInstanceOf(TransactionConflictException::class.java)
            .satisfies({ thrown ->
                val cause: Throwable? = thrown.cause
                assertThat(cause).isSameAs(conflicts[MAX_RETRIES])
                assertThat(cause!!.message).isEqualTo("conflict #$MAX_RETRIES")
            })

        assertThat(callIndex.get()).isEqualTo(MAX_RETRIES + 1)
    }

    @Test
    fun exhaustsRetries_recordsExpectedBackoffSchedule() {
        val sleeper = RecordingSleeper()
        val alwaysConflicts = WriteTransactionRetry.Attempt {
            throw TransactionConflictException("nope", null)
        }

        assertThatThrownBy(
            ThrowingCallable {
                WriteTransactionRetry.retryOnConflict(
                    MAX_RETRIES, INITIAL_WAIT_MS, BACKOFF, sleeper, "create schema", alwaysConflicts,
                )
            },
        )
            .isInstanceOf(TransactionConflictException::class.java)

        assertThat(sleeper.intervals())
            .`as`("backoff schedule for 10 retries at 100ms × 1.5")
            .containsExactlyElementsOf(expectedBackoffSchedule)
    }

    @Test
    fun nonConflictRuntimeException_propagatesImmediatelyWithoutRetry() {
        val sleeper = RecordingSleeper()
        val original = IllegalStateException("syntax error")
        val calls = AtomicInteger()
        val attempt = WriteTransactionRetry.Attempt {
            calls.incrementAndGet()
            throw original
        }

        assertThatThrownBy(
            ThrowingCallable {
                WriteTransactionRetry.retryOnConflict(
                    MAX_RETRIES, INITIAL_WAIT_MS, BACKOFF, sleeper, "create schema", attempt,
                )
            },
        )
            .isSameAs(original)

        assertThat(calls.get()).isEqualTo(1)
        assertThat(sleeper.intervals()).isEmpty()
    }

    @Test
    fun interruptedDuringSleep_setsInterruptFlagAndPropagates() {
        val interruptingSleeper = WriteTransactionRetry.Sleeper { _ ->
            throw InterruptedException("simulated interrupt")
        }
        val alwaysConflicts = WriteTransactionRetry.Attempt {
            throw TransactionConflictException("conflict", null)
        }

        try {
            assertThatThrownBy(
                ThrowingCallable {
                    WriteTransactionRetry.retryOnConflict(
                        MAX_RETRIES, INITIAL_WAIT_MS, BACKOFF, interruptingSleeper,
                        "create schema", alwaysConflicts,
                    )
                },
            )
                .isInstanceOf(RuntimeException::class.java)
                .hasMessageContaining("Interrupted while waiting to retry create schema")
                .hasCauseInstanceOf(InterruptedException::class.java)

            assertThat(Thread.currentThread().isInterrupted)
                .`as`("thread interrupt flag must be preserved after InterruptedException")
                .isTrue()
        } finally {
            // Clear the flag so this test doesn't poison subsequent tests on the same thread.
            Thread.interrupted()
        }
    }

    private class RecordingSleeper : WriteTransactionRetry.Sleeper {
        private val intervals: MutableList<Long> = ArrayList()

        override fun sleep(millis: Long) {
            intervals.add(millis)
        }

        fun intervals(): List<Long> = intervals
    }

    private class ScriptedAttempt(
        private val conflictsBeforeSuccess: Int,
    ) : WriteTransactionRetry.Attempt {
        private var calls: Int = 0

        override fun run() {
            calls++
            if (calls <= conflictsBeforeSuccess) {
                throw TransactionConflictException("scripted conflict #$calls", null)
            }
        }

        fun calls(): Int = calls
    }

    companion object {
        private const val MAX_RETRIES: Int = 10
        private const val INITIAL_WAIT_MS: Long = 100
        private const val BACKOFF: Double = 1.5
    }
}
