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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link WriteTransactionRetry#retryOnConflict}. Covers every
 * branch of the retry policy without touching a database: a recording Sleeper
 * captures intervals, and a scripted Attempt throws conflicts on demand.
 */
public class TestWriteTransactionRetry
{
    private static final int MAX_RETRIES = 10;
    private static final long INITIAL_WAIT_MS = 100;
    private static final double BACKOFF = 1.5;

    /**
     * Expected backoff schedule given (initialWait=100ms, multiplier=1.5x,
     * ceil after each multiplication). Pinned here so a regression in the
     * formula shows up as a clear test failure rather than drifting silently.
     */
    private static final List<Long> EXPECTED_BACKOFF_SCHEDULE =
            List.of(100L, 150L, 225L, 338L, 507L, 761L, 1142L, 1713L, 2570L, 3855L);

    @Test
    public void firstAttemptSucceeds_doesNotSleepOrRetry()
    {
        RecordingSleeper sleeper = new RecordingSleeper();
        AtomicInteger calls = new AtomicInteger();

        WriteTransactionRetry.retryOnConflict(
                MAX_RETRIES, INITIAL_WAIT_MS, BACKOFF, sleeper, "create schema",
                calls::incrementAndGet);

        assertThat(calls.get()).isEqualTo(1);
        assertThat(sleeper.intervals()).isEmpty();
    }

    @Test
    public void succeedsAfterTwoConflicts_callsAttemptThreeTimes_sleepsTwice()
    {
        RecordingSleeper sleeper = new RecordingSleeper();
        ScriptedAttempt attempt = new ScriptedAttempt(2);

        WriteTransactionRetry.retryOnConflict(
                MAX_RETRIES, INITIAL_WAIT_MS, BACKOFF, sleeper, "create schema", attempt);

        assertThat(attempt.calls()).isEqualTo(3);
        assertThat(sleeper.intervals()).containsExactly(100L, 150L);
    }

    @Test
    public void exhaustsRetries_throwsTransactionConflictWithLastConflictAsCause()
    {
        RecordingSleeper sleeper = new RecordingSleeper();
        TransactionConflictException[] conflicts = new TransactionConflictException[MAX_RETRIES + 1];
        AtomicInteger callIndex = new AtomicInteger();
        Runnable attempt = () -> {
            int i = callIndex.getAndIncrement();
            TransactionConflictException e = new TransactionConflictException("conflict #" + i, null);
            conflicts[i] = e;
            throw e;
        };

        assertThatThrownBy(() -> WriteTransactionRetry.retryOnConflict(
                MAX_RETRIES, INITIAL_WAIT_MS, BACKOFF, sleeper, "create schema", attempt::run))
                .isInstanceOf(TransactionConflictException.class)
                .hasMessageContaining("Failed to create schema")
                .hasMessageContaining("exceeded the maximum retry count of " + MAX_RETRIES)
                .hasMessageContaining("conflict #" + MAX_RETRIES)
                .hasCauseInstanceOf(TransactionConflictException.class)
                .satisfies(thrown -> {
                    Throwable cause = thrown.getCause();
                    assertThat(cause).isSameAs(conflicts[MAX_RETRIES]);
                    assertThat(cause.getMessage()).isEqualTo("conflict #" + MAX_RETRIES);
                });

        assertThat(callIndex.get()).isEqualTo(MAX_RETRIES + 1);
    }

    @Test
    public void exhaustsRetries_recordsExpectedBackoffSchedule()
    {
        RecordingSleeper sleeper = new RecordingSleeper();
        Runnable alwaysConflicts = () -> {
            throw new TransactionConflictException("nope", null);
        };

        assertThatThrownBy(() -> WriteTransactionRetry.retryOnConflict(
                MAX_RETRIES, INITIAL_WAIT_MS, BACKOFF, sleeper, "create schema", alwaysConflicts::run))
                .isInstanceOf(TransactionConflictException.class);

        assertThat(sleeper.intervals())
                .as("backoff schedule for 10 retries at 100ms × 1.5")
                .containsExactlyElementsOf(EXPECTED_BACKOFF_SCHEDULE);
    }

    @Test
    public void nonConflictRuntimeException_propagatesImmediatelyWithoutRetry()
    {
        RecordingSleeper sleeper = new RecordingSleeper();
        IllegalStateException original = new IllegalStateException("syntax error");
        AtomicInteger calls = new AtomicInteger();
        Runnable attempt = () -> {
            calls.incrementAndGet();
            throw original;
        };

        assertThatThrownBy(() -> WriteTransactionRetry.retryOnConflict(
                MAX_RETRIES, INITIAL_WAIT_MS, BACKOFF, sleeper, "create schema", attempt::run))
                .isSameAs(original);

        assertThat(calls.get()).isEqualTo(1);
        assertThat(sleeper.intervals()).isEmpty();
    }

    @Test
    public void interruptedDuringSleep_setsInterruptFlagAndPropagates()
    {
        WriteTransactionRetry.Sleeper interruptingSleeper = millis -> {
            throw new InterruptedException("simulated interrupt");
        };
        Runnable alwaysConflicts = () -> {
            throw new TransactionConflictException("conflict", null);
        };

        try {
            assertThatThrownBy(() -> WriteTransactionRetry.retryOnConflict(
                    MAX_RETRIES, INITIAL_WAIT_MS, BACKOFF, interruptingSleeper,
                    "create schema", alwaysConflicts::run))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Interrupted while waiting to retry create schema")
                    .hasCauseInstanceOf(InterruptedException.class);

            assertThat(Thread.currentThread().isInterrupted())
                    .as("thread interrupt flag must be preserved after InterruptedException")
                    .isTrue();
        }
        finally {
            // Clear the flag so this test doesn't poison subsequent tests on the same thread.
            Thread.interrupted();
        }
    }

    private static final class RecordingSleeper
            implements WriteTransactionRetry.Sleeper
    {
        private final List<Long> intervals = new ArrayList<>();

        @Override
        public void sleep(long millis)
        {
            intervals.add(millis);
        }

        List<Long> intervals()
        {
            return intervals;
        }
    }

    private static final class ScriptedAttempt
            implements WriteTransactionRetry.Attempt
    {
        private final int conflictsBeforeSuccess;
        private int calls;

        ScriptedAttempt(int conflictsBeforeSuccess)
        {
            this.conflictsBeforeSuccess = conflictsBeforeSuccess;
        }

        @Override
        public void run()
        {
            calls++;
            if (calls <= conflictsBeforeSuccess) {
                throw new TransactionConflictException("scripted conflict #" + calls, null);
            }
        }

        int calls()
        {
            return calls;
        }
    }
}
