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

/**
 * Thrown when a catalog operation fails due to a concurrent commit
 * (optimistic concurrency conflict on the snapshot sequence).
 *
 * By default these are *retryable* — the optimistic-retry policy
 * in [WriteTransactionRetry] will re-run the transaction. Subclasses
 * can override [retryable] to mark the conflict terminal (e.g.
 * the in-flight payload references catalog entities a concurrent commit
 * already removed; re-running with the same payload would fail
 * identically).
 */
open class TransactionConflictException(
    message: String,
    cause: Throwable?,
) : DucklakeException(message, cause) {
    /**
     * Whether the optimistic-retry policy should re-run the operation.
     * Defaults to `true`; non-retryable subclasses override.
     */
    open fun retryable(): Boolean {
        return true
    }
}
