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
 * A [TransactionConflictException] surfaced by [LogicalConflictCheck]:
 * a concurrent commit removed (or otherwise mutated) a catalog entity that
 * the in-flight transaction's payload references. Examples:
 *
 *  * `commitInsert` fragments name a `column_id` that an
 *    intervening `DROP COLUMN` end-snapshotted.
 *  * `commitDelete` fragments target a `data_file_id` that
 *    an intervening `DROP TABLE` or compaction end-snapshotted.
 *  * `addColumn` / `dropColumn` / `commitInsert` on a
 *    `table_id` that an intervening `DROP TABLE`
 *    end-snapshotted.
 *
 * This conflict is *not retryable*: the action's per-call arguments
 * (table IDs, fragment column / file IDs) are captured before the catalog
 * call and would feed the same stale references into a retry. The
 * [WriteTransactionRetry] loop bails out on these and rethrows
 * immediately.
 */
open class LogicalConflictException
        : TransactionConflictException
{
    constructor(message: String) : super(message, null)

    constructor(message: String, cause: Throwable?) : super(message, cause)

    override fun retryable(): Boolean
    {
        return false
    }
}
