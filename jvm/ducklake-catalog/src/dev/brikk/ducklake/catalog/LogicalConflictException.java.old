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
 * A {@link TransactionConflictException} surfaced by {@link LogicalConflictCheck}:
 * a concurrent commit removed (or otherwise mutated) a catalog entity that
 * the in-flight transaction's payload references. Examples:
 *
 * <ul>
 *   <li>{@code commitInsert} fragments name a {@code column_id} that an
 *       intervening {@code DROP COLUMN} end-snapshotted.</li>
 *   <li>{@code commitDelete} fragments target a {@code data_file_id} that
 *       an intervening {@code DROP TABLE} or compaction end-snapshotted.</li>
 *   <li>{@code addColumn} / {@code dropColumn} / {@code commitInsert} on a
 *       {@code table_id} that an intervening {@code DROP TABLE}
 *       end-snapshotted.</li>
 * </ul>
 *
 * <p>This conflict is <em>not retryable</em>: the action's per-call arguments
 * (table IDs, fragment column / file IDs) are captured before the catalog
 * call and would feed the same stale references into a retry. The
 * {@link WriteTransactionRetry} loop bails out on these and rethrows
 * immediately.
 */
public class LogicalConflictException
        extends TransactionConflictException
{
    public LogicalConflictException(String message)
    {
        super(message, null);
    }

    public LogicalConflictException(String message, Throwable cause)
    {
        super(message, cause);
    }

    @Override
    public boolean retryable()
    {
        return false;
    }
}
