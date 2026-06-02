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

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;

import static java.util.Objects.requireNonNull;
import static org.jooq.impl.DSL.name;

/**
 * Builds the "row is visible at snapshot N" predicate that appears in the
 * vast majority of catalog reads:
 *
 * <pre>{@code
 * begin_snapshot <= N AND (end_snapshot IS NULL OR end_snapshot > N)
 * }</pre>
 *
 * The {@link #activeAt(Table, long)} overload resolves the two columns by
 * name off the generated table, which also works on aliased tables. The
 * explicit-field overload is for dynamic tables (e.g.
 * {@code ducklake_inlined_data_{tableId}_{schemaVersion}}) that don't exist
 * at codegen time.
 */
final class SnapshotRange
{
    private SnapshotRange() {}

    static Condition activeAt(Table<?> table, long snapshotId)
    {
        Field<Long> begin = table.field(name("begin_snapshot"), Long.class);
        Field<Long> end = table.field(name("end_snapshot"), Long.class);
        requireNonNull(begin, "Table has no begin_snapshot column: " + table.getName());
        requireNonNull(end, "Table has no end_snapshot column: " + table.getName());
        return activeAt(begin, end, snapshotId);
    }

    static Condition activeAt(Field<Long> beginSnapshot, Field<Long> endSnapshot, long snapshotId)
    {
        return beginSnapshot.le(snapshotId)
                .and(endSnapshot.isNull().or(endSnapshot.gt(snapshotId)));
    }
}
