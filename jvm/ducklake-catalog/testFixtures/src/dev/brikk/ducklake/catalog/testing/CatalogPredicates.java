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
package dev.brikk.ducklake.catalog.testing;

import dev.brikk.ducklake.catalog.schema.tables.DucklakeTableTable;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;

import static dev.brikk.ducklake.catalog.schema.PublicDbTables.DUCKLAKE_TABLE;
import static java.util.Objects.requireNonNull;
import static org.jooq.impl.DSL.name;

/**
 * Composable {@link Condition} builders for the predicates that recur across catalog reads in
 * tests. Use these directly in {@code dsl.selectFrom(...).where(...)} chains rather than
 * inlining {@code .eq(...).and(...)} expressions — the named predicates document intent and
 * keep the active-row semantics defined in one place.
 */
public final class CatalogPredicates
{
    private CatalogPredicates() {}

    /**
     * {@code end_snapshot IS NULL} — row represents the currently-live version (i.e. has not
     * been superseded by a later snapshot). Most catalog-introspection tests want this rather
     * than {@link #activeAt}.
     */
    public static Condition currentlyActive(Field<Long> endSnapshot)
    {
        return requireNonNull(endSnapshot, "endSnapshot is null").isNull();
    }

    /**
     * {@code begin_snapshot <= snapshotId AND (end_snapshot IS NULL OR end_snapshot > snapshotId)}
     * — row is visible at snapshot {@code snapshotId}. Used by the runtime catalog for
     * point-in-time reads; tests typically need this only when querying the dynamic
     * {@code ducklake_inlined_data_<tableId>_<schemaVersion>} tables.
     */
    public static Condition activeAt(Field<Long> beginSnapshot, Field<Long> endSnapshot, long snapshotId)
    {
        requireNonNull(beginSnapshot, "beginSnapshot is null");
        requireNonNull(endSnapshot, "endSnapshot is null");
        return beginSnapshot.le(snapshotId)
                .and(endSnapshot.isNull().or(endSnapshot.gt(snapshotId)));
    }

    /**
     * Convenience overload of {@link #activeAt(Field, Field, long)} that resolves the two
     * columns by name off {@code table}. Works on aliased generated tables and on dynamic
     * tables built via {@link org.jooq.impl.DSL#table(org.jooq.Name)}.
     */
    public static Condition activeAt(Table<?> table, long snapshotId)
    {
        requireNonNull(table, "table is null");
        Field<Long> begin = table.field(name("begin_snapshot"), Long.class);
        Field<Long> end = table.field(name("end_snapshot"), Long.class);
        if (begin == null) {
            throw new IllegalArgumentException("Table has no begin_snapshot column: " + table.getName());
        }
        if (end == null) {
            throw new IllegalArgumentException("Table has no end_snapshot column: " + table.getName());
        }
        return activeAt(begin, end, snapshotId);
    }

    /**
     * {@code ducklake_table.table_name = ? AND ducklake_table.end_snapshot IS NULL} — the
     * canonical "find the currently-active row for a table by name" predicate. Combine with
     * {@link DucklakeTableTable#TABLE_ID} in a SELECT to get the table id.
     */
    public static Condition activeTableNamed(String tableName)
    {
        requireNonNull(tableName, "tableName is null");
        return DUCKLAKE_TABLE.TABLE_NAME.eq(tableName)
                .and(currentlyActive(DUCKLAKE_TABLE.END_SNAPSHOT));
    }
}
