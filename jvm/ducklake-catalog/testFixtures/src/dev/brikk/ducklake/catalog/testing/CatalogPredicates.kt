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
package dev.brikk.ducklake.catalog.testing

import dev.brikk.ducklake.catalog.schema.tables.DucklakeTableTable
import org.jooq.Condition
import org.jooq.Field
import org.jooq.Table
import org.jooq.impl.DSL.name

/**
 * Composable [Condition] builders for the predicates that recur across catalog reads in
 * tests. Use these directly in `dsl.selectFrom(...).where(...)` chains rather than
 * inlining `.eq(...).and(...)` expressions — the named predicates document intent and
 * keep the active-row semantics defined in one place.
 */
object CatalogPredicates {
    /**
     * `end_snapshot IS NULL` — row represents the currently-live version (i.e. has not
     * been superseded by a later snapshot). Most catalog-introspection tests want this rather
     * than [activeAt].
     */
    @JvmStatic
    fun currentlyActive(endSnapshot: Field<Long>): Condition {
        return endSnapshot.isNull()
    }

    /**
     * `begin_snapshot <= snapshotId AND (end_snapshot IS NULL OR end_snapshot > snapshotId)`
     * — row is visible at snapshot `snapshotId`. Used by the runtime catalog for
     * point-in-time reads; tests typically need this only when querying the dynamic
     * `ducklake_inlined_data_<tableId>_<schemaVersion>` tables.
     */
    @JvmStatic
    fun activeAt(beginSnapshot: Field<Long>, endSnapshot: Field<Long>, snapshotId: Long): Condition {
        return beginSnapshot.le(snapshotId)
            .and(endSnapshot.isNull().or(endSnapshot.gt(snapshotId)))
    }

    /**
     * Convenience overload of [activeAt] that resolves the two
     * columns by name off `table`. Works on aliased generated tables and on dynamic
     * tables built via [org.jooq.impl.DSL.table].
     */
    @JvmStatic
    fun activeAt(table: Table<*>, snapshotId: Long): Condition {
        val begin: Field<Long>? = table.field(name("begin_snapshot"), Long::class.java)
        val end: Field<Long>? = table.field(name("end_snapshot"), Long::class.java)
        requireNotNull(begin) { "Table has no begin_snapshot column: " + table.name }
        requireNotNull(end) { "Table has no end_snapshot column: " + table.name }
        return activeAt(begin, end, snapshotId)
    }

    /**
     * `<tab>.table_name = ? AND <tab>.end_snapshot IS NULL` — the canonical "find the
     * currently-active row for a table by name" predicate. The caller supplies the (possibly
     * aliased) [DucklakeTableTable] so this composes with the standard
     * `var tab = DUCKLAKE_TABLE.as("tab")` pattern at call sites without leaking
     * unaliased column references into the rendered SQL.
     */
    @JvmStatic
    fun activeTableNamed(tab: DucklakeTableTable, tableName: String): Condition {
        return tab.TABLE_NAME.eq(tableName)
            .and(currentlyActive(tab.END_SNAPSHOT))
    }
}
