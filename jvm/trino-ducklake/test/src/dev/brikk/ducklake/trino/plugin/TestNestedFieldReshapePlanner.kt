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
package dev.brikk.ducklake.trino.plugin

import dev.brikk.ducklake.catalog.DucklakeColumn
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.RowType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Pure tests of the current-vs-file column-tree merge that produces struct reshape plans. Matching
 * is by `column_id`, so add / drop / rename / reorder of struct subfields are all detected, and an
 * identical struct produces no plan (the builder then projects it plainly).
 */
class TestNestedFieldReshapePlanner {

    @Test
    fun addedSubfieldGetsNullFilePlan() {
        // current s = row(a, b) [a=201, b=202]; file had only a — b added since.
        val plans = NestedFieldReshapePlanner.buildPlans(
                listOf(structHandle(200, "s", "a" to INTEGER, "b" to INTEGER)),
                listOf(struct(200, "s"), child(201, "a", 200, 0), child(202, "b", 200, 1)),
                listOf(struct(200, "s"), child(201, "a", 200, 0)))

        assertThat(plans).containsOnlyKeys(200L)
        assertThat(plans[200L]).containsExactly(
                StructFieldPlan("a", INTEGER, "a", null),
                StructFieldPlan("b", INTEGER, null, null))
    }

    @Test
    fun droppedNonTrailingSubfieldOmittedAndReshapeForced() {
        // current s = row(a, c) [201, 203]; file had row(a, b, c) [201, 202, 203] — b dropped.
        val plans = NestedFieldReshapePlanner.buildPlans(
                listOf(structHandle(200, "s", "a" to INTEGER, "c" to INTEGER)),
                listOf(struct(200, "s"), child(201, "a", 200, 0), child(203, "c", 200, 2)),
                listOf(struct(200, "s"), child(201, "a", 200, 0), child(202, "b", 200, 1), child(203, "c", 200, 2)))

        assertThat(plans[200L]).containsExactly(
                StructFieldPlan("a", INTEGER, "a", null),
                StructFieldPlan("c", INTEGER, "c", null))
    }

    @Test
    fun renamedSubfieldMapsCurrentNameToFileName() {
        // current a (id 201); file stored it as old_a.
        val plans = NestedFieldReshapePlanner.buildPlans(
                listOf(structHandle(200, "s", "a" to INTEGER)),
                listOf(struct(200, "s"), child(201, "a", 200, 0)),
                listOf(struct(200, "s"), child(201, "old_a", 200, 0)))

        assertThat(plans[200L]).containsExactly(StructFieldPlan("a", INTEGER, "old_a", null))
    }

    @Test
    fun nestedStructDriftReshapesRecursively() {
        // current s = row(child row(a, b)); file = row(child row(a)). child=210, a=211, b=212.
        val inner = RowType.rowType(RowType.field("a", INTEGER), RowType.field("b", INTEGER))
        val plans = NestedFieldReshapePlanner.buildPlans(
                listOf(DucklakeColumnHandle(200, "s", RowType.rowType(RowType.field("child", inner)), true)),
                listOf(struct(200, "s"), child(210, "child", 200, 0), child(211, "a", 210, 0), child(212, "b", 210, 1)),
                listOf(struct(200, "s"), child(210, "child", 200, 0), child(211, "a", 210, 0)))

        assertThat(plans[200L]).containsExactly(
                StructFieldPlan("child", inner, "child", listOf(
                        StructFieldPlan("a", INTEGER, "a", null),
                        StructFieldPlan("b", INTEGER, null, null))))
    }

    @Test
    fun identicalStructProducesNoPlan() {
        val plans = NestedFieldReshapePlanner.buildPlans(
                listOf(structHandle(200, "s", "a" to INTEGER, "b" to INTEGER)),
                listOf(struct(200, "s"), child(201, "a", 200, 0), child(202, "b", 200, 1)),
                listOf(struct(200, "s"), child(201, "a", 200, 0), child(202, "b", 200, 1)))

        assertThat(plans).isEmpty()
    }

    @Test
    fun structAbsentFromFileGetsNoPlan() {
        // The whole struct column was ADDED after the file — absent from the file tree. The top-level
        // NULL projection handles it; no reshape plan.
        val plans = NestedFieldReshapePlanner.buildPlans(
                listOf(structHandle(200, "s", "a" to INTEGER)),
                listOf(struct(200, "s"), child(201, "a", 200, 0)),
                listOf(child(100, "id", null, 0)))

        assertThat(plans).isEmpty()
    }

    @Test
    fun scalarColumnsAreIgnored() {
        val plans = NestedFieldReshapePlanner.buildPlans(
                listOf(DucklakeColumnHandle(100, "id", INTEGER, true)),
                listOf(child(100, "id", null, 0)),
                listOf(child(100, "id", null, 0)))

        assertThat(plans).isEmpty()
    }

    private companion object {
        private fun struct(id: Long, name: String): DucklakeColumn = col(id, name, "struct", 1, null)

        private fun child(id: Long, name: String, parent: Long?, order: Long): DucklakeColumn =
                col(id, name, "integer", order, parent)

        private fun col(id: Long, name: String, type: String, order: Long, parent: Long?): DucklakeColumn =
                DucklakeColumn(id, 1L, null, 10L, order, name, type, true, parent)

        private fun structHandle(id: Long, name: String, vararg fields: Pair<String, io.trino.spi.type.Type>): DucklakeColumnHandle {
            val rowFields = fields.map { RowType.field(it.first, it.second) }
            return DucklakeColumnHandle(id, name, RowType.from(rowFields), true)
        }
    }
}
