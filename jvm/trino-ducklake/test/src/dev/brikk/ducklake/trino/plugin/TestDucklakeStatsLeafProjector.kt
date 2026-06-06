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
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.IntegerType.INTEGER
import io.trino.spi.type.MapType
import io.trino.spi.type.RowType
import io.trino.spi.type.Type
import io.trino.spi.type.TypeOperators
import io.trino.spi.type.VarcharType.VARCHAR
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

internal class TestDucklakeStatsLeafProjector {
    @Test
    fun flatColumnsEmitOneLeafEach() {
        val handles = listOf(
                handle(10L, "id", INTEGER),
                handle(11L, "name", VARCHAR))

        val leaves = DucklakeStatsLeafProjector.projectFromCatalogTree(handles, listOf())

        assertThat(leaves).containsExactly(
                LeafStatsTarget(10L, INTEGER, 0),
                LeafStatsTarget(11L, VARCHAR, 1))
    }

    @Test
    fun rowColumnEmitsOneLeafPerStructField() {
        // CREATE TABLE t (id INTEGER, data ROW(a INTEGER, b VARCHAR), tail BIGINT)
        // Catalog tree:
        //   id        → field_id 10
        //   data      → field_id 20
        //     data.a  → field_id 21
        //     data.b  → field_id 22
        //   tail      → field_id 30
        val dataRow = RowType.from(listOf(
                RowType.field("a", INTEGER),
                RowType.field("b", VARCHAR)))
        val handles = listOf(
                handle(10L, "id", INTEGER),
                handle(20L, "data", dataRow),
                handle(30L, "tail", BIGINT))
        val all = listOf(
                child(21L, "a", 20L),
                child(22L, "b", 20L))

        val leaves = DucklakeStatsLeafProjector.projectFromCatalogTree(handles, all)

        assertThat(leaves).containsExactly(
                LeafStatsTarget(10L, INTEGER, 0),
                LeafStatsTarget(21L, INTEGER, 1),
                LeafStatsTarget(22L, VARCHAR, 2),
                LeafStatsTarget(30L, BIGINT, 3))
    }

    @Test
    fun arrayColumnEmitsOneLeafForElement() {
        // CREATE TABLE t (id INTEGER, tags ARRAY<VARCHAR>)
        // Catalog tree:
        //   id              → field_id 10
        //   tags            → field_id 20
        //     tags.element  → field_id 21
        val handles = listOf(
                handle(10L, "id", INTEGER),
                handle(20L, "tags", ArrayType(VARCHAR)))
        val all = listOf(child(21L, "element", 20L))

        val leaves = DucklakeStatsLeafProjector.projectFromCatalogTree(handles, all)

        assertThat(leaves).containsExactly(
                LeafStatsTarget(10L, INTEGER, 0),
                LeafStatsTarget(21L, VARCHAR, 1))
    }

    @Test
    fun mapColumnEmitsKeyAndValueLeaves() {
        // CREATE TABLE t (attrs MAP<VARCHAR, BIGINT>)
        // Catalog tree:
        //   attrs        → field_id 10
        //     attrs.key   → field_id 11
        //     attrs.value → field_id 12
        val mapType = MapType(VARCHAR, BIGINT, TYPE_OPERATORS)
        val handles = listOf(handle(10L, "attrs", mapType))
        val all = listOf(
                child(11L, "key", 10L),
                child(12L, "value", 10L))

        val leaves = DucklakeStatsLeafProjector.projectFromCatalogTree(handles, all)

        assertThat(leaves).containsExactly(
                LeafStatsTarget(11L, VARCHAR, 0),
                LeafStatsTarget(12L, BIGINT, 1))
    }

    @Test
    fun arrayOfRowProducesOneLeafPerStructField() {
        // CREATE TABLE t (events ARRAY<ROW(id INTEGER, label VARCHAR)>)
        // Catalog tree:
        //   events                 → field_id 10
        //     events.element       → field_id 11   (the ROW)
        //       events.element.id    → field_id 12
        //       events.element.label → field_id 13
        val rowType = RowType.from(listOf(
                RowType.field("id", INTEGER),
                RowType.field("label", VARCHAR)))
        val handles = listOf(handle(10L, "events", ArrayType(rowType)))
        val all = listOf(
                child(11L, "element", 10L),
                child(12L, "id", 11L),
                child(13L, "label", 11L))

        val leaves = DucklakeStatsLeafProjector.projectFromCatalogTree(handles, all)

        assertThat(leaves).containsExactly(
                LeafStatsTarget(12L, INTEGER, 0),
                LeafStatsTarget(13L, VARCHAR, 1))
    }

    @Test
    fun rowOfRowProducesLeavesInDepthFirstOrder() {
        // CREATE TABLE t (data ROW(a INTEGER, nested ROW(b BIGINT, c VARCHAR)))
        // Catalog tree:
        //   data                  → field_id 10
        //     data.a              → field_id 11
        //     data.nested         → field_id 12
        //       data.nested.b     → field_id 13
        //       data.nested.c     → field_id 14
        val inner = RowType.from(listOf(
                RowType.field("b", BIGINT),
                RowType.field("c", VARCHAR)))
        val outer = RowType.from(listOf(
                RowType.field("a", INTEGER),
                RowType.field("nested", inner)))
        val handles = listOf(handle(10L, "data", outer))
        val all = listOf(
                child(11L, "a", 10L),
                child(12L, "nested", 10L),
                child(13L, "b", 12L),
                child(14L, "c", 12L))

        val leaves = DucklakeStatsLeafProjector.projectFromCatalogTree(handles, all)

        assertThat(leaves).containsExactly(
                LeafStatsTarget(11L, INTEGER, 0),
                LeafStatsTarget(13L, BIGINT, 1),
                LeafStatsTarget(14L, VARCHAR, 2))
    }

    @Test
    fun missingCatalogChildThrows() {
        val rowType = RowType.from(listOf(
                RowType.field("a", INTEGER),
                RowType.field("b", VARCHAR)))
        val handles = listOf(handle(10L, "data", rowType))
        // children list omits "b"
        val all = listOf(child(11L, "a", 10L))

        assertThatThrownBy { DucklakeStatsLeafProjector.projectFromCatalogTree(handles, all) }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("ROW child 'b'")
                .hasMessageContaining("field_id=10")
    }

    @Test
    fun rowIdColumnIsSkipped() {
        val handles = listOf(
                DucklakeColumnHandle.rowIdColumnHandle(),
                handle(10L, "id", INTEGER))

        val leaves = DucklakeStatsLeafProjector.projectFromCatalogTree(handles, listOf())

        // Only the real column gets a leaf, at parquetColumnIndex 0 (the synthetic row_id
        // column is not in the file at all).
        assertThat(leaves).containsExactly(LeafStatsTarget(10L, INTEGER, 0))
    }

    companion object {
        private val TYPE_OPERATORS = TypeOperators()

        private fun handle(id: Long, name: String, type: Type): DucklakeColumnHandle {
            return DucklakeColumnHandle(id, name, type, true)
        }

        private fun child(columnId: Long, name: String, parentColumnId: Long): DucklakeColumn {
            // The other DucklakeColumn fields are not consulted by the projector — only
            // columnId, columnName, and parentColumn are read. Pad with placeholders.
            return DucklakeColumn(
                    columnId,
                    0L,
                    null,
                    1L,
                    0L,
                    name,
                    "",
                    true,
                    parentColumnId)
        }
    }
}
