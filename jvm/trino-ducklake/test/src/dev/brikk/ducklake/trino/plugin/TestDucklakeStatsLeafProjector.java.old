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
package dev.brikk.ducklake.trino.plugin;

import dev.brikk.ducklake.catalog.DucklakeColumn;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDucklakeStatsLeafProjector
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Test
    void flatColumnsEmitOneLeafEach()
    {
        List<DucklakeColumnHandle> handles = List.of(
                handle(10, "id", INTEGER),
                handle(11, "name", VARCHAR));

        List<LeafStatsTarget> leaves = DucklakeStatsLeafProjector.projectFromCatalogTree(handles, List.of());

        assertThat(leaves).containsExactly(
                new LeafStatsTarget(10, INTEGER, 0),
                new LeafStatsTarget(11, VARCHAR, 1));
    }

    @Test
    void rowColumnEmitsOneLeafPerStructField()
    {
        // CREATE TABLE t (id INTEGER, data ROW(a INTEGER, b VARCHAR), tail BIGINT)
        // Catalog tree:
        //   id        → field_id 10
        //   data      → field_id 20
        //     data.a  → field_id 21
        //     data.b  → field_id 22
        //   tail      → field_id 30
        RowType dataRow = RowType.from(List.of(
                RowType.field("a", INTEGER),
                RowType.field("b", VARCHAR)));
        List<DucklakeColumnHandle> handles = List.of(
                handle(10, "id", INTEGER),
                handle(20, "data", dataRow),
                handle(30, "tail", BIGINT));
        List<DucklakeColumn> all = List.of(
                child(21, "a", 20),
                child(22, "b", 20));

        List<LeafStatsTarget> leaves = DucklakeStatsLeafProjector.projectFromCatalogTree(handles, all);

        assertThat(leaves).containsExactly(
                new LeafStatsTarget(10, INTEGER, 0),
                new LeafStatsTarget(21, INTEGER, 1),
                new LeafStatsTarget(22, VARCHAR, 2),
                new LeafStatsTarget(30, BIGINT, 3));
    }

    @Test
    void arrayColumnEmitsOneLeafForElement()
    {
        // CREATE TABLE t (id INTEGER, tags ARRAY<VARCHAR>)
        // Catalog tree:
        //   id              → field_id 10
        //   tags            → field_id 20
        //     tags.element  → field_id 21
        List<DucklakeColumnHandle> handles = List.of(
                handle(10, "id", INTEGER),
                handle(20, "tags", new ArrayType(VARCHAR)));
        List<DucklakeColumn> all = List.of(child(21, "element", 20));

        List<LeafStatsTarget> leaves = DucklakeStatsLeafProjector.projectFromCatalogTree(handles, all);

        assertThat(leaves).containsExactly(
                new LeafStatsTarget(10, INTEGER, 0),
                new LeafStatsTarget(21, VARCHAR, 1));
    }

    @Test
    void mapColumnEmitsKeyAndValueLeaves()
    {
        // CREATE TABLE t (attrs MAP<VARCHAR, BIGINT>)
        // Catalog tree:
        //   attrs        → field_id 10
        //     attrs.key   → field_id 11
        //     attrs.value → field_id 12
        MapType mapType = new MapType(VARCHAR, BIGINT, TYPE_OPERATORS);
        List<DucklakeColumnHandle> handles = List.of(handle(10, "attrs", mapType));
        List<DucklakeColumn> all = List.of(
                child(11, "key", 10),
                child(12, "value", 10));

        List<LeafStatsTarget> leaves = DucklakeStatsLeafProjector.projectFromCatalogTree(handles, all);

        assertThat(leaves).containsExactly(
                new LeafStatsTarget(11, VARCHAR, 0),
                new LeafStatsTarget(12, BIGINT, 1));
    }

    @Test
    void arrayOfRowProducesOneLeafPerStructField()
    {
        // CREATE TABLE t (events ARRAY<ROW(id INTEGER, label VARCHAR)>)
        // Catalog tree:
        //   events                 → field_id 10
        //     events.element       → field_id 11   (the ROW)
        //       events.element.id    → field_id 12
        //       events.element.label → field_id 13
        RowType rowType = RowType.from(List.of(
                RowType.field("id", INTEGER),
                RowType.field("label", VARCHAR)));
        List<DucklakeColumnHandle> handles = List.of(handle(10, "events", new ArrayType(rowType)));
        List<DucklakeColumn> all = List.of(
                child(11, "element", 10),
                child(12, "id", 11),
                child(13, "label", 11));

        List<LeafStatsTarget> leaves = DucklakeStatsLeafProjector.projectFromCatalogTree(handles, all);

        assertThat(leaves).containsExactly(
                new LeafStatsTarget(12, INTEGER, 0),
                new LeafStatsTarget(13, VARCHAR, 1));
    }

    @Test
    void rowOfRowProducesLeavesInDepthFirstOrder()
    {
        // CREATE TABLE t (data ROW(a INTEGER, nested ROW(b BIGINT, c VARCHAR)))
        // Catalog tree:
        //   data                  → field_id 10
        //     data.a              → field_id 11
        //     data.nested         → field_id 12
        //       data.nested.b     → field_id 13
        //       data.nested.c     → field_id 14
        RowType inner = RowType.from(List.of(
                RowType.field("b", BIGINT),
                RowType.field("c", VARCHAR)));
        RowType outer = RowType.from(List.of(
                RowType.field("a", INTEGER),
                RowType.field("nested", inner)));
        List<DucklakeColumnHandle> handles = List.of(handle(10, "data", outer));
        List<DucklakeColumn> all = List.of(
                child(11, "a", 10),
                child(12, "nested", 10),
                child(13, "b", 12),
                child(14, "c", 12));

        List<LeafStatsTarget> leaves = DucklakeStatsLeafProjector.projectFromCatalogTree(handles, all);

        assertThat(leaves).containsExactly(
                new LeafStatsTarget(11, INTEGER, 0),
                new LeafStatsTarget(13, BIGINT, 1),
                new LeafStatsTarget(14, VARCHAR, 2));
    }

    @Test
    void missingCatalogChildThrows()
    {
        RowType rowType = RowType.from(List.of(
                RowType.field("a", INTEGER),
                RowType.field("b", VARCHAR)));
        List<DucklakeColumnHandle> handles = List.of(handle(10, "data", rowType));
        // children list omits "b"
        List<DucklakeColumn> all = List.of(child(11, "a", 10));

        assertThatThrownBy(() -> DucklakeStatsLeafProjector.projectFromCatalogTree(handles, all))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ROW child 'b'")
                .hasMessageContaining("field_id=10");
    }

    @Test
    void rowIdColumnIsSkipped()
    {
        List<DucklakeColumnHandle> handles = List.of(
                DucklakeColumnHandle.rowIdColumnHandle(),
                handle(10, "id", INTEGER));

        List<LeafStatsTarget> leaves = DucklakeStatsLeafProjector.projectFromCatalogTree(handles, List.of());

        // Only the real column gets a leaf, at parquetColumnIndex 0 (the synthetic row_id
        // column is not in the file at all).
        assertThat(leaves).containsExactly(new LeafStatsTarget(10, INTEGER, 0));
    }

    private static DucklakeColumnHandle handle(long id, String name, Type type)
    {
        return new DucklakeColumnHandle(id, name, type, true);
    }

    private static DucklakeColumn child(long columnId, String name, long parentColumnId)
    {
        // The other DucklakeColumn fields are not consulted by the projector — only
        // columnId, columnName, and parentColumn are read. Pad with placeholders.
        return new DucklakeColumn(
                columnId,
                0L,
                Optional.empty(),
                1L,
                0L,
                name,
                "",
                true,
                Optional.of(parentColumnId));
    }
}
