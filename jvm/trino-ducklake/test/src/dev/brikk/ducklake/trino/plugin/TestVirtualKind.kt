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

import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import io.airlift.json.ObjectMapperProvider
import io.trino.spi.TrinoException
import io.trino.spi.connector.ColumnHandle
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.Type
import io.trino.spi.type.VarcharType.VARCHAR
import io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER
import io.trino.type.TypeDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.jupiter.api.Test

/**
 * Unit invariants for the virtual-column sentinel scheme. These guard the mitigation that
 * makes the magic-number approach safe: the MERGE row-id handle (-100) must NOT be
 * classified as a queryable virtual (or the write-path guard would reject it / it would
 * leak into getColumnHandles), reserved ids must be unique and stay clear of real columns
 * (always >= 0), and the helpers must agree with the enum. See DESIGN-virtual-columns.md.
 */
class TestVirtualKind {
    @Test
    fun reservedIdsAreNegativeUniqueAndClearOfMergeRowId() {
        val ids = VirtualKind.values().map { it.columnId }
        assertThat(ids).doesNotHaveDuplicates()
        assertThat(ids).allMatch { it < 0 }
        assertThat(ids).doesNotContain(DucklakeColumnHandle.ROW_ID_COLUMN_ID)
    }

    @Test
    fun fromColumnIdRoundTripsEveryKind() {
        for (kind in VirtualKind.values()) {
            assertThat(VirtualKind.fromColumnId(kind.columnId)).isEqualTo(kind)
        }
    }

    @Test
    fun fromColumnIdIsNullForRealColumnsAndMergeRowId() {
        assertThat(VirtualKind.fromColumnId(DucklakeColumnHandle.ROW_ID_COLUMN_ID)).isNull()
        assertThat(VirtualKind.fromColumnId(0L)).isNull()
        assertThat(VirtualKind.fromColumnId(42L)).isNull()
    }

    @Test
    fun mergeRowIdHandleIsNotVirtual() {
        val mergeRowId = DucklakeColumnHandle.rowIdColumnHandle()
        assertThat(mergeRowId.isRowIdColumn()).isTrue()
        assertThat(mergeRowId.isVirtual()).isFalse()
        assertThat(mergeRowId.virtualKind()).isNull()
    }

    @Test
    fun realColumnHandleIsNotVirtual() {
        val real = DucklakeColumnHandle(7L, "price", io.trino.spi.type.DoubleType.DOUBLE, true)
        assertThat(real.isVirtual()).isFalse()
        assertThat(real.virtualKind()).isNull()
    }

    @Test
    fun virtualHandlesCarryEnumNameTypeAndAreVirtual() {
        val path = VirtualKind.PATH.columnHandle()
        assertThat(path.columnId).isEqualTo(-101L)
        assertThat(path.columnName).isEqualTo("\$path")
        assertThat(path.columnType).isEqualTo(VARCHAR)
        assertThat(path.isVirtual()).isTrue()
        assertThat(path.virtualKind()).isEqualTo(VirtualKind.PATH)

        val snapshot = VirtualKind.SNAPSHOT_ID.columnHandle()
        assertThat(snapshot.columnId).isEqualTo(-102L)
        assertThat(snapshot.columnName).isEqualTo("\$snapshot_id")
        assertThat(snapshot.columnType).isEqualTo(BIGINT)
        assertThat(snapshot.isVirtual()).isTrue()
        assertThat(snapshot.virtualKind()).isEqualTo(VirtualKind.SNAPSHOT_ID)
    }

    /**
     * Trino serializes ColumnHandles to JSON for split distribution, so a virtual handle must
     * survive a round trip with its sentinel id (hence virtual-ness) intact (DESIGN § 5).
     */
    @Test
    fun virtualColumnHandlesRoundTripThroughJackson() {
        for (kind in VirtualKind.values()) {
            val original = kind.columnHandle()
            val json = MAPPER.writeValueAsString(original)
            val restored = MAPPER.readValue(json, DucklakeColumnHandle::class.java)
            assertThat(restored).isEqualTo(original)
            assertThat(restored.virtualKind()).isEqualTo(kind)
            assertThat(restored.isVirtual()).isTrue()
        }
    }

    /**
     * The write-path guard (beginInsert / beginMerge) must reject every queryable virtual but
     * leave real columns and the MERGE row-id channel (-100, not a VirtualKind) alone (DESIGN § 5).
     */
    @Test
    fun rejectVirtualColumnWritesRejectsVirtualsOnly() {
        for (kind in VirtualKind.values()) {
            assertThatThrownBy { DucklakeMetadata.rejectVirtualColumnWrites(listOf(kind.columnHandle())) }
                    .isInstanceOf(TrinoException::class.java)
                    .hasMessageContaining("Virtual column cannot be written")
                    .hasMessageContaining(kind.columnName)
        }
    }

    @Test
    fun rejectVirtualColumnWritesAllowsRealAndMergeRowIdColumns() {
        val allowed: List<ColumnHandle> = listOf(
                DucklakeColumnHandle(7L, "price", DOUBLE, true),
                DucklakeColumnHandle(0L, "id", BIGINT, false),
                DucklakeColumnHandle.rowIdColumnHandle())  // MERGE channel, id -100, not virtual
        assertThatCode { DucklakeMetadata.rejectVirtualColumnWrites(allowed) }
                .doesNotThrowAnyException()
    }

    companion object {
        // A virtual handle carries a Trino Type (VARCHAR/BIGINT), so the mapper needs the same
        // Type deserializer Trino registers in production for ColumnHandle serialization.
        private val MAPPER: ObjectMapper = ObjectMapperProvider().apply {
            setJsonDeserializers(mapOf<Class<*>, JsonDeserializer<*>>(
                    Type::class.java to TypeDeserializer(TESTING_TYPE_MANAGER)))
        }.get()
    }
}
