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

import io.airlift.json.JsonCodec
import io.airlift.json.JsonCodecFactory
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * Golden wire-format lock for the `@JacksonSerializedInternalJavaCompatibleClass` catalog types.
 *
 * Each case serializes a representative instance through the SAME airlift
 * [JsonCodec] the production Trino fragment paths use, then asserts the JSON
 * string EXACTLY equals an embedded golden literal so the wire shape is frozen.
 * This is the regression net for an upcoming `Optional` -> `T?` change on the
 * Jackson-typed fields: the goldens below capture the CURRENT behaviour, so any
 * shape drift (e.g. `Optional` switching from value-or-absent to
 * `{"present":...}`, or empties becoming explicit `null`) fails loudly.
 *
 * Observed `Optional` behaviour with this codec (airlift registers the JDK8
 * module): a present `Optional`/`OptionalLong`/`OptionalInt` serializes as the
 * bare value; an empty one is OMITTED from the object entirely (not `null`, not
 * `{"present":false}`).
 *
 * [DucklakeNameMap] / [DucklakeNameMapEntry] are the exception: they carry
 * `@JsonCreator`/`@JsonProperty` for DESERIALIZATION only and (unlike the other
 * marked types) are NOT `@JvmRecord` and have no `@get:JsonProperty` getters, so
 * the codec emits an empty `{}` on the way out and then rejects that empty
 * object on the way back in. Those cases lock the real (lossy, non-round-trip)
 * behaviour rather than pretending it round-trips.
 */
class TestJacksonWireFormat {
    private fun <T> codec(type: Class<T>): JsonCodec<T> = JsonCodecFactory().jsonCodec(type)

    /** Serialize through the airlift codec and assert the JSON is byte-for-byte the golden. */
    private fun <T> assertJson(type: Class<T>, instance: T, golden: String) {
        assertThat(codec(type).toJson(instance)).isEqualTo(golden)
    }

    /** Assert `fromJson(toJsonBytes(instance)) == instance` through the airlift codec. */
    private fun <T> assertRoundTrip(type: Class<T>, instance: T) {
        val c = codec(type)
        assertThat(c.fromJson(c.toJsonBytes(instance))).isEqualTo(instance)
    }

    // --- DucklakeColumn (Optional<Long> endSnapshot, parentColumn) ---

    @Test
    fun columnOptionalPresent() {
        val instance = DucklakeColumn(7L, 1L, 9L, 3L, 0L, "id", "BIGINT", true, 2L)
        assertJson(
            DucklakeColumn::class.java,
            instance,
            """{"columnId":7,"beginSnapshot":1,"endSnapshot":9,"tableId":3,"columnOrder":0,""" +
                """"columnName":"id","columnType":"BIGINT","nullsAllowed":true,"parentColumn":2}""",
        )
        assertRoundTrip(DucklakeColumn::class.java, instance)
    }

    @Test
    fun columnOptionalEmpty() {
        val instance = DucklakeColumn(7L, 1L, null, 3L, 0L, "id", "BIGINT", true, null)
        assertJson(
            DucklakeColumn::class.java,
            instance,
            """{"columnId":7,"beginSnapshot":1,"tableId":3,"columnOrder":0,""" +
                """"columnName":"id","columnType":"BIGINT","nullsAllowed":true}""",
        )
        assertRoundTrip(DucklakeColumn::class.java, instance)
    }

    // --- DucklakeFileColumnStats (Optional<String> minValue, maxValue) ---

    @Test
    fun fileColumnStatsOptionalPresent() {
        val instance = DucklakeFileColumnStats(1L, 512L, 100L, 5L, "a", "z", false)
        assertJson(
            DucklakeFileColumnStats::class.java,
            instance,
            """{"columnId":1,"columnSizeBytes":512,"valueCount":100,"nullCount":5,""" +
                """"minValue":"a","maxValue":"z","containsNan":false}""",
        )
        assertRoundTrip(DucklakeFileColumnStats::class.java, instance)
    }

    @Test
    fun fileColumnStatsOptionalEmpty() {
        val instance = DucklakeFileColumnStats(1L, 512L, 100L, 5L, null, null, true)
        assertJson(
            DucklakeFileColumnStats::class.java,
            instance,
            """{"columnId":1,"columnSizeBytes":512,"valueCount":100,"nullCount":5,"containsNan":true}""",
        )
        assertRoundTrip(DucklakeFileColumnStats::class.java, instance)
    }

    // --- DucklakeDeleteFragment (no Optional fields) ---

    @Test
    fun deleteFragment() {
        val instance = DucklakeDeleteFragment(11L, "del.parquet", 5L, 1024L, 200L, 2L)
        assertJson(
            DucklakeDeleteFragment::class.java,
            instance,
            """{"dataFileId":11,"path":"del.parquet","deleteCount":5,""" +
                """"fileSizeBytes":1024,"footerSize":200,"newDeleteCount":2}""",
        )
        assertRoundTrip(DucklakeDeleteFragment::class.java, instance)
    }

    // --- DucklakeWriteFragment (OptionalLong partitionId, Optional<DucklakeNameMap> nameMap) ---

    @Test
    fun writeFragmentOptionalPresent() {
        val instance = DucklakeWriteFragment(
            "f.parquet", true, "parquet", 1024L, 200L, 100L,
            listOf(DucklakeFileColumnStats(1L, 512L, 100L, 0L, "1", "9", false)),
            mapOf(0 to "US"),
            42L,
            DucklakeNameMap(listOf(DucklakeNameMapEntry("c", 1L))),
        )
        // NOTE: nameMap serializes as `{"entries":[{}]}` — DucklakeNameMapEntry has no
        // serialization getters, so its content is lost here (see nameMap* cases below).
        assertJson(
            DucklakeWriteFragment::class.java,
            instance,
            """{"path":"f.parquet","pathIsRelative":true,"fileFormat":"parquet",""" +
                """"fileSizeBytes":1024,"footerSize":200,"recordCount":100,""" +
                """"columnStats":[{"columnId":1,"columnSizeBytes":512,"valueCount":100,""" +
                """"nullCount":0,"minValue":"1","maxValue":"9","containsNan":false}],""" +
                """"partitionValues":{"0":"US"},"partitionId":42,"nameMap":{"entries":[{}]}}""",
        )
    }

    @Test
    fun writeFragmentOptionalEmpty() {
        val instance = DucklakeWriteFragment("f.parquet", 1024L, 200L, 100L, emptyList())
        assertJson(
            DucklakeWriteFragment::class.java,
            instance,
            """{"path":"f.parquet","pathIsRelative":true,"fileFormat":"parquet",""" +
                """"fileSizeBytes":1024,"footerSize":200,"recordCount":100,""" +
                """"columnStats":[],"partitionValues":{}}""",
        )
        // Round-trips because the empty fragment carries no name map (Optional.empty()).
        assertRoundTrip(DucklakeWriteFragment::class.java, instance)
    }

    // --- DucklakePartitionField (OptionalInt arity) ---

    @Test
    fun partitionFieldArityPresent() {
        val instance = DucklakePartitionField(0, 5L, DucklakePartitionTransform.BUCKET, 8)
        assertJson(
            DucklakePartitionField::class.java,
            instance,
            """{"partitionKeyIndex":0,"columnId":5,"transform":"BUCKET","arity":8}""",
        )
        assertRoundTrip(DucklakePartitionField::class.java, instance)
    }

    @Test
    fun partitionFieldArityEmpty() {
        val instance = DucklakePartitionField(0, 5L, DucklakePartitionTransform.IDENTITY, null)
        assertJson(
            DucklakePartitionField::class.java,
            instance,
            """{"partitionKeyIndex":0,"columnId":5,"transform":"IDENTITY"}""",
        )
        assertRoundTrip(DucklakePartitionField::class.java, instance)
    }

    // --- DucklakePartitionSpec (nested DucklakePartitionField list) ---

    @Test
    fun partitionSpec() {
        val instance = DucklakePartitionSpec(
            1L, 2L,
            listOf(DucklakePartitionField(0, 5L, DucklakePartitionTransform.IDENTITY, null)),
        )
        assertJson(
            DucklakePartitionSpec::class.java,
            instance,
            """{"partitionId":1,"tableId":2,"fields":[""" +
                """{"partitionKeyIndex":0,"columnId":5,"transform":"IDENTITY"}]}""",
        )
        assertRoundTrip(DucklakePartitionSpec::class.java, instance)
    }

    // --- PartitionFieldSpec (OptionalInt arity) ---

    @Test
    fun partitionFieldSpecArityPresent() {
        val instance = PartitionFieldSpec("c", DucklakePartitionTransform.BUCKET, 4)
        assertJson(
            PartitionFieldSpec::class.java,
            instance,
            """{"columnName":"c","transform":"BUCKET","arity":4}""",
        )
        assertRoundTrip(PartitionFieldSpec::class.java, instance)
    }

    @Test
    fun partitionFieldSpecArityEmpty() {
        val instance = PartitionFieldSpec("c", DucklakePartitionTransform.IDENTITY, null)
        assertJson(
            PartitionFieldSpec::class.java,
            instance,
            """{"columnName":"c","transform":"IDENTITY"}""",
        )
        assertRoundTrip(PartitionFieldSpec::class.java, instance)
    }

    // --- DucklakeNameMapEntry / DucklakeNameMap (deserialize-only annotations) ---
    //
    // These two are marked @JacksonSerializedInternalJavaCompatibleClass but, unlike the other
    // marked types, are NOT @JvmRecord and expose no serialization getters. The
    // codec therefore emits an empty `{}` (data is dropped) and then rejects that
    // empty object on read-back (sourceName is a non-null String with no value).
    // The asserts below LOCK that real lossy behaviour so the upcoming Optional
    // change can't silently alter it.

    @Test
    fun nameMapEntrySerializesEmptyAndIsNotRoundTrippable() {
        val c = codec(DucklakeNameMapEntry::class.java)
        val instance = DucklakeNameMapEntry("col", 3L, false, listOf(DucklakeNameMapEntry("child", 4L)))
        assertThat(c.toJson(instance)).isEqualTo("{}")
        assertThatThrownBy { c.fromJson(c.toJsonBytes(instance)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun nameMapSerializesEntriesAsEmptyAndIsNotRoundTrippable() {
        val c = codec(DucklakeNameMap::class.java)
        val instance = DucklakeNameMap(listOf(DucklakeNameMapEntry("col", 3L)))
        assertThat(c.toJson(instance)).isEqualTo("""{"entries":[{}]}""")
        assertThatThrownBy { c.fromJson(c.toJsonBytes(instance)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    // --- DucklakeDataFile (Optional<Long>/<String>/<Boolean> fields) ---

    @Test
    fun dataFileOptionalPresent() {
        val instance = DucklakeDataFile(
            1L, 2L, 3L, 4L, 5L, "f.parquet", true, "parquet",
            100L, 1024L, 200L, 0L, 7L, "del.parquet",
            true, 99L, "parquet",
            8L,
        )
        assertJson(
            DucklakeDataFile::class.java,
            instance,
            """{"dataFileId":1,"tableId":2,"beginSnapshot":3,"endSnapshot":4,"fileOrder":5,""" +
                """"path":"f.parquet","pathIsRelative":true,"fileFormat":"parquet","recordCount":100,""" +
                """"fileSizeBytes":1024,"footerSize":200,"rowIdStart":0,"partitionId":7,""" +
                """"deleteFilePath":"del.parquet","deleteFilePathIsRelative":true,""" +
                """"deleteFileFooterSize":99,"deleteFileFormat":"parquet","mappingId":8}""",
        )
        assertRoundTrip(DucklakeDataFile::class.java, instance)
    }

    @Test
    fun dataFileOptionalEmpty() {
        val instance = DucklakeDataFile(
            1L, 2L, 3L, null, 5L, "f.parquet", true, "parquet",
            100L, 1024L, 200L, 0L, null, null,
            null, null, null,
            null,
        )
        assertJson(
            DucklakeDataFile::class.java,
            instance,
            """{"dataFileId":1,"tableId":2,"beginSnapshot":3,"fileOrder":5,""" +
                """"path":"f.parquet","pathIsRelative":true,"fileFormat":"parquet","recordCount":100,""" +
                """"fileSizeBytes":1024,"footerSize":200,"rowIdStart":0}""",
        )
        assertRoundTrip(DucklakeDataFile::class.java, instance)
    }

    // --- DucklakeSnapshotChange (Optional<String> fields) ---

    @Test
    fun snapshotChangeOptionalPresent() {
        val instance = DucklakeSnapshotChange(
            1L, "made", "me",
            "msg", "extra",
        )
        assertJson(
            DucklakeSnapshotChange::class.java,
            instance,
            """{"snapshotId":1,"changesMade":"made","author":"me",""" +
                """"commitMessage":"msg","commitExtraInfo":"extra"}""",
        )
        assertRoundTrip(DucklakeSnapshotChange::class.java, instance)
    }

    @Test
    fun snapshotChangeOptionalEmpty() {
        val instance = DucklakeSnapshotChange(
            1L, null, null,
            null, null,
        )
        assertJson(
            DucklakeSnapshotChange::class.java,
            instance,
            """{"snapshotId":1}""",
        )
        assertRoundTrip(DucklakeSnapshotChange::class.java, instance)
    }

    // --- DucklakeColumnStats (Optional<String> minValue, maxValue) ---

    @Test
    fun columnStatsOptionalPresent() {
        val instance = DucklakeColumnStats(1L, 100L, 5L, 512L, "a", "z")
        assertJson(
            DucklakeColumnStats::class.java,
            instance,
            """{"columnId":1,"totalValueCount":100,"totalNullCount":5,""" +
                """"totalSizeBytes":512,"minValue":"a","maxValue":"z"}""",
        )
        assertRoundTrip(DucklakeColumnStats::class.java, instance)
    }

    @Test
    fun columnStatsOptionalEmpty() {
        val instance = DucklakeColumnStats(1L, 100L, 5L, 512L, null, null)
        assertJson(
            DucklakeColumnStats::class.java,
            instance,
            """{"columnId":1,"totalValueCount":100,"totalNullCount":5,"totalSizeBytes":512}""",
        )
        assertRoundTrip(DucklakeColumnStats::class.java, instance)
    }

    // --- DucklakeSchema (Optional<Long> endSnapshot, Optional<String> path, Optional<Boolean> pathIsRelative) ---

    @Test
    fun schemaOptionalPresent() {
        val uuid = java.util.UUID.fromString("00000000-0000-0000-0000-000000000001")
        val instance = DucklakeSchema(
            1L, uuid, 2L, 3L, "sch", "p", true,
        )
        assertJson(
            DucklakeSchema::class.java,
            instance,
            """{"schemaId":1,"schemaUuid":"00000000-0000-0000-0000-000000000001","beginSnapshot":2,""" +
                """"endSnapshot":3,"schemaName":"sch","path":"p","pathIsRelative":true}""",
        )
        assertRoundTrip(DucklakeSchema::class.java, instance)
    }

    @Test
    fun schemaOptionalEmpty() {
        val uuid = java.util.UUID.fromString("00000000-0000-0000-0000-000000000001")
        val instance = DucklakeSchema(
            1L, uuid, 2L, null, "sch", null, null,
        )
        assertJson(
            DucklakeSchema::class.java,
            instance,
            """{"schemaId":1,"schemaUuid":"00000000-0000-0000-0000-000000000001","beginSnapshot":2,""" +
                """"schemaName":"sch"}""",
        )
        assertRoundTrip(DucklakeSchema::class.java, instance)
    }

    // --- DucklakeTable (Optional<Long> endSnapshot, Optional<String> path, Optional<Boolean> pathIsRelative) ---

    @Test
    fun tableOptionalPresent() {
        val uuid = java.util.UUID.fromString("00000000-0000-0000-0000-000000000002")
        val instance = DucklakeTable(
            1L, uuid, 2L, 3L, 4L, "tbl", "p", false,
        )
        assertJson(
            DucklakeTable::class.java,
            instance,
            """{"tableId":1,"tableUuid":"00000000-0000-0000-0000-000000000002","beginSnapshot":2,""" +
                """"endSnapshot":3,"schemaId":4,"tableName":"tbl","path":"p","pathIsRelative":false}""",
        )
        assertRoundTrip(DucklakeTable::class.java, instance)
    }

    @Test
    fun tableOptionalEmpty() {
        val uuid = java.util.UUID.fromString("00000000-0000-0000-0000-000000000002")
        val instance = DucklakeTable(
            1L, uuid, 2L, null, 4L, "tbl", null, null,
        )
        assertJson(
            DucklakeTable::class.java,
            instance,
            """{"tableId":1,"tableUuid":"00000000-0000-0000-0000-000000000002","beginSnapshot":2,""" +
                """"schemaId":4,"tableName":"tbl"}""",
        )
        assertRoundTrip(DucklakeTable::class.java, instance)
    }

    // --- DucklakeView (Optional<String> viewMetadata, OptionalLong endSnapshot) ---

    @Test
    fun viewOptionalPresent() {
        val instance = DucklakeView(
            1L, "vu", 2L, "v", "SELECT 1", "duckdb",
            "meta", 3L, 4L,
        )
        assertJson(
            DucklakeView::class.java,
            instance,
            """{"viewId":1,"viewUuid":"vu","schemaId":2,"viewName":"v","sql":"SELECT 1",""" +
                """"dialect":"duckdb","viewMetadata":"meta","beginSnapshot":3,"endSnapshot":4}""",
        )
        assertRoundTrip(DucklakeView::class.java, instance)
    }

    @Test
    fun viewOptionalEmpty() {
        val instance = DucklakeView(
            1L, "vu", 2L, "v", "SELECT 1", "duckdb",
            null, 3L, null,
        )
        assertJson(
            DucklakeView::class.java,
            instance,
            """{"viewId":1,"viewUuid":"vu","schemaId":2,"viewName":"v","sql":"SELECT 1",""" +
                """"dialect":"duckdb","beginSnapshot":3}""",
        )
        assertRoundTrip(DucklakeView::class.java, instance)
    }
}
