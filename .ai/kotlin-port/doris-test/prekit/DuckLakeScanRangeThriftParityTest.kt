package dev.brikk.ducklake.doris.plugin

import org.apache.doris.thrift.TFileFormatType
import org.apache.doris.thrift.TFileRangeDesc
import org.apache.doris.thrift.TIcebergDeleteFileDesc
import org.apache.doris.thrift.TIcebergFileDesc
import org.apache.doris.thrift.TTableFormatFileDesc
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.TBinaryProtocol
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

/**
 * Locks the wire shape of [DuckLakeScanRange.populateRangeParams] to
 * the Iceberg-compatible Option A format (sanity-check §2.1). The BE reader
 * branches on `table_format_type == "iceberg"` and then consumes
 * `iceberg_params` unchanged; DuckLake parquet files carry field-ids
 * just like Iceberg's, so this is zero BE change for v1.
 *
 * The Option-A → Option-B flip (once upstream lands the 5-line BE patch
 * to recognise `"ducklake"`) is a one-string change. Parameterising
 * the discriminator below means the same golden survives the flip.
 */
internal class DuckLakeScanRangeThriftParityTest {

    @Test
    @Throws(Exception::class)
    fun formatDescMatchesIcebergGoldenForDataOnlyRange() {
        val actual = populatedFormatDesc(
            DuckLakeScanRange(PATH, START, LENGTH, FILE_SIZE, FILE_FORMAT),
        )
        val golden = goldenFormatDesc(PATH, "iceberg")

        assertThat(serialize(actual)).isEqualTo(serialize(golden))
    }

    @Test
    @Throws(Exception::class)
    fun rangeDescStaysUntouchedForUnpartitionedRange() {
        // The no-partitions branch in populateRangeParams must leave the
        // TFileRangeDesc empty — pinning this guards against accidentally
        // setting columns_from_path / columns_from_path_keys when there is
        // nothing to fill.
        val range = DuckLakeScanRange(PATH, START, LENGTH, FILE_SIZE, FILE_FORMAT)

        val formatDesc = TTableFormatFileDesc()
        val rangeDesc = TFileRangeDesc()
        range.populateRangeParams(formatDesc, rangeDesc)

        assertThat(rangeDesc.isSetColumnsFromPath).isFalse()
        assertThat(rangeDesc.isSetColumnsFromPathKeys).isFalse()
        assertThat(rangeDesc.isSetColumnsFromPathIsNull).isFalse()
    }

    @Test
    fun tableFormatTypeIsIcebergUntilOptionB() {
        // The discriminator string is the only field the Option-A → Option-B
        // flip changes. Keep this assertion in lockstep with
        // DuckLakeScanRange.TABLE_FORMAT_TYPE; flipping it without updating
        // the BE side is the deploy-time accident this test is here to catch.
        assertThat(
            DuckLakeScanRange(PATH, START, LENGTH, FILE_SIZE, FILE_FORMAT)
                .tableFormatType,
        ).isEqualTo("iceberg")
    }

    @Test
    @Throws(Exception::class)
    fun icebergParamsCarryFormatVersion2AndEmptyDeleteFiles() {
        // Asserts the load-bearing knobs by reading them off the populated
        // struct directly (cheaper than relying on the byte-identity test
        // for diagnostics when this regresses).
        val formatDesc = populatedFormatDesc(
            DuckLakeScanRange(PATH, START, LENGTH, FILE_SIZE, FILE_FORMAT),
        )

        assertThat(formatDesc.isSetIcebergParams).isTrue()
        val iceberg = formatDesc.icebergParams
        assertThat(iceberg.formatVersion).isEqualTo(2)
        assertThat(iceberg.originalFilePath).isEqualTo(PATH)
        assertThat(iceberg.isSetDeleteFiles).isTrue()
        assertThat(iceberg.deleteFiles).isEmpty()
        // Partition spec id / partition data json / row count / first row id
        // must remain unset so the BE reader picks its defaults.
        assertThat(iceberg.isSetPartitionSpecId).isFalse()
        assertThat(iceberg.isSetPartitionDataJson).isFalse()
        assertThat(iceberg.isSetContent).isFalse()
    }

    @Test
    @Throws(Exception::class)
    fun formatDescMatchesIcebergGoldenForPositionDeleteRange() {
        // Byte identity against a hand-built golden that mirrors how Iceberg
        // emits a single position-delete file. A divergence here means the BE
        // reader's delete-file dispatch could miss or misinterpret our entries.
        val range = DuckLakeScanRange(
            PATH, START, LENGTH, FILE_SIZE, FILE_FORMAT,
            listOf(DuckLakePositionDelete(DELETE_PATH, FILE_FORMAT)),
        )

        val actual = populatedFormatDesc(range)
        val golden = goldenFormatDescWithPositionDelete(PATH, "iceberg", DELETE_PATH)

        assertThat(serialize(actual)).isEqualTo(serialize(golden))
    }

    @Test
    @Throws(Exception::class)
    fun icebergParamsCarryPositionDeleteFile() {
        // Field-level assertions on the populated delete-file descriptor:
        // path, file format, content discriminator. Position bounds stay
        // unset — DuckLake's catalog doesn't track them in v1, so the BE
        // scans the full delete file (correct, just unpruned).
        val range = DuckLakeScanRange(
            PATH, START, LENGTH, FILE_SIZE, FILE_FORMAT,
            listOf(DuckLakePositionDelete(DELETE_PATH, FILE_FORMAT)),
        )

        val formatDesc = populatedFormatDesc(range)
        val iceberg = formatDesc.icebergParams

        assertThat(iceberg.deleteFiles).hasSize(1)
        val delete = iceberg.deleteFiles[0]
        assertThat(delete.path).isEqualTo(DELETE_PATH)
        assertThat(delete.fileFormat).isEqualTo(TFileFormatType.FORMAT_PARQUET)
        // POSITION_DELETE discriminator (= 1). DuckLake has no equality
        // deletes, so this constant should never change.
        assertThat(delete.content)
            .isEqualTo(DuckLakeScanRange.CONTENT_POSITION_DELETE)
            .isEqualTo(1)
        assertThat(delete.isSetPositionLowerBound).isFalse()
        assertThat(delete.isSetPositionUpperBound).isFalse()
        assertThat(delete.isSetFieldIds).isFalse()
    }

    @Test
    fun positionDeletesAccessorIsImmutable() {
        val pd = DuckLakePositionDelete(DELETE_PATH, FILE_FORMAT)
        val range = DuckLakeScanRange(
            PATH, START, LENGTH, FILE_SIZE, FILE_FORMAT, listOf(pd),
        )

        val deletes = range.positionDeletes()
        assertThat(deletes).containsExactly(pd)
        // List.copyOf produces an unmodifiable list; mutation must throw so
        // the range's internal state can't be corrupted by an outside caller.
        Assertions.assertThrows(
            UnsupportedOperationException::class.java,
        ) { deletes.add(DuckLakePositionDelete("x", "parquet")) }
    }

    companion object {
        private const val PATH = "s3://warehouse/sales/orders/data_0.parquet"
        private const val DELETE_PATH = "s3://warehouse/sales/orders/delete_0.parquet"
        private const val START = 0L
        private const val LENGTH = 4096L
        private const val FILE_SIZE = 4096L
        private const val FILE_FORMAT = "parquet"

        private fun populatedFormatDesc(range: DuckLakeScanRange): TTableFormatFileDesc {
            val formatDesc = TTableFormatFileDesc()
            val rangeDesc = TFileRangeDesc()
            range.populateRangeParams(formatDesc, rangeDesc)
            // The engine fills table_format_type on the descriptor before
            // dispatching; pin it here so the golden matches.
            formatDesc.setTableFormatType(range.tableFormatType)
            return formatDesc
        }

        /**
         * Hand-built mirror of `IcebergScanRange.populateRangeParams` for
         * a no-deletes, v2-format data file. Discriminator string is
         * parameterised so the Option-A → Option-B flip is a one-line change.
         */
        private fun goldenFormatDesc(path: String, discriminator: String): TTableFormatFileDesc {
            val formatDesc = TTableFormatFileDesc()
            formatDesc.setTableFormatType(discriminator)

            val iceberg = TIcebergFileDesc()
            iceberg.setFormatVersion(2)
            iceberg.setOriginalFilePath(path)
            iceberg.setDeleteFiles(ArrayList())

            formatDesc.setIcebergParams(iceberg)
            return formatDesc
        }

        /**
         * Hand-built golden for a v2 data file carrying one position-delete file.
         * Mirrors IcebergScanRange's single-position-delete emission with no
         * bounds set (DuckLake's catalog doesn't surface row-position bounds).
         */
        private fun goldenFormatDescWithPositionDelete(
            path: String,
            discriminator: String,
            deletePath: String,
        ): TTableFormatFileDesc {
            val formatDesc = TTableFormatFileDesc()
            formatDesc.setTableFormatType(discriminator)

            val iceberg = TIcebergFileDesc()
            iceberg.setFormatVersion(2)
            iceberg.setOriginalFilePath(path)

            val deletes = ArrayList<TIcebergDeleteFileDesc>()
            val delete = TIcebergDeleteFileDesc()
            delete.setPath(deletePath)
            delete.setFileFormat(TFileFormatType.FORMAT_PARQUET)
            delete.setContent(1) // POSITION_DELETE
            deletes.add(delete)
            iceberg.setDeleteFiles(deletes)

            formatDesc.setIcebergParams(iceberg)
            return formatDesc
        }

        @Throws(Exception::class)
        private fun serialize(desc: TTableFormatFileDesc): ByteArray {
            // Use TBinaryProtocol explicitly so the byte-equality contract isn't
            // at the mercy of the default protocol changing across thrift versions.
            return TSerializer(TBinaryProtocol.Factory()).serialize(desc)
        }
    }
}
