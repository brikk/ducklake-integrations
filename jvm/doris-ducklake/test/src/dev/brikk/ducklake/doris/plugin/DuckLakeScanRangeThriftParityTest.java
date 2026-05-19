package dev.brikk.ducklake.doris.plugin;

import java.util.ArrayList;

import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks the wire shape of {@link DuckLakeScanRange#populateRangeParams} to
 * the Iceberg-compatible Option A format (sanity-check §2.1). The BE reader
 * branches on {@code table_format_type == "iceberg"} and then consumes
 * {@code iceberg_params} unchanged; DuckLake parquet files carry field-ids
 * just like Iceberg's, so this is zero BE change for v1.
 *
 * <p>The Option-A → Option-B flip (once upstream lands the 5-line BE patch
 * to recognise {@code "ducklake"}) is a one-string change. Parameterising
 * the discriminator below means the same golden survives the flip.
 */
class DuckLakeScanRangeThriftParityTest {

    private static final String PATH = "s3://warehouse/sales/orders/data_0.parquet";
    private static final long START = 0L;
    private static final long LENGTH = 4096L;
    private static final long FILE_SIZE = 4096L;
    private static final String FILE_FORMAT = "parquet";

    @Test
    void formatDescMatchesIcebergGoldenForDataOnlyRange() throws Exception {
        TTableFormatFileDesc actual = populatedFormatDesc(new DuckLakeScanRange(
                PATH, START, LENGTH, FILE_SIZE, FILE_FORMAT));
        TTableFormatFileDesc golden = goldenFormatDesc(PATH, "iceberg");

        assertThat(serialize(actual)).isEqualTo(serialize(golden));
    }

    @Test
    void rangeDescStaysUntouchedForUnpartitionedRange() throws Exception {
        // The no-partitions branch in populateRangeParams must leave the
        // TFileRangeDesc empty — pinning this guards against accidentally
        // setting columns_from_path / columns_from_path_keys when there is
        // nothing to fill.
        DuckLakeScanRange range = new DuckLakeScanRange(
                PATH, START, LENGTH, FILE_SIZE, FILE_FORMAT);

        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(formatDesc, rangeDesc);

        assertThat(rangeDesc.isSetColumnsFromPath()).isFalse();
        assertThat(rangeDesc.isSetColumnsFromPathKeys()).isFalse();
        assertThat(rangeDesc.isSetColumnsFromPathIsNull()).isFalse();
    }

    @Test
    void tableFormatTypeIsIcebergUntilOptionB() {
        // The discriminator string is the only field the Option-A → Option-B
        // flip changes. Keep this assertion in lockstep with
        // DuckLakeScanRange.TABLE_FORMAT_TYPE; flipping it without updating
        // the BE side is the deploy-time accident this test is here to catch.
        assertThat(new DuckLakeScanRange(PATH, START, LENGTH, FILE_SIZE, FILE_FORMAT)
                .getTableFormatType()).isEqualTo("iceberg");
    }

    @Test
    void icebergParamsCarryFormatVersion2AndEmptyDeleteFiles() throws Exception {
        // Asserts the load-bearing knobs by reading them off the populated
        // struct directly (cheaper than relying on the byte-identity test
        // for diagnostics when this regresses).
        TTableFormatFileDesc formatDesc = populatedFormatDesc(
                new DuckLakeScanRange(PATH, START, LENGTH, FILE_SIZE, FILE_FORMAT));

        assertThat(formatDesc.isSetIcebergParams()).isTrue();
        TIcebergFileDesc iceberg = formatDesc.getIcebergParams();
        assertThat(iceberg.getFormatVersion()).isEqualTo(2);
        assertThat(iceberg.getOriginalFilePath()).isEqualTo(PATH);
        assertThat(iceberg.isSetDeleteFiles()).isTrue();
        assertThat(iceberg.getDeleteFiles()).isEmpty();
        // Partition spec id / partition data json / row count / first row id
        // must remain unset so the BE reader picks its defaults.
        assertThat(iceberg.isSetPartitionSpecId()).isFalse();
        assertThat(iceberg.isSetPartitionDataJson()).isFalse();
        assertThat(iceberg.isSetContent()).isFalse();
    }

    private static TTableFormatFileDesc populatedFormatDesc(DuckLakeScanRange range) {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(formatDesc, rangeDesc);
        // The engine fills table_format_type on the descriptor before
        // dispatching; pin it here so the golden matches.
        formatDesc.setTableFormatType(range.getTableFormatType());
        return formatDesc;
    }

    /**
     * Hand-built mirror of {@code IcebergScanRange.populateRangeParams} for
     * a no-deletes, v2-format data file. Discriminator string is
     * parameterised so the Option-A → Option-B flip is a one-line change.
     */
    private static TTableFormatFileDesc goldenFormatDesc(String path, String discriminator) {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        formatDesc.setTableFormatType(discriminator);

        TIcebergFileDesc iceberg = new TIcebergFileDesc();
        iceberg.setFormatVersion(2);
        iceberg.setOriginalFilePath(path);
        iceberg.setDeleteFiles(new ArrayList<>());

        formatDesc.setIcebergParams(iceberg);
        return formatDesc;
    }

    private static byte[] serialize(TTableFormatFileDesc desc) throws Exception {
        // Use TBinaryProtocol explicitly so the byte-equality contract isn't
        // at the mercy of the default protocol changing across thrift versions.
        return new TSerializer(new TBinaryProtocol.Factory()).serialize(desc);
    }
}
