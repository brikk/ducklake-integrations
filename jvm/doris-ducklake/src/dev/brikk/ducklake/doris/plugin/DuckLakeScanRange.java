package dev.brikk.ducklake.doris.plugin;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.doris.connector.api.scan.ConnectorDeleteFile;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

/**
 * Per-file scan range emitted by {@link DuckLakeScanPlanProvider}.
 *
 * <p>Step 4 / Option A wire format (sanity-check §2.1): emits
 * {@code table_format_type = "iceberg"} and packs the file shape into
 * {@code iceberg_params} so Doris's BE dispatches the existing Iceberg
 * parquet reader. DuckLake's parquet files already carry field-ids on each
 * column, which is what the reader consumes — zero BE change needed.
 *
 * <p>{@code deleteFiles} stays empty for v1 SELECT-* scope; Step 7 will
 * populate {@code iceberg_params.delete_files} from DuckLake's positional
 * delete rows.
 */
final class DuckLakeScanRange implements ConnectorScanRange, Serializable {

    private static final long serialVersionUID = 1L;

    // Iceberg v2 is the minimum that enables the BE reader's delete-file
    // dispatch path. v2 also matches DuckLake's positional-delete semantics
    // (sanity-check §2.1) so the Step 7 flip is just populating
    // iceberg_params.delete_files — no version bump required.
    static final int ICEBERG_FORMAT_VERSION = 2;

    // TODO(option-B): once upstream lands the 5-line BE patch recognising
    // "ducklake" as a synonym of "iceberg" in file_scanner.cpp dispatch
    // (sanity-check §2.1), flip this to "ducklake". The thrift bytes for
    // iceberg_params remain identical — only this discriminator changes.
    static final String TABLE_FORMAT_TYPE = "iceberg";

    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final String fileFormat;

    DuckLakeScanRange(String path, long start, long length, long fileSize, String fileFormat) {
        this.path = Objects.requireNonNull(path, "path");
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.fileFormat = Objects.requireNonNull(fileFormat, "fileFormat");
    }

    @Override
    public ConnectorScanRangeType getRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    @Override
    public Optional<String> getPath() {
        return Optional.of(path);
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public long getFileSize() {
        return fileSize;
    }

    @Override
    public String getFileFormat() {
        return fileFormat;
    }

    @Override
    public Map<String, String> getProperties() {
        return Collections.emptyMap();
    }

    @Override
    public String getTableFormatType() {
        return TABLE_FORMAT_TYPE;
    }

    @Override
    public Map<String, String> getPartitionValues() {
        return Collections.emptyMap();
    }

    @Override
    public List<ConnectorDeleteFile> getDeleteFiles() {
        return Collections.emptyList();
    }

    @Override
    public void populateRangeParams(TTableFormatFileDesc formatDesc, TFileRangeDesc rangeDesc) {
        Objects.requireNonNull(formatDesc, "formatDesc");
        Objects.requireNonNull(rangeDesc, "rangeDesc");

        TIcebergFileDesc fileDesc = new TIcebergFileDesc();
        fileDesc.setFormatVersion(ICEBERG_FORMAT_VERSION);
        fileDesc.setOriginalFilePath(path);
        // v2 enables BE delete-file dispatch; explicit empty list signals
        // "no deletes" for this range. Step 7 populates this.
        fileDesc.setDeleteFiles(new ArrayList<>());

        formatDesc.setIcebergParams(fileDesc);
        // Partition values / columns_from_path stay unset — partition pruning
        // is a Step-6 / v1.5 concern.
    }
}
