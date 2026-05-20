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
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
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
 * <p>Step 7: position-delete files supplied via {@link DuckLakePositionDelete}
 * are emitted as {@code iceberg_params.delete_files} entries with
 * {@code content = POSITION_DELETE (1)}. DuckLake catalogues only one active
 * position-delete file per data file per snapshot, so {@code positionDeletes}
 * has size 0 or 1 in v1; the wire list shape leaves headroom for that to
 * relax without a shape change.
 */
final class DuckLakeScanRange implements ConnectorScanRange, Serializable {

    private static final long serialVersionUID = 1L;

    // Iceberg v2 is the minimum that enables the BE reader's delete-file
    // dispatch path. v2 also matches DuckLake's positional-delete semantics
    // (sanity-check §2.1) so populating iceberg_params.delete_files is the
    // entire Step 7 wire-side delta — no version bump required.
    static final int ICEBERG_FORMAT_VERSION = 2;

    // TODO(option-B): once upstream lands the 5-line BE patch recognising
    // "ducklake" as a synonym of "iceberg" in file_scanner.cpp dispatch
    // (sanity-check §2.1), flip this to "ducklake". The thrift bytes for
    // iceberg_params remain identical — only this discriminator changes.
    static final String TABLE_FORMAT_TYPE = "iceberg";

    // Iceberg's TIcebergDeleteFileDesc.content discriminator: 1 = position
    // delete, 2 = equality delete, 3 = deletion vector. DuckLake only emits
    // position deletes (sanity-check §4: no equality-delete capability).
    // Mirrors IcebergDeleteFileDescriptor.Kind.POSITION_DELETE.wireValue().
    static final int CONTENT_POSITION_DELETE = 1;

    private final String path;
    private final long start;
    private final long length;
    private final long fileSize;
    private final String fileFormat;
    private final List<DuckLakePositionDelete> positionDeletes;

    DuckLakeScanRange(String path, long start, long length, long fileSize, String fileFormat) {
        this(path, start, length, fileSize, fileFormat, List.of());
    }

    DuckLakeScanRange(String path,
                      long start,
                      long length,
                      long fileSize,
                      String fileFormat,
                      List<DuckLakePositionDelete> positionDeletes) {
        this.path = Objects.requireNonNull(path, "path");
        this.start = start;
        this.length = length;
        this.fileSize = fileSize;
        this.fileFormat = Objects.requireNonNull(fileFormat, "fileFormat");
        this.positionDeletes = List.copyOf(
                Objects.requireNonNull(positionDeletes, "positionDeletes"));
    }

    List<DuckLakePositionDelete> positionDeletes() {
        return positionDeletes;
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
        // v2 enables BE delete-file dispatch. We always emit the list field
        // (even when empty) so the BE side reads a present-but-empty list
        // rather than an unset field — matches IcebergScanRange behaviour.
        List<TIcebergDeleteFileDesc> deletes = new ArrayList<>(positionDeletes.size());
        for (DuckLakePositionDelete pd : positionDeletes) {
            deletes.add(toThrift(pd));
        }
        fileDesc.setDeleteFiles(deletes);

        formatDesc.setIcebergParams(fileDesc);
        // Partition values / columns_from_path stay unset — partition pruning
        // is a Step-6 / v1.5 concern.
    }

    private static TIcebergDeleteFileDesc toThrift(DuckLakePositionDelete pd) {
        TIcebergDeleteFileDesc t = new TIcebergDeleteFileDesc();
        t.setPath(pd.path());
        // DuckLake's catalog records the delete file's format on the
        // ducklake_delete_file.format column ("parquet" in v1). The BE
        // reader's iceberg path needs TFileFormatType (an enum); map by
        // lowercased name. Anything other than parquet would be a writer
        // upgrade we'd need to coordinate with the BE side anyway.
        switch (pd.fileFormat().toLowerCase(java.util.Locale.ROOT)) {
            case "parquet" -> t.setFileFormat(TFileFormatType.FORMAT_PARQUET);
            default -> throw new IllegalStateException(
                    "Unsupported DuckLake delete-file format: " + pd.fileFormat());
        }
        // POSITION_DELETE discriminator. position_lower_bound /
        // position_upper_bound are left unset: DuckLake's catalog doesn't
        // record row-position bounds on delete files in v1, so the BE has
        // to scan the full delete file. Functionally correct; pruning
        // opportunity for a later pass.
        t.setContent(CONTENT_POSITION_DELETE);
        return t;
    }
}
