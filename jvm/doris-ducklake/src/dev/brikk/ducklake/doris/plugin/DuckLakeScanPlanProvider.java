package dev.brikk.ducklake.doris.plugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import dev.brikk.ducklake.catalog.DucklakeCatalog;
import dev.brikk.ducklake.catalog.DucklakeDataFile;
import dev.brikk.ducklake.catalog.DucklakeSchema;
import dev.brikk.ducklake.catalog.DucklakeTable;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TFileScanRangeParams;

/**
 * Splits a DuckLake table read into per-file scan ranges. v1 emits one range
 * per {@code DucklakeDataFile} active at the snapshot pinned on the
 * {@link DuckLakeTableHandle}; deletes / pushdown / time-travel resolution
 * layer on top per the roadmap.
 */
final class DuckLakeScanPlanProvider implements ConnectorScanPlanProvider {

    // Plugin-private prefix used to ferry storage credentials through the
    // scan-node-properties → populateScanLevelParams transit. Mirrors the
    // pattern in IcebergScanPlanProvider#PROP_LOCATION_PREFIX; the prefix is
    // stripped before the keys land in TFileScanRangeParams so the BE sees
    // the canonical "s3.*" form it normalises in S3ObjStorage.
    static final String PROP_LOCATION_PREFIX = "ducklake.location.";

    // PluginDrivenScanNode reads this key out of getScanNodeProperties() to
    // decide which BE reader to dispatch to (PluginDrivenScanNode.java
    // mapFileFormatType: "parquet" → FORMAT_PARQUET, default FORMAT_JNI).
    // ConnectorScanRange#getFileFormat() is NOT consumed there — the engine
    // never reads it directly for the plugin-driven path.
    static final String PROP_FILE_FORMAT_TYPE = "file_format_type";

    // DuckLake's writer always produces parquet. The catalog records the
    // format per data file (DucklakeDataFile.fileFormat) and our scan
    // plan validates it matches, but for the scan-node-level reader
    // dispatch one shared answer is sufficient.
    static final String PARQUET_FORMAT = "parquet";

    private final DucklakeCatalog catalog;
    private final DuckLakePathResolver pathResolver;
    private final Map<String, String> catalogProperties;

    DuckLakeScanPlanProvider(DucklakeCatalog catalog,
                              DuckLakePathResolver pathResolver,
                              Map<String, String> catalogProperties) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.pathResolver = Objects.requireNonNull(pathResolver, "pathResolver");
        this.catalogProperties = Map.copyOf(
                Objects.requireNonNull(catalogProperties, "catalogProperties"));
    }

    @Override
    public ConnectorScanRangeType getScanRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorScanRequest req) {
        Objects.requireNonNull(req, "req");
        DuckLakeTableHandle handle = asDuckLakeHandle(req.getTable());

        DucklakeSchema schema = catalog.getSchema(handle.database(), handle.snapshotId())
                .orElseThrow(() -> new IllegalStateException(
                        "Schema metadata missing for '" + handle.database() + "' at snapshot "
                                + handle.snapshotId()));
        DucklakeTable table = catalog.getTableById(handle.tableId(), handle.snapshotId())
                .orElseThrow(() -> new IllegalStateException(
                        "Table metadata missing for tableId=" + handle.tableId()
                                + " at snapshot " + handle.snapshotId()));

        String tableDataPath = pathResolver.resolveTableDataPath(schema, table);

        List<DucklakeDataFile> dataFiles = catalog.getDataFiles(
                handle.tableId(), handle.snapshotId());

        List<ConnectorScanRange> ranges = new ArrayList<>(dataFiles.size());
        for (DucklakeDataFile file : dataFiles) {
            String absolutePath = pathResolver.resolveFilePath(
                    file.path(), file.pathIsRelative(), tableDataPath);
            // Full-file extent: start=0, length=fileSize. BE splits internally
            // along parquet row groups; pre-splitting in the planner is a v2 concern.
            ranges.add(new DuckLakeScanRange(
                    absolutePath,
                    0L,
                    file.fileSizeBytes(),
                    file.fileSizeBytes(),
                    normalizeFileFormat(file.fileFormat())));
        }
        return ranges;
    }

    /**
     * Emits the storage credentials (s3.*, hdfs.*, AWS_*, etc.) the BE needs
     * to open data files, under {@link #PROP_LOCATION_PREFIX} so
     * {@link #populateScanLevelParams} can strip the prefix back off on its
     * way to the thrift descriptor. Mirrors the Iceberg connector's pattern;
     * DuckLake adds no vended-credential layer for v1 (no REST catalog), so
     * we pass through the static catalog properties only.
     */
    @Override
    public Map<String, String> getScanNodeProperties(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        Map<String, String> out = new LinkedHashMap<>();
        // Tell PluginDrivenScanNode which BE reader to dispatch (else it
        // defaults to FORMAT_JNI and the iceberg reader bails with
        // "Not supported create reader for table format: iceberg / file
        // format: FORMAT_JNI").
        out.put(PROP_FILE_FORMAT_TYPE, PARQUET_FORMAT);
        for (Map.Entry<String, String> entry : catalogProperties.entrySet()) {
            if (isStorageProperty(entry.getKey())) {
                out.put(PROP_LOCATION_PREFIX + entry.getKey(), entry.getValue());
            }
        }
        return out;
    }

    @Override
    public void populateScanLevelParams(TFileScanRangeParams params,
                                        Map<String, String> nodeProperties) {
        Objects.requireNonNull(params, "params");
        if (nodeProperties == null || nodeProperties.isEmpty()) {
            return;
        }
        if (params.getProperties() == null) {
            params.setProperties(new HashMap<>());
        }
        Map<String, String> out = params.getProperties();
        for (Map.Entry<String, String> entry : nodeProperties.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(PROP_LOCATION_PREFIX)) {
                continue;
            }
            String stripped = key.substring(PROP_LOCATION_PREFIX.length());
            // The BE's S3ClientFactory::convert_properties_to_s3_conf
            // (be/src/util/s3_util.cpp) reads `AWS_*` keys verbatim — the
            // FE-side S3ObjStorage normaliser does NOT run on the parquet
            // reader path. So we emit both the FE-style ("s3.*") key and the
            // canonical BE-style ("AWS_*") alias. Belt + suspenders: if the
            // user supplies one form, the BE sees the other too.
            out.put(stripped, entry.getValue());
            String alias = canonicalAwsAlias(stripped);
            if (alias != null) {
                out.putIfAbsent(alias, entry.getValue());
            }
        }
    }

    /**
     * Maps an FE-form storage key to its BE-canonical {@code AWS_*} alias,
     * or {@code null} when the key has no alias. Mirrors the lookups in
     * {@code be/src/util/s3_util.cpp} constants:
     * <pre>
     *   AWS_ACCESS_KEY / AWS_SECRET_KEY / AWS_ENDPOINT / AWS_REGION / AWS_TOKEN
     * </pre>
     */
    static String canonicalAwsAlias(String unprefixedKey) {
        return switch (unprefixedKey) {
            case "s3.access_key" -> "AWS_ACCESS_KEY";
            case "s3.secret_key" -> "AWS_SECRET_KEY";
            case "s3.endpoint" -> "AWS_ENDPOINT";
            case "s3.region" -> "AWS_REGION";
            case "s3.session_token" -> "AWS_TOKEN";
            default -> null;
        };
    }

    /**
     * Recognises the storage-credential keys our plugin forwards to the BE.
     * Covers Doris's primary FE-form ({@code s3.*}), the canonical
     * {@code AWS_*} aliases, the legacy {@code aws.*} form, and the
     * path-style toggle. Engine-injected DuckLake-specific keys
     * ({@code metadata.*}, {@code storage.warehouse}, {@code type},
     * {@code enable.mapping.varbinary}) are deliberately excluded — they're
     * either FE-only or carry no meaning on the BE.
     */
    static boolean isStorageProperty(String key) {
        return key.startsWith("s3.")
                || key.startsWith("AWS_")
                || key.startsWith("aws.")
                || key.startsWith("fs.")
                || key.equals("use_path_style");
    }

    private static String normalizeFileFormat(String catalogFormat) {
        // The catalog stores the format as-recorded by the writer
        // ("parquet" / "PARQUET"). Doris's BE reader dispatch is case-sensitive
        // on the file_format string in the thrift range descriptor.
        return catalogFormat == null ? "parquet" : catalogFormat.toLowerCase();
    }

    private static DuckLakeTableHandle asDuckLakeHandle(Object handle) {
        if (handle instanceof DuckLakeTableHandle dlh) {
            return dlh;
        }
        throw new IllegalArgumentException(
                "Expected DuckLakeTableHandle, got "
                        + (handle == null ? "null" : handle.getClass().getName()));
    }
}
