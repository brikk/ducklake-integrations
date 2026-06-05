package dev.brikk.ducklake.doris.plugin;

import java.io.Serializable;
import java.util.Objects;

/**
 * Position-delete file descriptor carried on a {@link DuckLakeScanRange}.
 *
 * <p>DuckLake's catalog enforces at most one active position-delete file per
 * data file at any given snapshot — {@code JdbcDucklakeCatalog#checkDeleteFileOverlap}
 * raises {@code LogicalConflictException} when two transactions race on the
 * same {@code data_file_id}. The wire format ({@code iceberg_params.delete_files})
 * is a list to leave headroom for that invariant relaxing later; today every
 * range carries either zero or one of these.
 *
 * <p>{@code path} is already resolved to an absolute URI by
 * {@link DuckLakePathResolver} before construction; the BE reader consumes it
 * verbatim. {@code fileFormat} is lower-cased to match the BE's case-sensitive
 * format dispatch.
 */
record DuckLakePositionDelete(String path, String fileFormat) implements Serializable {

    private static final long serialVersionUID = 1L;

    DuckLakePositionDelete {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(fileFormat, "fileFormat");
    }
}
