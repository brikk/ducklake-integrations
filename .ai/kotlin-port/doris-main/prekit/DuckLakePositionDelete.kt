package dev.brikk.ducklake.doris.plugin

import java.io.Serializable
import java.util.Objects

/**
 * Position-delete file descriptor carried on a [DuckLakeScanRange].
 *
 * DuckLake's catalog enforces at most one active position-delete file per
 * data file at any given snapshot — `JdbcDucklakeCatalog#checkDeleteFileOverlap`
 * raises `LogicalConflictException` when two transactions race on the
 * same `data_file_id`. The wire format (`iceberg_params.delete_files`)
 * is a list to leave headroom for that invariant relaxing later; today every
 * range carries either zero or one of these.
 *
 * `path` is already resolved to an absolute URI by
 * [DuckLakePathResolver] before construction; the BE reader consumes it
 * verbatim. `fileFormat` is lower-cased to match the BE's case-sensitive
 * format dispatch.
 */
internal data class DuckLakePositionDelete(
    @get:JvmName("path") val path: String,
    @get:JvmName("fileFormat") val fileFormat: String,
) : Serializable {

    init {
        Objects.requireNonNull(path, "path")
        Objects.requireNonNull(fileFormat, "fileFormat")
    }

    companion object {
        private const val serialVersionUID: Long = 1L
    }
}
