package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable

/**
 * Resolves DuckLake's three-level scoped paths
 * (catalog data path &rarr; schema path &rarr; table path &rarr; per-file path)
 * into absolute storage URIs the BE Parquet reader can open.
 *
 * Mirrors `dev.brikk.ducklake.trino.plugin.DucklakePathResolver` but
 * drops the Guice `@Inject` attachment so the plugin classloader stays
 * Guice-free.
 */
internal class DuckLakePathResolver(
    private val catalog: DucklakeCatalog,
    private val configuredDataPath: String?,
) {

    /** The catalog warehouse root — the scan root for a catalog-wide sweep. */
    fun rootDataPath(): String =
        (catalog.getDataPath() ?: configuredDataPath)
            ?: throw IllegalStateException("No data path configured for relative file paths")

    /** A schema's data dir (root + schema.path) — the scan root for a schema-wide sweep. */
    fun resolveSchemaDataPath(schema: DucklakeSchema): String =
        resolveScopedPath(schema.path, schema.pathIsRelative, rootDataPath())

    fun resolveTableDataPath(schema: DucklakeSchema, table: DucklakeTable): String =
        resolveScopedPath(table.path, table.pathIsRelative, resolveSchemaDataPath(schema))

    fun resolveFilePath(path: String, isRelative: Boolean, tableDataPath: String): String {
        if (!isRelative) {
            return path
        }
        return joinPaths(tableDataPath, path)
    }

    companion object {
        fun resolveScopedPath(
            path: String?,
            isRelative: Boolean?,
            parentPath: String,
        ): String {
            if (path == null || path.isBlank()) {
                return parentPath
            }
            if (isRelative ?: false) {
                return joinPaths(parentPath, path)
            }
            return path
        }

        private fun joinPaths(parent: String, child: String): String {
            if (parent.endsWith("/")) {
                return parent + child
            }
            return "$parent/$child"
        }
    }
}
