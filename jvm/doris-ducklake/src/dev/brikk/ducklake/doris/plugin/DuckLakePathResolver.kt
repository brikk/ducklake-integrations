package dev.brikk.ducklake.doris.plugin

import java.util.Optional

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

    fun resolveTableDataPath(schema: DucklakeSchema, table: DucklakeTable): String {
        val rootDataPath = (catalog.getDataPath() ?: configuredDataPath)
            ?: throw IllegalStateException("No data path configured for relative file paths")
        val schemaDataPath = resolveScopedPath(schema.path, schema.pathIsRelative, rootDataPath)
        return resolveScopedPath(table.path, table.pathIsRelative, schemaDataPath)
    }

    fun resolveFilePath(path: String, isRelative: Boolean, tableDataPath: String): String {
        if (!isRelative) {
            return path
        }
        return joinPaths(tableDataPath, path)
    }

    companion object {
        @JvmStatic
        fun resolveScopedPath(
            path: Optional<String>,
            isRelative: Optional<Boolean>,
            parentPath: String,
        ): String {
            if (path.isEmpty || path.get().isBlank()) {
                return parentPath
            }
            if (isRelative.orElse(false)) {
                return joinPaths(parentPath, path.get())
            }
            return path.get()
        }

        private fun joinPaths(parent: String, child: String): String {
            if (parent.endsWith("/")) {
                return parent + child
            }
            return "$parent/$child"
        }
    }
}
