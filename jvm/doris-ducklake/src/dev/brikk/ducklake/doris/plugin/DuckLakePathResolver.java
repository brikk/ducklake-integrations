package dev.brikk.ducklake.doris.plugin;

import java.util.Objects;
import java.util.Optional;

import dev.brikk.ducklake.catalog.DucklakeCatalog;
import dev.brikk.ducklake.catalog.DucklakeSchema;
import dev.brikk.ducklake.catalog.DucklakeTable;

/**
 * Resolves DuckLake's three-level scoped paths
 * (catalog data path &rarr; schema path &rarr; table path &rarr; per-file path)
 * into absolute storage URIs the BE Parquet reader can open.
 *
 * <p>Mirrors {@code dev.brikk.ducklake.trino.plugin.DucklakePathResolver} but
 * drops the Guice {@code @Inject} attachment so the plugin classloader stays
 * Guice-free.
 */
final class DuckLakePathResolver {

    private final DucklakeCatalog catalog;
    private final String configuredDataPath;

    DuckLakePathResolver(DucklakeCatalog catalog, String configuredDataPath) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.configuredDataPath = configuredDataPath;
    }

    String resolveTableDataPath(DucklakeSchema schema, DucklakeTable table) {
        Optional<String> catalogDataPath = catalog.getDataPath();
        String rootDataPath = catalogDataPath.orElse(configuredDataPath);
        if (rootDataPath == null) {
            throw new IllegalStateException("No data path configured for relative file paths");
        }
        String schemaDataPath = resolveScopedPath(schema.path(), schema.pathIsRelative(), rootDataPath);
        return resolveScopedPath(table.path(), table.pathIsRelative(), schemaDataPath);
    }

    String resolveFilePath(String path, boolean isRelative, String tableDataPath) {
        if (!isRelative) {
            return path;
        }
        return joinPaths(tableDataPath, path);
    }

    static String resolveScopedPath(Optional<String> path,
                                    Optional<Boolean> isRelative,
                                    String parentPath) {
        if (path.isEmpty() || path.get().isBlank()) {
            return parentPath;
        }
        if (isRelative.orElse(false)) {
            return joinPaths(parentPath, path.get());
        }
        return path.get();
    }

    private static String joinPaths(String parent, String child) {
        if (parent.endsWith("/")) {
            return parent + child;
        }
        return parent + "/" + child;
    }
}
