/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.brikk.ducklake.trino.plugin;

import com.google.inject.Inject;
import dev.brikk.ducklake.catalog.DucklakeCatalog;
import dev.brikk.ducklake.catalog.DucklakeSchema;
import dev.brikk.ducklake.catalog.DucklakeTable;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DucklakePathResolver
{
    private final DucklakeCatalog catalog;
    private final String configuredDataPath;

    @Inject
    public DucklakePathResolver(DucklakeCatalog catalog, DucklakeConfig config)
    {
        this(catalog, requireNonNull(config, "config is null").getDataPath());
    }

    public DucklakePathResolver(DucklakeCatalog catalog, String configuredDataPath)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.configuredDataPath = configuredDataPath;
    }

    public String resolveTableDataPath(DucklakeSchema schema, DucklakeTable table)
    {
        Optional<String> catalogDataPath = catalog.getDataPath();
        String rootDataPath = catalogDataPath.orElse(configuredDataPath);

        if (rootDataPath == null) {
            throw new IllegalStateException("No data path configured for relative file paths");
        }

        String schemaDataPath = resolveScopedPath(schema.path(), schema.pathIsRelative(), rootDataPath);
        return resolveScopedPath(table.path(), table.pathIsRelative(), schemaDataPath);
    }

    public String resolveFilePath(String path, boolean isRelative, String tableDataPath)
    {
        if (!isRelative) {
            return path;
        }
        return joinPaths(tableDataPath, path);
    }

    String resolveScopedPath(Optional<String> path, Optional<Boolean> isRelative, String parentPath)
    {
        if (path.isEmpty() || path.get().isBlank()) {
            return parentPath;
        }

        if (isRelative.orElse(false)) {
            return joinPaths(parentPath, path.get());
        }
        return path.get();
    }

    private static String joinPaths(String parent, String child)
    {
        if (parent.endsWith("/")) {
            return parent + child;
        }
        return parent + "/" + child;
    }
}
