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
package dev.brikk.ducklake.trino.plugin

import com.google.inject.Inject
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeSchema
import dev.brikk.ducklake.catalog.DucklakeTable

import java.util.Optional

open class DucklakePathResolver(
        private val catalog: DucklakeCatalog,
        private val configuredDataPath: String?)
{
    @Inject
    constructor(catalog: DucklakeCatalog, config: DucklakeConfig)
            : this(catalog, config.getDataPath())

    open fun resolveTableDataPath(schema: DucklakeSchema, table: DucklakeTable): String
    {
        val catalogDataPath: Optional<String> = catalog.getDataPath()
        val rootDataPath: String = catalogDataPath.orElse(configuredDataPath)
            ?: throw IllegalStateException("No data path configured for relative file paths")

        val schemaDataPath = resolveScopedPath(schema.path, schema.pathIsRelative, rootDataPath)
        return resolveScopedPath(table.path, table.pathIsRelative, schemaDataPath)
    }

    open fun resolveFilePath(path: String, isRelative: Boolean, tableDataPath: String): String
    {
        if (!isRelative) {
            return path
        }
        return joinPaths(tableDataPath, path)
    }

    fun resolveScopedPath(path: Optional<String>, isRelative: Optional<Boolean>, parentPath: String): String
    {
        if (path.isEmpty || path.get().isBlank()) {
            return parentPath
        }

        if (isRelative.orElse(false)) {
            return joinPaths(parentPath, path.get())
        }
        return path.get()
    }

    companion object {
        private fun joinPaths(parent: String, child: String): String
        {
            if (parent.endsWith("/")) {
                return "$parent$child"
            }
            return "$parent/$child"
        }
    }
}
