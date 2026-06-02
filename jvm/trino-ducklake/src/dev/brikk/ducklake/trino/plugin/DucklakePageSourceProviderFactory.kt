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
import io.trino.parquet.ParquetReaderOptions
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import dev.brikk.ducklake.catalog.DucklakeCatalog
import io.trino.plugin.hive.parquet.ParquetReaderConfig
import io.trino.spi.connector.ConnectorPageSourceProviderFactory

open class DucklakePageSourceProviderFactory @Inject constructor(
        fileSystemFactory: DucklakeFileSystemFactory,
        fileFormatDataSourceStats: FileFormatDataSourceStats,
        parquetReaderConfig: ParquetReaderConfig,
        catalog: DucklakeCatalog,
        duckDbReadCache: DucklakeMaterializedFileCache,
        duckDbS3Config: DuckDbS3Config,
        ducklakeConfig: DucklakeConfig,
        executorFactory: DucklakeDuckDbExecutorFactory)
        : ConnectorPageSourceProviderFactory
{
    private val fileSystemFactory: DucklakeFileSystemFactory = fileSystemFactory
    private val fileFormatDataSourceStats: FileFormatDataSourceStats = fileFormatDataSourceStats
    private val parquetReaderOptions: ParquetReaderOptions = parquetReaderConfig.toParquetReaderOptions()
    private val catalog: DucklakeCatalog = catalog
    private val duckDbReadCache: DucklakeMaterializedFileCache = duckDbReadCache
    private val duckDbS3Config: DuckDbS3Config = duckDbS3Config
    private val ducklakeConfig: DucklakeConfig = ducklakeConfig
    private val executorFactory: DucklakeDuckDbExecutorFactory = executorFactory

    override fun createPageSourceProvider(): DucklakePageSourceProvider
    {
        return DucklakePageSourceProvider(fileSystemFactory, fileFormatDataSourceStats, parquetReaderOptions, catalog, duckDbReadCache, duckDbS3Config, ducklakeConfig, executorFactory)
    }
}
