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

import com.google.inject.Binder
import com.google.inject.Module
import com.google.inject.Scopes
import com.google.inject.multibindings.Multibinder
import com.google.inject.multibindings.OptionalBinder.newOptionalBinder
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment
import dev.brikk.ducklake.catalog.DucklakeWriteFragment
import io.airlift.configuration.ConfigBinder.configBinder
import io.airlift.json.JsonCodecBinder.jsonCodecBinder
import io.trino.filesystem.cache.SplitAffinityProvider
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProviderFactory
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager
import io.trino.plugin.base.classloader.ForClassLoaderSafe
import io.trino.plugin.base.metrics.FileFormatDataSourceStats
import io.trino.plugin.hive.parquet.ParquetReaderConfig
import io.trino.plugin.hive.parquet.ParquetWriterConfig
import io.trino.spi.connector.ConnectorPageSinkProvider
import io.trino.spi.connector.ConnectorPageSourceProviderFactory
import io.trino.spi.connector.ConnectorSplitManager
import io.trino.spi.function.table.ConnectorTableFunction
import io.trino.spi.procedure.Procedure
import org.weakref.jmx.guice.ExportBinder.newExporter

class DucklakeModule(catalogConfig: Map<String, String>) : Module {
    private val catalogConfig: Map<String, String> = catalogConfig.toMap()

    override fun configure(binder: Binder) {
        // Configuration
        configBinder(binder).bindConfig(DucklakeConfig::class.java)
        configBinder(binder).bindConfig(ParquetReaderConfig::class.java)
        configBinder(binder).bindConfig(ParquetWriterConfig::class.java)

        // Snapshot of the S3 settings the user gave the FileSystemModule, also needed for
        // DuckDB's httpfs extension (N2). Bound from the raw catalog map rather than via
        // a parallel @Config class so the user only configures s3 once.
        binder.bind(DuckDbS3Config::class.java).toInstance(DuckDbS3Config.fromCatalogConfig(catalogConfig))

        // Core connector components
        binder.bind(DucklakeConnector::class.java).`in`(Scopes.SINGLETON)
        binder.bind(DucklakeTransactionManager::class.java).`in`(Scopes.SINGLETON)
        binder.bind(DucklakeMetadataFactory::class.java).`in`(Scopes.SINGLETON)
        binder.bind(DucklakeSnapshotResolver::class.java).`in`(Scopes.SINGLETON)
        binder.bind(DucklakeSessionProperties::class.java).`in`(Scopes.SINGLETON)

        // Catalog
        binder.bind(DucklakeCatalog::class.java).toProvider(DucklakeCatalogProvider::class.java).`in`(Scopes.SINGLETON)

        // File system factory
        binder.bind(DucklakeFileSystemFactory::class.java).to(DefaultDucklakeFileSystemFactory::class.java).`in`(Scopes.SINGLETON)

        // Path resolver
        binder.bind(DucklakePathResolver::class.java).`in`(Scopes.SINGLETON)

        // Always-on split affinity. FileSystemModule's setDefault() binds Noop; when
        // fs.cache.enabled=true the Alluxio module's setBinding() supersedes both. We
        // only install our setBinding() when caching is off — same key shape, so
        // behavior is identical either way; the operator just doesn't have to configure
        // fs.cache.directories etc. to get node pinning for DuckDB-format reads (which
        // bypass TrinoFileSystem and gain nothing from the cache itself).
        if (!java.lang.Boolean.parseBoolean(catalogConfig.getOrDefault("fs.cache.enabled", "false"))) {
            newOptionalBinder(binder, SplitAffinityProvider::class.java)
                    .setBinding()
                    .to(DucklakeAlwaysOnSplitAffinityProvider::class.java)
                    .`in`(Scopes.SINGLETON)
        }

        // Split manager
        binder.bind(ConnectorSplitManager::class.java)
                .annotatedWith(ForClassLoaderSafe::class.java)
                .to(DucklakeSplitManager::class.java)
                .`in`(Scopes.SINGLETON)
        binder.bind(ConnectorSplitManager::class.java)
                .to(ClassLoaderSafeConnectorSplitManager::class.java)
                .`in`(Scopes.SINGLETON)

        // Page source provider
        binder.bind(ConnectorPageSourceProviderFactory::class.java)
                .annotatedWith(ForClassLoaderSafe::class.java)
                .to(DucklakePageSourceProviderFactory::class.java)
                .`in`(Scopes.SINGLETON)
        binder.bind(DucklakePageSourceProviderFactory::class.java).`in`(Scopes.SINGLETON)
        binder.bind(ConnectorPageSourceProviderFactory::class.java)
                .to(ClassLoaderSafeConnectorPageSourceProviderFactory::class.java)
                .`in`(Scopes.SINGLETON)

        // Page sink provider
        binder.bind(ConnectorPageSinkProvider::class.java)
                .annotatedWith(ForClassLoaderSafe::class.java)
                .to(DucklakePageSinkProvider::class.java)
                .`in`(Scopes.SINGLETON)
        binder.bind(ConnectorPageSinkProvider::class.java)
                .to(ClassLoaderSafeConnectorPageSinkProvider::class.java)
                .`in`(Scopes.SINGLETON)

        // Write fragment codecs
        jsonCodecBinder(binder).bindJsonCodec(DucklakeWriteFragment::class.java)
        jsonCodecBinder(binder).bindJsonCodec(DucklakeDeleteFragment::class.java)

        // Parquet reader configuration and stats
        binder.bind(FileFormatDataSourceStats::class.java).`in`(Scopes.SINGLETON)
        newExporter(binder).export(FileFormatDataSourceStats::class.java).withGeneratedName()

        // Type converter
        binder.bind(DucklakeTypeConverter::class.java).`in`(Scopes.SINGLETON)

        // Table properties
        binder.bind(DucklakeTableProperties::class.java).`in`(Scopes.SINGLETON)

        // DuckDB-format read cache (per-JVM)
        binder.bind(DucklakeMaterializedFileCache::class.java).`in`(Scopes.SINGLETON)

        // DuckDB execution-engine factory (selects InProcess / Quack / Swanlake
        // per ducklake.execution-engine config).
        binder.bind(DucklakeDuckDbExecutorFactory::class.java).`in`(Scopes.SINGLETON)

        // Procedures (per-catalog, exposed under <catalog>.system.<name>).
        bindProcedures(binder)

        // Table functions (per-catalog, invoked as TABLE(<catalog>.system.<name>(...))).
        // DucklakeFunctionProvider routes each function's handle to its split processor.
        val tableFunctionBinder = Multibinder.newSetBinder(binder, ConnectorTableFunction::class.java)
        tableFunctionBinder.addBinding().to(LanceVectorSearchTableFunction::class.java).`in`(Scopes.SINGLETON)
        tableFunctionBinder.addBinding().to(LanceFtsTableFunction::class.java).`in`(Scopes.SINGLETON)
        tableFunctionBinder.addBinding().to(LanceHybridSearchTableFunction::class.java).`in`(Scopes.SINGLETON)
        binder.bind(DucklakeFunctionProvider::class.java).`in`(Scopes.SINGLETON)
    }

    private fun bindProcedures(binder: com.google.inject.Binder) {
        val procedureBinder = Multibinder.newSetBinder(binder, Procedure::class.java)
        procedureBinder.addBinding().toProvider(DucklakeAddFilesProcedure::class.java).`in`(Scopes.SINGLETON)
        procedureBinder.addBinding().toProvider(DucklakeFlushInlinedDataProcedure::class.java).`in`(Scopes.SINGLETON)
        procedureBinder.addBinding().toProvider(DucklakeRemoveOrphanFilesProcedure::class.java).`in`(Scopes.SINGLETON)
        procedureBinder.addBinding().toProvider(DucklakeExpireSnapshotsProcedure::class.java).`in`(Scopes.SINGLETON)
        procedureBinder.addBinding().toProvider(DucklakeCleanupOldFilesProcedure::class.java).`in`(Scopes.SINGLETON)
    }
}
