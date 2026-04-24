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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProviderFactory;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ForClassLoaderSafe;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import dev.brikk.ducklake.catalog.DucklakeCatalog;
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment;
import dev.brikk.ducklake.catalog.DucklakeWriteFragment;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.ConnectorSplitManager;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class DucklakeModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        // Configuration
        configBinder(binder).bindConfig(DucklakeConfig.class);
        configBinder(binder).bindConfig(ParquetReaderConfig.class);
        configBinder(binder).bindConfig(ParquetWriterConfig.class);

        // Core connector components
        binder.bind(DucklakeConnector.class).in(Scopes.SINGLETON);
        binder.bind(DucklakeTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(DucklakeMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(DucklakeSnapshotResolver.class).in(Scopes.SINGLETON);
        binder.bind(DucklakeSessionProperties.class).in(Scopes.SINGLETON);

        // Catalog
        binder.bind(DucklakeCatalog.class).toProvider(DucklakeCatalogProvider.class).in(Scopes.SINGLETON);

        // File system factory
        binder.bind(DucklakeFileSystemFactory.class).to(DefaultDucklakeFileSystemFactory.class).in(Scopes.SINGLETON);

        // Path resolver
        binder.bind(DucklakePathResolver.class).in(Scopes.SINGLETON);

        // Split manager
        binder.bind(ConnectorSplitManager.class)
                .annotatedWith(ForClassLoaderSafe.class)
                .to(DucklakeSplitManager.class)
                .in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class)
                .to(ClassLoaderSafeConnectorSplitManager.class)
                .in(Scopes.SINGLETON);

        // Page source provider
        binder.bind(ConnectorPageSourceProviderFactory.class)
                .annotatedWith(ForClassLoaderSafe.class)
                .to(DucklakePageSourceProviderFactory.class)
                .in(Scopes.SINGLETON);
        binder.bind(DucklakePageSourceProviderFactory.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProviderFactory.class)
                .to(ClassLoaderSafeConnectorPageSourceProviderFactory.class)
                .in(Scopes.SINGLETON);

        // Page sink provider
        binder.bind(ConnectorPageSinkProvider.class)
                .annotatedWith(ForClassLoaderSafe.class)
                .to(DucklakePageSinkProvider.class)
                .in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class)
                .to(ClassLoaderSafeConnectorPageSinkProvider.class)
                .in(Scopes.SINGLETON);

        // Write fragment codecs
        jsonCodecBinder(binder).bindJsonCodec(DucklakeWriteFragment.class);
        jsonCodecBinder(binder).bindJsonCodec(DucklakeDeleteFragment.class);

        // Parquet reader configuration and stats
        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        // Type converter
        binder.bind(DucklakeTypeConverter.class).in(Scopes.SINGLETON);

        // Table properties
        binder.bind(DucklakeTableProperties.class).in(Scopes.SINGLETON);
    }
}
