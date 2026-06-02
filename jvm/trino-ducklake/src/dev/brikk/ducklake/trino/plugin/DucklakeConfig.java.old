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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import dev.brikk.ducklake.catalog.DucklakeCatalogConfig;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import io.airlift.units.DataSize;
import io.airlift.units.MinDataSize;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Configuration for Ducklake connector.
 */
public class DucklakeConfig
{
    private String catalogDatabaseUrl;
    private String catalogDatabaseUser;
    private String catalogDatabasePassword;
    private String dataPath;
    private int maxCatalogConnections = 10;
    private OptionalLong defaultSnapshotId = OptionalLong.empty();
    private Optional<Instant> defaultSnapshotTimestamp = Optional.empty();
    private DucklakeTemporalPartitionEncoding temporalPartitionEncoding = DucklakeTemporalPartitionEncoding.CALENDAR;
    private boolean temporalPartitionEncodingReadLeniency = true;
    private DataSize duckdbAutoHttpfsThreshold = DataSize.ofBytes(64L * 1024 * 1024);
    private DucklakeExecutionEngine executionEngine = DucklakeExecutionEngine.DUCKDB_LOCAL;
    private String quackHost;
    private int quackPort = 9494;
    private String quackToken;
    private DataSize duckdbMemoryLimit;
    private Integer duckdbThreads;
    private String duckdbTempDirectory;
    private DataSize duckdbTempDirectoryMaxSize;
    private boolean duckdbEnableObjectCache = true;
    private DataSize duckdbTargetWriteBytes = DataSize.ofBytes(512L * 1024 * 1024);
    private Optional<String> duckdbParityExtensionPath = Optional.empty();

    @NotNull
    public String getCatalogDatabaseUrl()
    {
        return catalogDatabaseUrl;
    }

    @Config("ducklake.catalog.database-url")
    @ConfigDescription("JDBC URL for the Ducklake catalog database (e.g., jdbc:postgresql://host/db)")
    public DucklakeConfig setCatalogDatabaseUrl(String catalogDatabaseUrl)
    {
        this.catalogDatabaseUrl = catalogDatabaseUrl;
        return this;
    }

    public String getCatalogDatabaseUser()
    {
        return catalogDatabaseUser;
    }

    @Config("ducklake.catalog.database-user")
    @ConfigDescription("Username for the catalog database (required for PostgreSQL)")
    public DucklakeConfig setCatalogDatabaseUser(String catalogDatabaseUser)
    {
        this.catalogDatabaseUser = catalogDatabaseUser;
        return this;
    }

    public String getCatalogDatabasePassword()
    {
        return catalogDatabasePassword;
    }

    @Config("ducklake.catalog.database-password")
    @ConfigDescription("Password for the catalog database (required for PostgreSQL)")
    public DucklakeConfig setCatalogDatabasePassword(String catalogDatabasePassword)
    {
        this.catalogDatabasePassword = catalogDatabasePassword;
        return this;
    }

    public String getDataPath()
    {
        return dataPath;
    }

    @Config("ducklake.data-path")
    @ConfigDescription("Base path for relative data file paths (from ducklake_metadata table)")
    public DucklakeConfig setDataPath(String dataPath)
    {
        this.dataPath = dataPath;
        return this;
    }

    public int getMaxCatalogConnections()
    {
        return maxCatalogConnections;
    }

    @Config("ducklake.catalog.max-connections")
    @ConfigDescription("Maximum number of JDBC connections to the catalog database")
    public DucklakeConfig setMaxCatalogConnections(int maxCatalogConnections)
    {
        this.maxCatalogConnections = maxCatalogConnections;
        return this;
    }

    public OptionalLong getDefaultSnapshotId()
    {
        return defaultSnapshotId;
    }

    @Config("ducklake.default-snapshot-id")
    @ConfigDescription("Optional default DuckLake snapshot ID for reads when query/session does not specify a snapshot")
    public DucklakeConfig setDefaultSnapshotId(Long defaultSnapshotId)
    {
        this.defaultSnapshotId = defaultSnapshotId == null ? OptionalLong.empty() : OptionalLong.of(defaultSnapshotId);
        return this;
    }

    public Optional<Instant> getDefaultSnapshotTimestamp()
    {
        return defaultSnapshotTimestamp;
    }

    @Config("ducklake.default-snapshot-timestamp")
    @ConfigDescription("Optional default DuckLake snapshot timestamp (ISO-8601 instant) for reads when query/session does not specify a snapshot")
    public DucklakeConfig setDefaultSnapshotTimestamp(String defaultSnapshotTimestamp)
    {
        if (defaultSnapshotTimestamp == null) {
            this.defaultSnapshotTimestamp = Optional.empty();
            return this;
        }

        try {
            this.defaultSnapshotTimestamp = Optional.of(Instant.parse(defaultSnapshotTimestamp));
        }
        catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid ducklake.default-snapshot-timestamp value: " + defaultSnapshotTimestamp, e);
        }
        return this;
    }

    @NotNull
    @Deprecated
    public DucklakeTemporalPartitionEncoding getTemporalPartitionEncoding()
    {
        return temporalPartitionEncoding;
    }

    /**
     * @deprecated DuckLake 1.0 settled on calendar encoding for {@code
     * ducklake_file_partition_value.partition_value} (spec PR
     * <a href="https://github.com/duckdb/ducklake-web/pull/349">duckdb/ducklake-web#349</a>);
     * the epoch path is retained but no longer expected. Leave on the default
     * ({@code calendar}) for spec-conformant catalogs. Will be removed once a future
     * spec revision either confirms calendar permanently or formally reintroduces
     * epoch as an alternate.
     */
    @Deprecated
    @Config("ducklake.temporal-partition-encoding")
    @ConfigDescription("Deprecated. DuckLake 1.0 settled on calendar; epoch path retained for compatibility with pre-resolution catalogs. Values: calendar (default, spec-conformant) or epoch.")
    public DucklakeConfig setTemporalPartitionEncoding(String temporalPartitionEncoding)
    {
        if (temporalPartitionEncoding == null) {
            this.temporalPartitionEncoding = DucklakeTemporalPartitionEncoding.CALENDAR;
            return this;
        }

        try {
            this.temporalPartitionEncoding = DucklakeTemporalPartitionEncoding.fromString(temporalPartitionEncoding);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid ducklake.temporal-partition-encoding value: " + temporalPartitionEncoding + " (expected: calendar or epoch)", e);
        }
        return this;
    }

    @Deprecated
    public boolean isTemporalPartitionEncodingReadLeniency()
    {
        return temporalPartitionEncodingReadLeniency;
    }

    /**
     * @deprecated Companion to {@link #setTemporalPartitionEncoding(String)}; see that
     * setter's note for the DuckLake 1.0 spec resolution. Leniency only matters when
     * epoch and calendar interpretations diverge, which a v1-conformant catalog never
     * triggers.
     */
    @Deprecated
    @Config("ducklake.temporal-partition-encoding-read-leniency")
    @ConfigDescription("Deprecated. If true, temporal partition pruning keeps files unless both calendar and epoch interpretations exclude them. Only relevant when reading mixed-encoding catalogs from before the DuckLake 1.0 calendar-only resolution.")
    public DucklakeConfig setTemporalPartitionEncodingReadLeniency(boolean temporalPartitionEncodingReadLeniency)
    {
        this.temporalPartitionEncodingReadLeniency = temporalPartitionEncodingReadLeniency;
        return this;
    }

    @AssertTrue(message = "Only one of ducklake.default-snapshot-id or ducklake.default-snapshot-timestamp may be set")
    public boolean isSnapshotDefaultsValid()
    {
        return defaultSnapshotId.isEmpty() || defaultSnapshotTimestamp.isEmpty();
    }

    @NotNull
    @MinDataSize("0B")
    public DataSize getDuckdbAutoHttpfsThreshold()
    {
        return duckdbAutoHttpfsThreshold;
    }

    @Config("ducklake.duckdb.auto-httpfs-threshold")
    @ConfigDescription("Size threshold for the 'auto' duckdb_read_mode: data files at or above this size stream via DuckDB's httpfs extension; smaller files materialize to local tmp first. Ignored when duckdb_read_mode is set explicitly to 'materialize' or 'httpfs'.")
    public DucklakeConfig setDuckdbAutoHttpfsThreshold(DataSize duckdbAutoHttpfsThreshold)
    {
        this.duckdbAutoHttpfsThreshold = duckdbAutoHttpfsThreshold;
        return this;
    }

    @NotNull
    public DucklakeExecutionEngine getExecutionEngine()
    {
        return executionEngine;
    }

    @Config("ducklake.execution-engine")
    @ConfigDescription("Where the DuckDB-format read path executes: 'duckdb_local' (default — embedded DuckDB in the Trino worker JVM), 'quack' (out-of-process DuckDB reached over the Quack RPC protocol), or 'swanlake' (out-of-process DuckDB over Arrow Flight SQL — reserved). For 'quack', set ducklake.quack.host/port/token. The .db file must be reachable by the chosen engine — for 'quack' that means either a shared local path (multi-container pod) or s3:// access (not yet implemented on the quack path).")
    public DucklakeConfig setExecutionEngine(DucklakeExecutionEngine executionEngine)
    {
        this.executionEngine = executionEngine;
        return this;
    }

    public String getQuackHost()
    {
        return quackHost;
    }

    @Config("ducklake.quack.host")
    @ConfigDescription("Hostname of the Quack-serving DuckDB instance used when ducklake.execution-engine=quack. Required for the quack engine; ignored otherwise.")
    public DucklakeConfig setQuackHost(String quackHost)
    {
        this.quackHost = quackHost;
        return this;
    }

    public int getQuackPort()
    {
        return quackPort;
    }

    @Config("ducklake.quack.port")
    @ConfigDescription("Quack server port. Defaults to 9494.")
    public DucklakeConfig setQuackPort(int quackPort)
    {
        this.quackPort = quackPort;
        return this;
    }

    public String getQuackToken()
    {
        return quackToken;
    }

    @Config("ducklake.quack.token")
    @ConfigDescription("Quack server auth token. Required when ducklake.execution-engine=quack. Min 4 characters (Quack server-side requirement).")
    public DucklakeConfig setQuackToken(String quackToken)
    {
        this.quackToken = quackToken;
        return this;
    }

    public DataSize getDuckdbMemoryLimit()
    {
        return duckdbMemoryLimit;
    }

    @Config("ducklake.duckdb.memory-limit")
    @ConfigDescription("DuckDB engine memory_limit (per-split for duckdb_local; instance-wide for quack). Unset by default.")
    public DucklakeConfig setDuckdbMemoryLimit(DataSize duckdbMemoryLimit)
    {
        this.duckdbMemoryLimit = duckdbMemoryLimit;
        return this;
    }

    public Integer getDuckdbThreads()
    {
        return duckdbThreads;
    }

    @Config("ducklake.duckdb.threads")
    @ConfigDescription("DuckDB engine thread count. Unset by default (DuckDB picks per CPU count).")
    public DucklakeConfig setDuckdbThreads(Integer duckdbThreads)
    {
        this.duckdbThreads = duckdbThreads;
        return this;
    }

    public String getDuckdbTempDirectory()
    {
        return duckdbTempDirectory;
    }

    @Config("ducklake.duckdb.temp-directory")
    @ConfigDescription("DuckDB temp_directory (spill location). Unset by default.")
    public DucklakeConfig setDuckdbTempDirectory(String duckdbTempDirectory)
    {
        this.duckdbTempDirectory = duckdbTempDirectory;
        return this;
    }

    public DataSize getDuckdbTempDirectoryMaxSize()
    {
        return duckdbTempDirectoryMaxSize;
    }

    @Config("ducklake.duckdb.temp-directory-max-size")
    @ConfigDescription("DuckDB max_temp_directory_size (cap on spill usage). Unset by default.")
    public DucklakeConfig setDuckdbTempDirectoryMaxSize(DataSize duckdbTempDirectoryMaxSize)
    {
        this.duckdbTempDirectoryMaxSize = duckdbTempDirectoryMaxSize;
        return this;
    }

    public boolean isDuckdbEnableObjectCache()
    {
        return duckdbEnableObjectCache;
    }

    @Config("ducklake.duckdb.enable-object-cache")
    @ConfigDescription("Whether to enable DuckDB's enable_object_cache (parquet metadata reuse across queries). Default true.")
    public DucklakeConfig setDuckdbEnableObjectCache(boolean duckdbEnableObjectCache)
    {
        this.duckdbEnableObjectCache = duckdbEnableObjectCache;
        return this;
    }

    @NotNull
    @MinDataSize("1kB")
    public DataSize getDuckdbTargetWriteBytes()
    {
        return duckdbTargetWriteBytes;
    }

    @Config("ducklake.duckdb.target-write-bytes")
    @ConfigDescription("Approximate logical input bytes appended before the DuckDB-format writer rolls to a new .db file. On-disk size will be smaller depending on data compressibility — typically 3-5×.")
    public DucklakeConfig setDuckdbTargetWriteBytes(DataSize duckdbTargetWriteBytes)
    {
        this.duckdbTargetWriteBytes = duckdbTargetWriteBytes;
        return this;
    }

    public DuckDbTuning toDuckDbTuning()
    {
        return new DuckDbTuning(
                Optional.ofNullable(duckdbMemoryLimit),
                duckdbThreads == null ? java.util.OptionalInt.empty() : java.util.OptionalInt.of(duckdbThreads),
                Optional.ofNullable(duckdbTempDirectory),
                Optional.ofNullable(duckdbTempDirectoryMaxSize),
                duckdbEnableObjectCache);
    }

    @AssertTrue(message = "ducklake.quack.host and ducklake.quack.token must be set when ducklake.execution-engine=quack")
    public boolean isQuackEngineConfigComplete()
    {
        if (executionEngine != DucklakeExecutionEngine.QUACK) {
            return true;
        }
        return quackHost != null && !quackHost.isBlank()
                && quackToken != null && !quackToken.isBlank();
    }

    public DucklakeCatalogConfig toCatalogConfig()
    {
        return new DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(catalogDatabaseUrl)
                .setCatalogDatabaseUser(catalogDatabaseUser)
                .setCatalogDatabasePassword(catalogDatabasePassword)
                .setDataPath(dataPath)
                .setMaxCatalogConnections(maxCatalogConnections);
    }

    public Optional<String> getDuckdbParityExtensionPath()
    {
        return duckdbParityExtensionPath;
    }

    @Config("ducklake.duckdb.parity-extension-path")
    @ConfigDescription("Filesystem path to the trino_parity.duckdb_extension binary (https://github.com/brikk/duckdb-trino-parity-extension). When set, executors LOAD this extension on every DuckDB attach instead of parsing the in-tree trino-function-aliases.sql resource — faster per-split setup, and the only source of truth for the trino_<name> macros + trino_meta() catalog. If unset, falls back to the in-tree SQL replay (current default). The path is passed to DuckDB's LOAD '<path>'; allow_unsigned_extensions is enabled before LOAD.")
    public DucklakeConfig setDuckdbParityExtensionPath(String duckdbParityExtensionPath)
    {
        this.duckdbParityExtensionPath = Optional.ofNullable(duckdbParityExtensionPath)
                .map(String::strip)
                .filter(s -> !s.isEmpty());
        return this;
    }
}
