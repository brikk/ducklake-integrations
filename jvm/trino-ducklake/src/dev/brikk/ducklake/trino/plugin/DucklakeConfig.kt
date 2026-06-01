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

import dev.brikk.ducklake.catalog.DucklakeCatalogConfig
import io.airlift.configuration.Config
import io.airlift.configuration.ConfigDescription
import io.airlift.units.DataSize
import io.airlift.units.MinDataSize
import jakarta.validation.constraints.AssertTrue
import jakarta.validation.constraints.NotNull
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.Optional
import java.util.OptionalLong

/**
 * Configuration for Ducklake connector.
 */
public class DucklakeConfig {
    private var catalogDatabaseUrl: String? = null
    private var catalogDatabaseUser: String? = null
    private var catalogDatabasePassword: String? = null
    private var dataPath: String? = null
    private var maxCatalogConnections: Int = 10
    private var defaultSnapshotId: OptionalLong = OptionalLong.empty()
    private var defaultSnapshotTimestamp: Optional<Instant> = Optional.empty()
    private var temporalPartitionEncoding: DucklakeTemporalPartitionEncoding = DucklakeTemporalPartitionEncoding.CALENDAR
    private var temporalPartitionEncodingReadLeniency: Boolean = true
    private var duckdbAutoHttpfsThreshold: DataSize = DataSize.ofBytes(64L * 1024 * 1024)
    private var executionEngine: DucklakeExecutionEngine = DucklakeExecutionEngine.DUCKDB_LOCAL
    private var quackHost: String? = null
    private var quackPort: Int = 9494
    private var quackToken: String? = null
    private var duckdbMemoryLimit: DataSize? = null
    private var duckdbThreads: Int? = null
    private var duckdbTempDirectory: String? = null
    private var duckdbTempDirectoryMaxSize: DataSize? = null
    private var duckdbEnableObjectCache: Boolean = true
    private var duckdbTargetWriteBytes: DataSize = DataSize.ofBytes(512L * 1024 * 1024)
    private var duckdbParityExtensionPath: Optional<String> = Optional.empty()

    @NotNull
    fun getCatalogDatabaseUrl(): String? {
        return catalogDatabaseUrl
    }

    @Config("ducklake.catalog.database-url")
    @ConfigDescription("JDBC URL for the Ducklake catalog database (e.g., jdbc:postgresql://host/db)")
    fun setCatalogDatabaseUrl(catalogDatabaseUrl: String?): DucklakeConfig {
        this.catalogDatabaseUrl = catalogDatabaseUrl
        return this
    }

    fun getCatalogDatabaseUser(): String? {
        return catalogDatabaseUser
    }

    @Config("ducklake.catalog.database-user")
    @ConfigDescription("Username for the catalog database (required for PostgreSQL)")
    fun setCatalogDatabaseUser(catalogDatabaseUser: String?): DucklakeConfig {
        this.catalogDatabaseUser = catalogDatabaseUser
        return this
    }

    fun getCatalogDatabasePassword(): String? {
        return catalogDatabasePassword
    }

    @Config("ducklake.catalog.database-password")
    @ConfigDescription("Password for the catalog database (required for PostgreSQL)")
    fun setCatalogDatabasePassword(catalogDatabasePassword: String?): DucklakeConfig {
        this.catalogDatabasePassword = catalogDatabasePassword
        return this
    }

    fun getDataPath(): String? {
        return dataPath
    }

    @Config("ducklake.data-path")
    @ConfigDescription("Base path for relative data file paths (from ducklake_metadata table)")
    fun setDataPath(dataPath: String?): DucklakeConfig {
        this.dataPath = dataPath
        return this
    }

    fun getMaxCatalogConnections(): Int {
        return maxCatalogConnections
    }

    @Config("ducklake.catalog.max-connections")
    @ConfigDescription("Maximum number of JDBC connections to the catalog database")
    fun setMaxCatalogConnections(maxCatalogConnections: Int): DucklakeConfig {
        this.maxCatalogConnections = maxCatalogConnections
        return this
    }

    fun getDefaultSnapshotId(): OptionalLong {
        return defaultSnapshotId
    }

    @Config("ducklake.default-snapshot-id")
    @ConfigDescription("Optional default DuckLake snapshot ID for reads when query/session does not specify a snapshot")
    fun setDefaultSnapshotId(defaultSnapshotId: Long?): DucklakeConfig {
        this.defaultSnapshotId = if (defaultSnapshotId == null) OptionalLong.empty() else OptionalLong.of(defaultSnapshotId)
        return this
    }

    fun getDefaultSnapshotTimestamp(): Optional<Instant> {
        return defaultSnapshotTimestamp
    }

    @Config("ducklake.default-snapshot-timestamp")
    @ConfigDescription("Optional default DuckLake snapshot timestamp (ISO-8601 instant) for reads when query/session does not specify a snapshot")
    fun setDefaultSnapshotTimestamp(defaultSnapshotTimestamp: String?): DucklakeConfig {
        if (defaultSnapshotTimestamp == null) {
            this.defaultSnapshotTimestamp = Optional.empty()
            return this
        }

        try {
            this.defaultSnapshotTimestamp = Optional.of(Instant.parse(defaultSnapshotTimestamp))
        }
        catch (e: DateTimeParseException) {
            throw IllegalArgumentException("Invalid ducklake.default-snapshot-timestamp value: " + defaultSnapshotTimestamp, e)
        }
        return this
    }

    @NotNull
    @Deprecated("")
    fun getTemporalPartitionEncoding(): DucklakeTemporalPartitionEncoding {
        return temporalPartitionEncoding
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
    @Deprecated("")
    @Config("ducklake.temporal-partition-encoding")
    @ConfigDescription("Deprecated. DuckLake 1.0 settled on calendar; epoch path retained for compatibility with pre-resolution catalogs. Values: calendar (default, spec-conformant) or epoch.")
    fun setTemporalPartitionEncoding(temporalPartitionEncoding: String?): DucklakeConfig {
        if (temporalPartitionEncoding == null) {
            this.temporalPartitionEncoding = DucklakeTemporalPartitionEncoding.CALENDAR
            return this
        }

        try {
            this.temporalPartitionEncoding = DucklakeTemporalPartitionEncoding.fromString(temporalPartitionEncoding)
        }
        catch (e: IllegalArgumentException) {
            throw IllegalArgumentException("Invalid ducklake.temporal-partition-encoding value: " + temporalPartitionEncoding + " (expected: calendar or epoch)", e)
        }
        return this
    }

    @Deprecated("")
    fun isTemporalPartitionEncodingReadLeniency(): Boolean {
        return temporalPartitionEncodingReadLeniency
    }

    /**
     * @deprecated Companion to {@link #setTemporalPartitionEncoding(String)}; see that
     * setter's note for the DuckLake 1.0 spec resolution. Leniency only matters when
     * epoch and calendar interpretations diverge, which a v1-conformant catalog never
     * triggers.
     */
    @Deprecated("")
    @Config("ducklake.temporal-partition-encoding-read-leniency")
    @ConfigDescription("Deprecated. If true, temporal partition pruning keeps files unless both calendar and epoch interpretations exclude them. Only relevant when reading mixed-encoding catalogs from before the DuckLake 1.0 calendar-only resolution.")
    fun setTemporalPartitionEncodingReadLeniency(temporalPartitionEncodingReadLeniency: Boolean): DucklakeConfig {
        this.temporalPartitionEncodingReadLeniency = temporalPartitionEncodingReadLeniency
        return this
    }

    @AssertTrue(message = "Only one of ducklake.default-snapshot-id or ducklake.default-snapshot-timestamp may be set")
    fun isSnapshotDefaultsValid(): Boolean {
        return defaultSnapshotId.isEmpty() || defaultSnapshotTimestamp.isEmpty()
    }

    @NotNull
    @MinDataSize("0B")
    fun getDuckdbAutoHttpfsThreshold(): DataSize {
        return duckdbAutoHttpfsThreshold
    }

    @Config("ducklake.duckdb.auto-httpfs-threshold")
    @ConfigDescription("Size threshold for the 'auto' duckdb_read_mode: data files at or above this size stream via DuckDB's httpfs extension; smaller files materialize to local tmp first. Ignored when duckdb_read_mode is set explicitly to 'materialize' or 'httpfs'.")
    fun setDuckdbAutoHttpfsThreshold(duckdbAutoHttpfsThreshold: DataSize): DucklakeConfig {
        this.duckdbAutoHttpfsThreshold = duckdbAutoHttpfsThreshold
        return this
    }

    @NotNull
    fun getExecutionEngine(): DucklakeExecutionEngine {
        return executionEngine
    }

    @Config("ducklake.execution-engine")
    @ConfigDescription("Where the DuckDB-format read path executes: 'duckdb_local' (default — embedded DuckDB in the Trino worker JVM), 'quack' (out-of-process DuckDB reached over the Quack RPC protocol), or 'swanlake' (out-of-process DuckDB over Arrow Flight SQL — reserved). For 'quack', set ducklake.quack.host/port/token. The .db file must be reachable by the chosen engine — for 'quack' that means either a shared local path (multi-container pod) or s3:// access (not yet implemented on the quack path).")
    fun setExecutionEngine(executionEngine: DucklakeExecutionEngine): DucklakeConfig {
        this.executionEngine = executionEngine
        return this
    }

    fun getQuackHost(): String? {
        return quackHost
    }

    @Config("ducklake.quack.host")
    @ConfigDescription("Hostname of the Quack-serving DuckDB instance used when ducklake.execution-engine=quack. Required for the quack engine; ignored otherwise.")
    fun setQuackHost(quackHost: String?): DucklakeConfig {
        this.quackHost = quackHost
        return this
    }

    fun getQuackPort(): Int {
        return quackPort
    }

    @Config("ducklake.quack.port")
    @ConfigDescription("Quack server port. Defaults to 9494.")
    fun setQuackPort(quackPort: Int): DucklakeConfig {
        this.quackPort = quackPort
        return this
    }

    fun getQuackToken(): String? {
        return quackToken
    }

    @Config("ducklake.quack.token")
    @ConfigDescription("Quack server auth token. Required when ducklake.execution-engine=quack. Min 4 characters (Quack server-side requirement).")
    fun setQuackToken(quackToken: String?): DucklakeConfig {
        this.quackToken = quackToken
        return this
    }

    fun getDuckdbMemoryLimit(): DataSize? {
        return duckdbMemoryLimit
    }

    @Config("ducklake.duckdb.memory-limit")
    @ConfigDescription("DuckDB engine memory_limit (per-split for duckdb_local; instance-wide for quack). Unset by default.")
    fun setDuckdbMemoryLimit(duckdbMemoryLimit: DataSize?): DucklakeConfig {
        this.duckdbMemoryLimit = duckdbMemoryLimit
        return this
    }

    fun getDuckdbThreads(): Int? {
        return duckdbThreads
    }

    @Config("ducklake.duckdb.threads")
    @ConfigDescription("DuckDB engine thread count. Unset by default (DuckDB picks per CPU count).")
    fun setDuckdbThreads(duckdbThreads: Int?): DucklakeConfig {
        this.duckdbThreads = duckdbThreads
        return this
    }

    fun getDuckdbTempDirectory(): String? {
        return duckdbTempDirectory
    }

    @Config("ducklake.duckdb.temp-directory")
    @ConfigDescription("DuckDB temp_directory (spill location). Unset by default.")
    fun setDuckdbTempDirectory(duckdbTempDirectory: String?): DucklakeConfig {
        this.duckdbTempDirectory = duckdbTempDirectory
        return this
    }

    fun getDuckdbTempDirectoryMaxSize(): DataSize? {
        return duckdbTempDirectoryMaxSize
    }

    @Config("ducklake.duckdb.temp-directory-max-size")
    @ConfigDescription("DuckDB max_temp_directory_size (cap on spill usage). Unset by default.")
    fun setDuckdbTempDirectoryMaxSize(duckdbTempDirectoryMaxSize: DataSize?): DucklakeConfig {
        this.duckdbTempDirectoryMaxSize = duckdbTempDirectoryMaxSize
        return this
    }

    fun isDuckdbEnableObjectCache(): Boolean {
        return duckdbEnableObjectCache
    }

    @Config("ducklake.duckdb.enable-object-cache")
    @ConfigDescription("Whether to enable DuckDB's enable_object_cache (parquet metadata reuse across queries). Default true.")
    fun setDuckdbEnableObjectCache(duckdbEnableObjectCache: Boolean): DucklakeConfig {
        this.duckdbEnableObjectCache = duckdbEnableObjectCache
        return this
    }

    @NotNull
    @MinDataSize("1kB")
    fun getDuckdbTargetWriteBytes(): DataSize {
        return duckdbTargetWriteBytes
    }

    @Config("ducklake.duckdb.target-write-bytes")
    @ConfigDescription("Approximate logical input bytes appended before the DuckDB-format writer rolls to a new .db file. On-disk size will be smaller depending on data compressibility — typically 3-5×.")
    fun setDuckdbTargetWriteBytes(duckdbTargetWriteBytes: DataSize): DucklakeConfig {
        this.duckdbTargetWriteBytes = duckdbTargetWriteBytes
        return this
    }

    fun toDuckDbTuning(): DuckDbTuning {
        return DuckDbTuning(
                Optional.ofNullable<DataSize>(duckdbMemoryLimit),
                if (duckdbThreads == null) java.util.OptionalInt.empty() else java.util.OptionalInt.of(duckdbThreads!!),
                Optional.ofNullable<String>(duckdbTempDirectory),
                Optional.ofNullable<DataSize>(duckdbTempDirectoryMaxSize),
                duckdbEnableObjectCache)
    }

    @AssertTrue(message = "ducklake.quack.host and ducklake.quack.token must be set when ducklake.execution-engine=quack")
    fun isQuackEngineConfigComplete(): Boolean {
        if (executionEngine != DucklakeExecutionEngine.QUACK) {
            return true
        }
        return quackHost != null && !quackHost!!.isBlank()
                && quackToken != null && !quackToken!!.isBlank()
    }

    fun toCatalogConfig(): DucklakeCatalogConfig {
        return DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(catalogDatabaseUrl)
                .setCatalogDatabaseUser(catalogDatabaseUser)
                .setCatalogDatabasePassword(catalogDatabasePassword)
                .setDataPath(dataPath)
                .setMaxCatalogConnections(maxCatalogConnections)
    }

    fun getDuckdbParityExtensionPath(): Optional<String> {
        return duckdbParityExtensionPath
    }

    @Config("ducklake.duckdb.parity-extension-path")
    @ConfigDescription("Filesystem path to the trino_parity.duckdb_extension binary (https://github.com/brikk/duckdb-trino-parity-extension). When set, executors LOAD this extension on every DuckDB attach instead of parsing the in-tree trino-function-aliases.sql resource — faster per-split setup, and the only source of truth for the trino_<name> macros + trino_meta() catalog. If unset, falls back to the in-tree SQL replay (current default). The path is passed to DuckDB's LOAD '<path>'; allow_unsigned_extensions is enabled before LOAD.")
    fun setDuckdbParityExtensionPath(duckdbParityExtensionPath: String?): DucklakeConfig {
        this.duckdbParityExtensionPath = Optional.ofNullable<String>(duckdbParityExtensionPath)
                .map<String> { s -> s.trim() }
                .filter { s -> s.isNotEmpty() }
        return this
    }
}
