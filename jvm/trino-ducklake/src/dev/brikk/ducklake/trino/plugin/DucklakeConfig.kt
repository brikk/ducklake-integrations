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
import io.airlift.units.Duration
import io.airlift.units.MinDuration
import jakarta.validation.constraints.AssertTrue
import jakarta.validation.constraints.NotNull
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.Optional
import java.util.OptionalLong

/**
 * Configuration for Ducklake connector.
 */
class DucklakeConfig {
    private var catalogDatabaseUrl: String? = null
    private var catalogDatabaseUser: String? = null
    private var catalogDatabasePassword: String? = null
    private var dataPath: String? = null
    private var maxCatalogConnections: Int = 10
    private var defaultSnapshotId: OptionalLong = OptionalLong.empty()
    private var defaultSnapshotTimestamp: Optional<Instant> = Optional.empty()
    private var temporalPartitionEncoding: DucklakeTemporalPartitionEncoding = DucklakeTemporalPartitionEncoding.CALENDAR
    private var temporalPartitionEncodingReadLeniency: Boolean = true
    private var removeOrphanFilesMinRetention: Duration = Duration.valueOf("7d")
    private var maintenanceMinRetention: Duration = Duration.valueOf("7d")

    @NotNull
    fun getCatalogDatabaseUrl(): String? {
        return catalogDatabaseUrl
    }

    @Config("ducklake.catalog.database-url")
    @ConfigDescription("JDBC URL for the Ducklake catalog database " +
            "(e.g., jdbc:postgresql://host/db or jdbc:mysql://host:3306/db); the backend dialect " +
            "is inferred from the URL scheme")
    fun setCatalogDatabaseUrl(catalogDatabaseUrl: String?): DucklakeConfig {
        this.catalogDatabaseUrl = catalogDatabaseUrl
        return this
    }

    fun getCatalogDatabaseUser(): String? {
        return catalogDatabaseUser
    }

    @Config("ducklake.catalog.database-user")
    @ConfigDescription("Username for the catalog database (required for PostgreSQL and MySQL)")
    fun setCatalogDatabaseUser(catalogDatabaseUser: String?): DucklakeConfig {
        this.catalogDatabaseUser = catalogDatabaseUser
        return this
    }

    fun getCatalogDatabasePassword(): String? {
        return catalogDatabasePassword
    }

    @Config("ducklake.catalog.database-password")
    @ConfigDescription("Password for the catalog database (required for PostgreSQL and MySQL)")
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
            throw IllegalArgumentException("Invalid ducklake.default-snapshot-timestamp value: $defaultSnapshotTimestamp", e)
        }
        return this
    }

    @NotNull
    @Deprecated("")
    fun getTemporalPartitionEncoding(): DucklakeTemporalPartitionEncoding {
        return temporalPartitionEncoding
    }

    /**
     * @deprecated DuckLake 1.0 settled on calendar encoding for
     * `ducklake_file_partition_value.partition_value` (spec PR
     * <a href="https://github.com/duckdb/ducklake-web/pull/349">duckdb/ducklake-web#349</a>);
     * the epoch path is retained but no longer expected. Leave on the default
     * (`calendar`) for spec-conformant catalogs. Will be removed once a future
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
            throw IllegalArgumentException("Invalid ducklake.temporal-partition-encoding value: $temporalPartitionEncoding (expected: calendar or epoch)", e)
        }
        return this
    }

    @Deprecated("")
    fun isTemporalPartitionEncodingReadLeniency(): Boolean {
        return temporalPartitionEncodingReadLeniency
    }

    /**
     * @deprecated Companion to [setTemporalPartitionEncoding]; see that
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
        return defaultSnapshotId.isEmpty || defaultSnapshotTimestamp.isEmpty
    }

    @NotNull
    @MinDuration("0s")
    fun getRemoveOrphanFilesMinRetention(): Duration {
        return removeOrphanFilesMinRetention
    }

    @Config("ducklake.remove-orphan-files.min-retention")
    @ConfigDescription("Minimum age a file must have before remove_orphan_files may delete it. " +
            "Floors the procedure's retention_threshold argument so the maintenance op cannot " +
            "delete files an in-flight (possibly cross-engine) writer just produced. Default 7d.")
    fun setRemoveOrphanFilesMinRetention(removeOrphanFilesMinRetention: Duration): DucklakeConfig {
        this.removeOrphanFilesMinRetention = removeOrphanFilesMinRetention
        return this
    }

    @NotNull
    @MinDuration("0s")
    fun getMaintenanceMinRetention(): Duration {
        return maintenanceMinRetention
    }

    @Config("ducklake.maintenance.min-retention")
    @ConfigDescription("Minimum age floor shared by expire_snapshots (retention mode — protects " +
            "recent time-travel) and cleanup_old_files (the grace period before a scheduled file " +
            "is physically deleted — protects in-flight, possibly cross-engine, readers). Default 7d.")
    fun setMaintenanceMinRetention(maintenanceMinRetention: Duration): DucklakeConfig {
        this.maintenanceMinRetention = maintenanceMinRetention
        return this
    }

    fun toCatalogConfig(): DucklakeCatalogConfig {
        val catalogConfig = DucklakeCatalogConfig()
        catalogConfig.catalogDatabaseUrl = catalogDatabaseUrl
        catalogConfig.catalogDatabaseUser = catalogDatabaseUser
        catalogConfig.catalogDatabasePassword = catalogDatabasePassword
        catalogConfig.dataPath = dataPath
        catalogConfig.maxCatalogConnections = maxCatalogConnections
        return catalogConfig
    }
}
