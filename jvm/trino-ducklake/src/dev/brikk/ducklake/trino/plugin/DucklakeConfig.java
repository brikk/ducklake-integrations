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
    public DucklakeTemporalPartitionEncoding getTemporalPartitionEncoding()
    {
        return temporalPartitionEncoding;
    }

    @Config("ducklake.temporal-partition-encoding")
    @ConfigDescription("Temporal partition encoding used for writes and strict reads: calendar or epoch")
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

    public boolean isTemporalPartitionEncodingReadLeniency()
    {
        return temporalPartitionEncodingReadLeniency;
    }

    @Config("ducklake.temporal-partition-encoding-read-leniency")
    @ConfigDescription("If true, temporal partition pruning keeps files unless both calendar and epoch interpretations exclude them")
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

    public DucklakeCatalogConfig toCatalogConfig()
    {
        return new DucklakeCatalogConfig()
                .setCatalogDatabaseUrl(catalogDatabaseUrl)
                .setCatalogDatabaseUser(catalogDatabaseUser)
                .setCatalogDatabasePassword(catalogDatabasePassword)
                .setDataPath(dataPath)
                .setMaxCatalogConnections(maxCatalogConnections);
    }
}
