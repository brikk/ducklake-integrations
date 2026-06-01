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
import dev.brikk.ducklake.catalog.DucklakeSnapshot;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static java.util.Objects.requireNonNull;

/**
 * Resolves the effective snapshot for a read using precedence:
 * query override > session properties > catalog defaults > current snapshot.
 */
public class DucklakeSnapshotResolver
{
    private final DucklakeCatalog catalog;
    private final OptionalLong catalogDefaultSnapshotId;
    private final Optional<Instant> catalogDefaultSnapshotTimestamp;

    @Inject
    public DucklakeSnapshotResolver(DucklakeCatalog catalog, DucklakeConfig config)
    {
        this(catalog, config.getDefaultSnapshotId(), config.getDefaultSnapshotTimestamp());
    }

    public DucklakeSnapshotResolver(DucklakeCatalog catalog, OptionalLong catalogDefaultSnapshotId, Optional<Instant> catalogDefaultSnapshotTimestamp)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.catalogDefaultSnapshotId = requireNonNull(catalogDefaultSnapshotId, "catalogDefaultSnapshotId is null");
        this.catalogDefaultSnapshotTimestamp = requireNonNull(catalogDefaultSnapshotTimestamp, "catalogDefaultSnapshotTimestamp is null");

        if (catalogDefaultSnapshotId.isPresent() && catalogDefaultSnapshotTimestamp.isPresent()) {
            throw new TrinoException(INVALID_ARGUMENTS, "Catalog snapshot defaults cannot set both snapshot ID and snapshot timestamp");
        }
    }

    public long resolveSnapshotId(ConnectorSession session)
    {
        return resolveSnapshotId(session, OptionalLong.empty(), Optional.empty());
    }

    public long resolveSnapshotId(ConnectorSession session, OptionalLong querySnapshotId, Optional<Instant> querySnapshotTimestamp)
    {
        requireNonNull(session, "session is null");
        requireNonNull(querySnapshotId, "querySnapshotId is null");
        requireNonNull(querySnapshotTimestamp, "querySnapshotTimestamp is null");

        if (querySnapshotId.isPresent() && querySnapshotTimestamp.isPresent()) {
            throw new TrinoException(INVALID_ARGUMENTS, "Query snapshot reference cannot set both snapshot ID and snapshot timestamp");
        }
        if (querySnapshotId.isPresent()) {
            return resolveSnapshotIdById(querySnapshotId.getAsLong());
        }
        if (querySnapshotTimestamp.isPresent()) {
            return resolveSnapshotIdAtOrBefore(querySnapshotTimestamp.get());
        }

        OptionalLong sessionSnapshotId = DucklakeSessionProperties.getReadSnapshotId(session);
        Optional<Instant> sessionSnapshotTimestamp = DucklakeSessionProperties.getReadSnapshotTimestamp(session);
        if (sessionSnapshotId.isPresent() && sessionSnapshotTimestamp.isPresent()) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "Session properties read_snapshot_id and read_snapshot_timestamp are mutually exclusive");
        }
        if (sessionSnapshotId.isPresent()) {
            return resolveSnapshotIdById(sessionSnapshotId.getAsLong());
        }
        if (sessionSnapshotTimestamp.isPresent()) {
            return resolveSnapshotIdAtOrBefore(sessionSnapshotTimestamp.get());
        }

        if (catalogDefaultSnapshotId.isPresent()) {
            return resolveSnapshotIdById(catalogDefaultSnapshotId.getAsLong());
        }
        if (catalogDefaultSnapshotTimestamp.isPresent()) {
            return resolveSnapshotIdAtOrBefore(catalogDefaultSnapshotTimestamp.get());
        }

        return catalog.getCurrentSnapshotId();
    }

    public long resolveSnapshotIdById(long snapshotId)
    {
        if (snapshotId <= 0) {
            throw new TrinoException(INVALID_ARGUMENTS, "DuckLake snapshot ID must be greater than 0: " + snapshotId);
        }
        DucklakeSnapshot snapshot = catalog.getSnapshot(snapshotId)
                .orElseThrow(() -> new TrinoException(INVALID_ARGUMENTS, "DuckLake snapshot ID does not exist: " + snapshotId));
        return snapshot.snapshotId();
    }

    public long resolveSnapshotIdAtOrBefore(Instant timestamp)
    {
        requireNonNull(timestamp, "timestamp is null");
        DucklakeSnapshot snapshot = catalog.getSnapshotAtOrBefore(timestamp)
                .orElseThrow(() -> new TrinoException(INVALID_ARGUMENTS, "No DuckLake snapshot exists at or before timestamp: " + timestamp));
        return snapshot.snapshotId();
    }
}
