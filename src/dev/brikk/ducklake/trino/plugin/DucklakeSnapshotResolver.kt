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
import dev.brikk.ducklake.catalog.DucklakeCatalogNotInitializedException
import dev.brikk.ducklake.catalog.DucklakeSnapshot
import io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR
import io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS
import io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorSession
import java.time.Instant
import java.util.Optional
import java.util.OptionalLong

/**
 * Resolves the effective snapshot for a read using precedence:
 * query override > session properties > catalog defaults > current snapshot.
 */
open class DucklakeSnapshotResolver(
        catalog: DucklakeCatalog,
        private val catalogDefaultSnapshotId: Long?,
        private val catalogDefaultSnapshotTimestamp: Instant?) {
    private val catalog: DucklakeCatalog = catalog

    init {
        if (this.catalogDefaultSnapshotId != null && this.catalogDefaultSnapshotTimestamp != null) {
            throw TrinoException(INVALID_ARGUMENTS, "Catalog snapshot defaults cannot set both snapshot ID and snapshot timestamp")
        }
    }

    @Inject
    constructor(catalog: DucklakeCatalog, config: DucklakeConfig)
            : this(catalog,
                    config.getDefaultSnapshotId().let { if (it.isPresent) it.asLong else null },
                    config.getDefaultSnapshotTimestamp().orElse(null))

    fun resolveSnapshotId(session: ConnectorSession): Long {
        return resolveSnapshotId(session, null, null)
    }

    fun resolveSnapshotId(session: ConnectorSession, querySnapshotId: Long?, querySnapshotTimestamp: Instant?): Long {
        if (querySnapshotId != null && querySnapshotTimestamp != null) {
            throw TrinoException(INVALID_ARGUMENTS, "Query snapshot reference cannot set both snapshot ID and snapshot timestamp")
        }
        if (querySnapshotId != null) {
            return resolveSnapshotIdById(querySnapshotId)
        }
        if (querySnapshotTimestamp != null) {
            return resolveSnapshotIdAtOrBefore(querySnapshotTimestamp)
        }

        val sessionSnapshotId = DucklakeSessionProperties.getReadSnapshotId(session)
        val sessionSnapshotTimestamp = DucklakeSessionProperties.getReadSnapshotTimestamp(session)
        if (sessionSnapshotId.isPresent && sessionSnapshotTimestamp.isPresent) {
            throw TrinoException(INVALID_SESSION_PROPERTY, "Session properties read_snapshot_id and read_snapshot_timestamp are mutually exclusive")
        }
        if (sessionSnapshotId.isPresent) {
            return resolveSnapshotIdById(sessionSnapshotId.asLong)
        }
        if (sessionSnapshotTimestamp.isPresent) {
            return resolveSnapshotIdAtOrBefore(sessionSnapshotTimestamp.get())
        }

        if (catalogDefaultSnapshotId != null) {
            return resolveSnapshotIdById(catalogDefaultSnapshotId)
        }
        if (catalogDefaultSnapshotTimestamp != null) {
            return resolveSnapshotIdAtOrBefore(catalogDefaultSnapshotTimestamp)
        }

        return notInitializedAware { catalog.currentSnapshotId }
    }

    fun resolveSnapshotIdById(snapshotId: Long): Long {
        if (snapshotId <= 0) {
            throw TrinoException(INVALID_ARGUMENTS, "DuckLake snapshot ID must be greater than 0: $snapshotId")
        }
        val snapshot: DucklakeSnapshot = notInitializedAware { catalog.getSnapshot(snapshotId) }
                ?: throw TrinoException(INVALID_ARGUMENTS, "DuckLake snapshot ID does not exist: $snapshotId")
        return snapshot.snapshotId
    }

    fun resolveSnapshotIdAtOrBefore(timestamp: Instant): Long {
        val snapshot: DucklakeSnapshot = notInitializedAware { catalog.getSnapshotAtOrBefore(timestamp) }
                ?: throw TrinoException(INVALID_ARGUMENTS, "No DuckLake snapshot exists at or before timestamp: $timestamp")
        return snapshot.snapshotId
    }

    /**
     * Translates the catalog-lib [DucklakeCatalogNotInitializedException] (reachable DB, but the
     * `ducklake_*` schema was never bootstrapped) into a user-facing [TrinoException] with the
     * actionable message, instead of letting it surface as an opaque internal error.
     */
    private inline fun <T> notInitializedAware(block: () -> T): T {
        try {
            return block()
        }
        catch (e: DucklakeCatalogNotInitializedException) {
            throw TrinoException(GENERIC_USER_ERROR, e.message, e)
        }
    }
}
