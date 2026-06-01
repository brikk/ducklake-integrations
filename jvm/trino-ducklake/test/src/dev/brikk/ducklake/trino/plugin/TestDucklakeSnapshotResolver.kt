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

import com.google.common.collect.ImmutableMap
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeSnapshot
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.READ_SNAPSHOT_ID
import dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.READ_SNAPSHOT_TIMESTAMP
import io.trino.spi.TrinoException
import io.trino.spi.connector.ConnectorSession
import io.trino.testing.TestingConnectorSession
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.Optional
import java.util.OptionalLong

class TestDucklakeSnapshotResolver {
    private var catalog: DucklakeCatalog? = null
    private var sessionProperties: DucklakeSessionProperties? = null

    @BeforeEach
    @Throws(Exception::class)
    fun setUp() {
        catalog = JdbcDucklakeCatalog(DucklakeTestCatalogEnvironment.createDucklakeConfig().toCatalogConfig())
        sessionProperties = DucklakeSessionProperties()
    }

    @AfterEach
    fun tearDown() {
        if (catalog != null) {
            catalog!!.close()
        }
    }

    @Test
    fun testQuerySnapshotOverridesSessionAndCatalog() {
        val currentSnapshotId = catalog!!.currentSnapshotId
        val resolver = DucklakeSnapshotResolver(catalog, OptionalLong.of(currentSnapshotId + 1_000_000), Optional.empty())
        val session: ConnectorSession = createSession(ImmutableMap.of(READ_SNAPSHOT_ID, currentSnapshotId + 2_000_000))

        val resolvedSnapshotId = resolver.resolveSnapshotId(session, OptionalLong.of(currentSnapshotId), Optional.empty())

        assertThat(resolvedSnapshotId).isEqualTo(currentSnapshotId)
    }

    @Test
    fun testSessionSnapshotOverridesCatalogDefault() {
        val currentSnapshotId = catalog!!.currentSnapshotId
        val resolver = DucklakeSnapshotResolver(catalog, OptionalLong.of(currentSnapshotId + 1_000_000), Optional.empty())
        val session: ConnectorSession = createSession(ImmutableMap.of(READ_SNAPSHOT_ID, currentSnapshotId))

        assertThat(resolver.resolveSnapshotId(session)).isEqualTo(currentSnapshotId)
    }

    @Test
    fun testSessionTimestampSnapshotResolution() {
        val currentSnapshot: DucklakeSnapshot = catalog!!.getSnapshot(catalog!!.currentSnapshotId).orElseThrow()
        val sessionTimestamp: Instant = currentSnapshot.snapshotTime.plusMillis(1)
        val expectedSnapshotId = catalog!!.getSnapshotAtOrBefore(sessionTimestamp).orElseThrow().snapshotId
        val resolver = DucklakeSnapshotResolver(catalog, OptionalLong.empty(), Optional.empty())
        val session: ConnectorSession = createSession(ImmutableMap.of(
                READ_SNAPSHOT_TIMESTAMP,
                sessionTimestamp.toString()))

        assertThat(resolver.resolveSnapshotId(session)).isEqualTo(expectedSnapshotId)
    }

    @Test
    fun testCatalogDefaultSnapshotUsedWhenSessionUnset() {
        val currentSnapshotId = catalog!!.currentSnapshotId
        val resolver = DucklakeSnapshotResolver(catalog, OptionalLong.of(currentSnapshotId), Optional.empty())

        assertThat(resolver.resolveSnapshotId(createSession(ImmutableMap.of()))).isEqualTo(currentSnapshotId)
    }

    @Test
    fun testCatalogDefaultSnapshotTimestampUsedWhenSessionUnset() {
        val currentSnapshot: DucklakeSnapshot = catalog!!.getSnapshot(catalog!!.currentSnapshotId).orElseThrow()
        val catalogDefaultTimestamp: Instant = currentSnapshot.snapshotTime.plusMillis(1)
        val expectedSnapshotId = catalog!!.getSnapshotAtOrBefore(catalogDefaultTimestamp).orElseThrow().snapshotId
        val resolver = DucklakeSnapshotResolver(catalog, OptionalLong.empty(), Optional.of(catalogDefaultTimestamp))

        assertThat(resolver.resolveSnapshotId(createSession(ImmutableMap.of()))).isEqualTo(expectedSnapshotId)
    }

    @Test
    fun testFallsBackToCurrentSnapshotWhenNoOverrides() {
        val resolver = DucklakeSnapshotResolver(catalog, OptionalLong.empty(), Optional.empty())

        assertThat(resolver.resolveSnapshotId(createSession(ImmutableMap.of()))).isEqualTo(catalog!!.currentSnapshotId)
    }

    @Test
    fun testSessionSnapshotPropertiesAreMutuallyExclusive() {
        val currentSnapshotId = catalog!!.currentSnapshotId
        val currentSnapshot: DucklakeSnapshot = catalog!!.getSnapshot(currentSnapshotId).orElseThrow()
        val resolver = DucklakeSnapshotResolver(catalog, OptionalLong.empty(), Optional.empty())
        val session: ConnectorSession = createSession(ImmutableMap.of(
                READ_SNAPSHOT_ID, currentSnapshotId,
                READ_SNAPSHOT_TIMESTAMP, currentSnapshot.snapshotTime.toString()))

        assertThatThrownBy { resolver.resolveSnapshotId(session) }
                .isInstanceOf(TrinoException::class.java)
                .hasMessageContaining("mutually exclusive")
    }

    @Test
    fun testQuerySnapshotReferenceMustBeExclusive() {
        val currentSnapshotId = catalog!!.currentSnapshotId
        val currentSnapshotTime: Instant = catalog!!.getSnapshot(currentSnapshotId).orElseThrow().snapshotTime
        val resolver = DucklakeSnapshotResolver(catalog, OptionalLong.empty(), Optional.empty())

        assertThatThrownBy { resolver.resolveSnapshotId(createSession(ImmutableMap.of()), OptionalLong.of(currentSnapshotId), Optional.of(currentSnapshotTime)) }
                .isInstanceOf(TrinoException::class.java)
                .hasMessageContaining("cannot set both snapshot ID and snapshot timestamp")
    }

    @Test
    fun testResolveSnapshotAtOrBeforeTimestamp() {
        val currentSnapshotId = catalog!!.currentSnapshotId
        val currentSnapshotTime: Instant = catalog!!.getSnapshot(currentSnapshotId).orElseThrow().snapshotTime
        val resolver = DucklakeSnapshotResolver(catalog, OptionalLong.empty(), Optional.empty())

        assertThat(resolver.resolveSnapshotIdAtOrBefore(currentSnapshotTime)).isEqualTo(currentSnapshotId)
        assertThatThrownBy { resolver.resolveSnapshotIdAtOrBefore(Instant.EPOCH) }
                .isInstanceOf(TrinoException::class.java)
                .hasMessageContaining("No DuckLake snapshot exists at or before timestamp")
    }

    private fun createSession(propertyValues: Map<String, Any>): ConnectorSession {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties!!.getSessionProperties())
                .setPropertyValues(propertyValues)
                .build()
    }
}
