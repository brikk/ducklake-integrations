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

import com.google.common.collect.ImmutableMap;
import dev.brikk.ducklake.catalog.DucklakeCatalog;
import dev.brikk.ducklake.catalog.DucklakeSnapshot;
import dev.brikk.ducklake.catalog.JdbcDucklakeCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.READ_SNAPSHOT_ID;
import static dev.brikk.ducklake.trino.plugin.DucklakeSessionProperties.READ_SNAPSHOT_TIMESTAMP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDucklakeSnapshotResolver
{
    private DucklakeCatalog catalog;
    private DucklakeSessionProperties sessionProperties;

    @BeforeEach
    public void setUp()
            throws Exception
    {
        catalog = new JdbcDucklakeCatalog(DucklakeTestCatalogEnvironment.createDucklakeConfig().toCatalogConfig());
        sessionProperties = new DucklakeSessionProperties();
    }

    @AfterEach
    public void tearDown()
    {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    public void testQuerySnapshotOverridesSessionAndCatalog()
    {
        long currentSnapshotId = catalog.getCurrentSnapshotId();
        DucklakeSnapshotResolver resolver = new DucklakeSnapshotResolver(catalog, OptionalLong.of(currentSnapshotId + 1_000_000), Optional.empty());
        ConnectorSession session = createSession(ImmutableMap.of(READ_SNAPSHOT_ID, currentSnapshotId + 2_000_000));

        long resolvedSnapshotId = resolver.resolveSnapshotId(session, OptionalLong.of(currentSnapshotId), Optional.empty());

        assertThat(resolvedSnapshotId).isEqualTo(currentSnapshotId);
    }

    @Test
    public void testSessionSnapshotOverridesCatalogDefault()
    {
        long currentSnapshotId = catalog.getCurrentSnapshotId();
        DucklakeSnapshotResolver resolver = new DucklakeSnapshotResolver(catalog, OptionalLong.of(currentSnapshotId + 1_000_000), Optional.empty());
        ConnectorSession session = createSession(ImmutableMap.of(READ_SNAPSHOT_ID, currentSnapshotId));

        assertThat(resolver.resolveSnapshotId(session)).isEqualTo(currentSnapshotId);
    }

    @Test
    public void testSessionTimestampSnapshotResolution()
    {
        DucklakeSnapshot currentSnapshot = catalog.getSnapshot(catalog.getCurrentSnapshotId()).orElseThrow();
        Instant sessionTimestamp = currentSnapshot.snapshotTime().plusMillis(1);
        long expectedSnapshotId = catalog.getSnapshotAtOrBefore(sessionTimestamp).orElseThrow().snapshotId();
        DucklakeSnapshotResolver resolver = new DucklakeSnapshotResolver(catalog, OptionalLong.empty(), Optional.empty());
        ConnectorSession session = createSession(ImmutableMap.of(
                READ_SNAPSHOT_TIMESTAMP,
                sessionTimestamp.toString()));

        assertThat(resolver.resolveSnapshotId(session)).isEqualTo(expectedSnapshotId);
    }

    @Test
    public void testCatalogDefaultSnapshotUsedWhenSessionUnset()
    {
        long currentSnapshotId = catalog.getCurrentSnapshotId();
        DucklakeSnapshotResolver resolver = new DucklakeSnapshotResolver(catalog, OptionalLong.of(currentSnapshotId), Optional.empty());

        assertThat(resolver.resolveSnapshotId(createSession(ImmutableMap.of()))).isEqualTo(currentSnapshotId);
    }

    @Test
    public void testCatalogDefaultSnapshotTimestampUsedWhenSessionUnset()
    {
        DucklakeSnapshot currentSnapshot = catalog.getSnapshot(catalog.getCurrentSnapshotId()).orElseThrow();
        Instant catalogDefaultTimestamp = currentSnapshot.snapshotTime().plusMillis(1);
        long expectedSnapshotId = catalog.getSnapshotAtOrBefore(catalogDefaultTimestamp).orElseThrow().snapshotId();
        DucklakeSnapshotResolver resolver = new DucklakeSnapshotResolver(catalog, OptionalLong.empty(), Optional.of(catalogDefaultTimestamp));

        assertThat(resolver.resolveSnapshotId(createSession(ImmutableMap.of()))).isEqualTo(expectedSnapshotId);
    }

    @Test
    public void testFallsBackToCurrentSnapshotWhenNoOverrides()
    {
        DucklakeSnapshotResolver resolver = new DucklakeSnapshotResolver(catalog, OptionalLong.empty(), Optional.empty());

        assertThat(resolver.resolveSnapshotId(createSession(ImmutableMap.of()))).isEqualTo(catalog.getCurrentSnapshotId());
    }

    @Test
    public void testSessionSnapshotPropertiesAreMutuallyExclusive()
    {
        long currentSnapshotId = catalog.getCurrentSnapshotId();
        DucklakeSnapshot currentSnapshot = catalog.getSnapshot(currentSnapshotId).orElseThrow();
        DucklakeSnapshotResolver resolver = new DucklakeSnapshotResolver(catalog, OptionalLong.empty(), Optional.empty());
        ConnectorSession session = createSession(ImmutableMap.of(
                READ_SNAPSHOT_ID, currentSnapshotId,
                READ_SNAPSHOT_TIMESTAMP, currentSnapshot.snapshotTime().toString()));

        assertThatThrownBy(() -> resolver.resolveSnapshotId(session))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("mutually exclusive");
    }

    @Test
    public void testQuerySnapshotReferenceMustBeExclusive()
    {
        long currentSnapshotId = catalog.getCurrentSnapshotId();
        Instant currentSnapshotTime = catalog.getSnapshot(currentSnapshotId).orElseThrow().snapshotTime();
        DucklakeSnapshotResolver resolver = new DucklakeSnapshotResolver(catalog, OptionalLong.empty(), Optional.empty());

        assertThatThrownBy(() -> resolver.resolveSnapshotId(createSession(ImmutableMap.of()), OptionalLong.of(currentSnapshotId), Optional.of(currentSnapshotTime)))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("cannot set both snapshot ID and snapshot timestamp");
    }

    @Test
    public void testResolveSnapshotAtOrBeforeTimestamp()
    {
        long currentSnapshotId = catalog.getCurrentSnapshotId();
        Instant currentSnapshotTime = catalog.getSnapshot(currentSnapshotId).orElseThrow().snapshotTime();
        DucklakeSnapshotResolver resolver = new DucklakeSnapshotResolver(catalog, OptionalLong.empty(), Optional.empty());

        assertThat(resolver.resolveSnapshotIdAtOrBefore(currentSnapshotTime)).isEqualTo(currentSnapshotId);
        assertThatThrownBy(() -> resolver.resolveSnapshotIdAtOrBefore(Instant.EPOCH))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("No DuckLake snapshot exists at or before timestamp");
    }

    private ConnectorSession createSession(Map<String, Object> propertyValues)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .setPropertyValues(propertyValues)
                .build();
    }
}
