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
package dev.brikk.ducklake.catalog;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins that {@link JdbcDucklakeCatalog#newCatalogUuid()} returns UUIDv7. The
 * {@code java-uuid-generator} library exposes very similarly named factories for v1 and v7
 * ({@code timeBasedGenerator} vs {@code timeBasedEpochGenerator}) — this test exists to
 * catch an accidental swap.
 */
public class TestJdbcDucklakeCatalogUuidVersion
{
    @Test
    public void testNewCatalogUuidIsV7()
    {
        UUID uuid = UUID.fromString(JdbcDucklakeCatalog.newCatalogUuid());
        assertThat(uuid.version()).as("UUID version for catalog identity (expected v7)").isEqualTo(7);
        // RFC 4122 variant bits: top two bits of clock_seq_hi_and_reserved are 10.
        assertThat(uuid.variant()).isEqualTo(2);
    }

    @Test
    public void testNewCatalogUuidIsTimeOrderedRoughlyNow()
    {
        // v7 embeds a 48-bit unix-ms timestamp in the top 48 bits. Verify it's within a few
        // seconds of System.currentTimeMillis() — that proves we're actually producing v7, not
        // a random v4 that happens to have bits 48..51 equal to 0x7 by chance.
        long before = System.currentTimeMillis();
        UUID uuid = UUID.fromString(JdbcDucklakeCatalog.newCatalogUuid());
        long after = System.currentTimeMillis();

        long embeddedMillis = uuid.getMostSignificantBits() >>> 16;
        assertThat(embeddedMillis).isBetween(before - 1_000, after + 1_000);
    }

    @Test
    public void testConsecutiveUuidsAreDistinct()
    {
        String a = JdbcDucklakeCatalog.newCatalogUuid();
        String b = JdbcDucklakeCatalog.newCatalogUuid();
        assertThat(a).isNotEqualTo(b);
    }
}
