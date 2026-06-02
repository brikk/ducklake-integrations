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
import dev.brikk.ducklake.catalog.TableLocationSpec;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit-tests for {@code DucklakeTableProperties.LOCATION_PROPERTY} —
 * scheme detection, trailing-slash normalization, blank-rejection, and
 * path-traversal rejection. The integration round-trip (Trino write →
 * catalog row → DuckDB read of the same files) is covered by
 * {@code TestDucklakeCrossEngineTableLocation}.
 */
public class TestDucklakeTablePropertiesLocation
{
    @Test
    public void testAbsoluteS3LocationParsesAsAbsolute()
    {
        Optional<TableLocationSpec> location = DucklakeTableProperties.getLocation(
                ImmutableMap.of("location", "s3://bucket/data/foo/"));

        assertThat(location).isPresent();
        assertThat(location.get().path()).isEqualTo("s3://bucket/data/foo/");
        assertThat(location.get().isRelative()).isFalse();
    }

    @Test
    public void testAbsoluteFileLocationParsesAsAbsolute()
    {
        Optional<TableLocationSpec> location = DucklakeTableProperties.getLocation(
                ImmutableMap.of("location", "file:///abs/path/"));

        assertThat(location).isPresent();
        assertThat(location.get().path()).isEqualTo("file:///abs/path/");
        assertThat(location.get().isRelative()).isFalse();
    }

    @Test
    public void testAbsoluteAbfssLocationParsesAsAbsolute()
    {
        Optional<TableLocationSpec> location = DucklakeTableProperties.getLocation(
                ImmutableMap.of("location", "abfss://container@acct.dfs.core.windows.net/data/"));

        assertThat(location).isPresent();
        assertThat(location.get().path()).isEqualTo("abfss://container@acct.dfs.core.windows.net/data/");
        assertThat(location.get().isRelative()).isFalse();
    }

    @Test
    public void testRelativeLocationParsesAsRelative()
    {
        Optional<TableLocationSpec> location = DucklakeTableProperties.getLocation(
                ImmutableMap.of("location", "special_dir/"));

        assertThat(location).isPresent();
        assertThat(location.get().path()).isEqualTo("special_dir/");
        assertThat(location.get().isRelative()).isTrue();
    }

    @Test
    public void testMissingTrailingSlashIsAppended()
    {
        Optional<TableLocationSpec> absolute = DucklakeTableProperties.getLocation(
                ImmutableMap.of("location", "s3://bucket/data/foo"));
        assertThat(absolute).isPresent();
        assertThat(absolute.get().path()).isEqualTo("s3://bucket/data/foo/");
        assertThat(absolute.get().isRelative()).isFalse();

        Optional<TableLocationSpec> relative = DucklakeTableProperties.getLocation(
                ImmutableMap.of("location", "rel_dir"));
        assertThat(relative).isPresent();
        assertThat(relative.get().path()).isEqualTo("rel_dir/");
        assertThat(relative.get().isRelative()).isTrue();
    }

    @Test
    public void testMissingPropertyReturnsEmpty()
    {
        assertThat(DucklakeTableProperties.getLocation(ImmutableMap.of())).isEmpty();
    }

    @Test
    public void testBlankPropertyReturnsEmpty()
    {
        // Validation runs at property-set time; getLocation treats post-validation blank as "no value".
        assertThat(DucklakeTableProperties.getLocation(emptyValue(""))).isEmpty();
        assertThat(DucklakeTableProperties.getLocation(emptyValue("   "))).isEmpty();
    }

    @Test
    public void testValidatorRejectsBlank()
    {
        PropertyMetadata<?> meta = locationPropertyMetadata();
        assertThatThrownBy(() -> meta.decode(""))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("location must not be blank");
        assertThatThrownBy(() -> meta.decode("   "))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("location must not be blank");
    }

    @Test
    public void testValidatorRejectsPathTraversal()
    {
        PropertyMetadata<?> meta = locationPropertyMetadata();
        assertThatThrownBy(() -> meta.decode("../escape/"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("..");
        assertThatThrownBy(() -> meta.decode("s3://bucket/../escape/"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("..");
        // Embedded ".." in the middle of a path is rejected too.
        assertThatThrownBy(() -> meta.decode("a/../b/"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("..");
        // Windows-style traversal also rejected.
        assertThatThrownBy(() -> meta.decode("a\\..\\b\\"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("..");
    }

    @Test
    public void testValidatorAcceptsDoubleDotInsideSegment()
    {
        // ".." is only rejected when it stands alone as a segment.
        // A segment that contains ".." as a substring (e.g. "my..dir") is fine.
        PropertyMetadata<?> meta = locationPropertyMetadata();
        meta.decode("my..dir/");
        meta.decode("s3://bucket/dir..with..dots/");
    }

    private static Map<String, Object> emptyValue(String raw)
    {
        // Mirror what Trino does after PropertyMetadata.decode runs the validator; this
        // helper exists because the validator itself rejects blank input.
        return ImmutableMap.of("location", raw);
    }

    private static PropertyMetadata<?> locationPropertyMetadata()
    {
        List<PropertyMetadata<?>> properties = new DucklakeTableProperties().getTableProperties();
        return properties.stream()
                .filter(p -> p.getName().equals("location"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("location property not registered"));
    }
}
