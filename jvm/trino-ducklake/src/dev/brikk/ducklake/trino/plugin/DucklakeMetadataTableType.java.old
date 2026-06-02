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

import java.util.Locale;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public enum DucklakeMetadataTableType
{
    FILES("files"),
    SNAPSHOTS("snapshots"),
    CURRENT_SNAPSHOT("current_snapshot"),
    SNAPSHOT_CHANGES("snapshot_changes");

    private final String suffix;

    DucklakeMetadataTableType(String suffix)
    {
        this.suffix = requireNonNull(suffix, "suffix is null");
    }

    public String suffix()
    {
        return suffix;
    }

    public static Optional<DucklakeMetadataTableType> fromSuffix(String suffix)
    {
        String normalized = requireNonNull(suffix, "suffix is null")
                .toLowerCase(Locale.ENGLISH);

        for (DucklakeMetadataTableType value : values()) {
            if (value.suffix.equals(normalized)) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }
}
