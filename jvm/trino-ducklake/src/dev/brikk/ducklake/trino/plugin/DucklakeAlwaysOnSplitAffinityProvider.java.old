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

import io.trino.filesystem.cache.SplitAffinityProvider;

import java.util.Optional;

/**
 * Returns a per-file affinity key unconditionally, so the engine routes splits for the
 * same file to the same worker across queries regardless of whether the filesystem
 * cache is enabled. DuckLake's DuckDB-format reads go through the sidecar's own httpfs
 * and bypass {@code TrinoFileSystem}, so they get no benefit from {@code fs.cache.*} but
 * still benefit from node pinning (keeps the DuckDB sidecar's buffer pool warm for the
 * same files).
 * <p>
 * Key shape matches {@code CacheSplitAffinityProvider} so behavior is identical when
 * {@code fs.cache.enabled=true} — that binding takes over via {@code setBinding()}.
 */
public class DucklakeAlwaysOnSplitAffinityProvider
        implements SplitAffinityProvider
{
    @Override
    public Optional<String> getKey(String path, long offset, long length)
    {
        return Optional.of(path + ":" + offset + ":" + length);
    }
}
