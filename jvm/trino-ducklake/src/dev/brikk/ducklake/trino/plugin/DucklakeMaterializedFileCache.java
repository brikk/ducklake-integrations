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

import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Per-JVM cache that materializes remote DuckLake DuckDB-format files to a known
 * local path so DuckDB's {@code ATTACH} (which requires a real filesystem path) can
 * open them. Files are keyed by {@code (remotePath, fileSize)}; if the remote file is
 * rewritten with a different size at the same path the cache will re-fetch.
 *
 * <p>Phase 1 semantics — brutal and basic:
 * <ul>
 *   <li>One copy per key under {@code ${java.io.tmpdir}/ducklake-read/}.
 *   <li>Atomic write: download to {@code <name>.partial}, then atomic rename.
 *   <li>Concurrent {@code materialize} calls for the same key serialize on a per-key lock,
 *       so only one downloader runs at a time and the others wait for the rename.
 *   <li>No eviction. Files persist for the JVM (and survive past it, since they live in
 *       tmpdir). Capacity caps and TTL come in a later step if measurements show they're
 *       needed.
 * </ul>
 *
 * The connector does not delete files from this cache because READ_ONLY {@code ATTACH}es
 * may still hold them open across split lifetimes. OS tmpdir cleanup is the long-term
 * eviction story.
 */
@Singleton
final class DucklakeMaterializedFileCache
{
    private static final Logger log = Logger.get(DucklakeMaterializedFileCache.class);

    private final Path cacheDir;
    private final ConcurrentMap<String, Object> keyLocks = new ConcurrentHashMap<>();

    DucklakeMaterializedFileCache()
    {
        this(defaultCacheDir());
    }

    DucklakeMaterializedFileCache(Path cacheDir)
    {
        this.cacheDir = cacheDir;
        try {
            Files.createDirectories(cacheDir);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to create DuckDB read cache dir: " + cacheDir, e);
        }
    }

    private static Path defaultCacheDir()
    {
        return Paths.get(System.getProperty("java.io.tmpdir"), "ducklake-read");
    }

    /**
     * Return a local filesystem path for the given remote DuckDB file, downloading it
     * if not already present. Safe to call concurrently with the same key.
     */
    Path materialize(TrinoFileSystem fileSystem, Location remotePath, long fileSize)
            throws IOException
    {
        String key = cacheKey(remotePath, fileSize);
        Path local = cacheDir.resolve(key + ".db");
        if (isReady(local, fileSize)) {
            return local;
        }

        Object lock = keyLocks.computeIfAbsent(key, _ -> new Object());
        synchronized (lock) {
            try {
                if (isReady(local, fileSize)) {
                    return local;
                }
                Path partial = cacheDir.resolve(key + ".partial");
                Files.deleteIfExists(partial);
                downloadTo(fileSystem, remotePath, partial);
                Files.move(partial, local, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                log.debug("Materialized %s (%d bytes) -> %s", remotePath, fileSize, local);
                return local;
            }
            finally {
                keyLocks.remove(key, lock);
            }
        }
    }

    private static boolean isReady(Path local, long expectedSize)
            throws IOException
    {
        if (!Files.exists(local)) {
            return false;
        }
        long actual = Files.size(local);
        if (actual != expectedSize) {
            // Stale or torn write — drop it and re-fetch.
            Files.deleteIfExists(local);
            return false;
        }
        return true;
    }

    private static void downloadTo(TrinoFileSystem fileSystem, Location remotePath, Path target)
            throws IOException
    {
        TrinoInputFile inputFile = fileSystem.newInputFile(remotePath);
        try (InputStream in = inputFile.newStream();
                OutputStream out = Files.newOutputStream(target)) {
            in.transferTo(out);
        }
    }

    private static String cacheKey(Location remotePath, long fileSize)
    {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(remotePath.toString().getBytes());
            md.update((byte) ':');
            md.update(Long.toString(fileSize).getBytes());
            return HexFormat.of().formatHex(md.digest()).substring(0, 32);
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
