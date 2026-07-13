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

import com.google.inject.Singleton
import io.airlift.log.Logger
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.HexFormat
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

/**
 * Per-JVM cache that materializes remote DuckLake DuckDB-format files to a known
 * local path so DuckDB's `ATTACH` (which requires a real filesystem path) can
 * open them. Files are keyed by `(remotePath, fileSize)`; if the remote file is
 * rewritten with a different size at the same path the cache will re-fetch.
 *
 * Phase 1 semantics — brutal and basic:
 *
 *  * One copy per key under `${java.io.tmpdir}/ducklake-read/`.
 *  * Atomic write: download to a process-unique `<key>.<pid>-<uuid>.partial`, then atomic
 * rename to the shared `<key>.db`. The staging name is per-process so two co-located JVMs
 * sharing the tmpdir can never write to the same partial file.
 *  * Concurrent `materialize` calls for the same key serialize on a per-key lock,
 * so only one downloader per JVM runs at a time and the others wait for the rename.
 *  * No eviction. Files persist for the JVM (and survive past it, since they live in
 * tmpdir). Capacity caps and TTL come in a later step if measurements show they're
 * needed.
 *
 * The connector does not delete files from this cache because READ_ONLY `ATTACH`es
 * may still hold them open across split lifetimes. OS tmpdir cleanup is the long-term
 * eviction story.
 */
@Singleton
class DucklakeMaterializedFileCache(private val cacheDir: Path) {
    private val keyLocks: ConcurrentMap<String, Any> = ConcurrentHashMap()

    init {
        try {
            Files.createDirectories(cacheDir)
        }
        catch (e: IOException) {
            throw RuntimeException("Failed to create DuckDB read cache dir: $cacheDir", e)
        }
    }

    constructor() : this(defaultCacheDir())

    /**
     * Return a local filesystem path for the given remote DuckDB file, downloading it
     * if not already present. Safe to call concurrently with the same key.
     *
     * The per-key lock is interned for the lifetime of the JVM — it is never removed from
     * [keyLocks]. Removing it inside the `synchronized` block (as an earlier version did in
     * a `finally`) could hand a stale monitor to an already-waiting thread: thread A clears
     * the mapping and exits while B is parked on the old monitor, then C `computeIfAbsent`s a
     * fresh monitor and enters — so B and C would run the critical section concurrently and
     * redundantly re-download. (Cross-JVM safety no longer depends on this lock: the staging
     * file is process-unique, so the worst case for two JVMs is a duplicate download, not a
     * corrupt file.) The unbounded map is cheap: keys are bounded by the set of distinct
     * `(remotePath, fileSize)` pairs, which already matches the no-eviction design of the cache.
     */
    fun materialize(fileSystem: TrinoFileSystem, remotePath: Location, fileSize: Long): Path {
        val key = cacheKey(remotePath, fileSize)
        val local = cacheDir.resolve("$key.db")
        if (isReady(local, fileSize)) {
            return local
        }

        val lock = keyLocks.computeIfAbsent(key) { Any() }
        return synchronized(lock) {
            if (isReady(local, fileSize)) {
                local
            }
            else {
                // Process-unique staging name: the per-key lock only serializes writers WITHIN
                // this JVM, so two co-located workers sharing the tmpdir must not download into the
                // same `<key>.partial` (interleaved full-size writes could pass the size check and
                // hand a corrupt .db to DuckDB ATTACH). Each attempt gets its own file and only the
                // final atomic rename to `<key>.db` is shared.
                val partial = cacheDir.resolve("$key.${uniqueSuffix()}.partial")
                try {
                    val downloaded = downloadTo(fileSystem, remotePath, partial)
                    if (downloaded != fileSize) {
                        // Truncated/short download (interrupted stream, or stale catalog fileSize):
                        // fail loudly here rather than moving a corrupt .db into place and handing it
                        // to DuckDB ATTACH. isReady() only catches this on the *next* call for the key.
                        throw IOException(
                            "Truncated download for $remotePath: expected $fileSize bytes but read $downloaded",
                        )
                    }
                    Files.move(partial, local, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
                    log.debug("Materialized %s (%d bytes) -> %s", remotePath, fileSize, local)
                    local
                }
                finally {
                    // No-op after a successful ATOMIC_MOVE (partial no longer exists); on any
                    // failure this removes our own staging file so it can't leak.
                    Files.deleteIfExists(partial)
                }
            }
        }
    }

    companion object {
        private val log: Logger = Logger.get(DucklakeMaterializedFileCache::class.java)

        private fun defaultCacheDir(): Path {
            return Paths.get(System.getProperty("java.io.tmpdir"), "ducklake-read")
        }

        private fun isReady(local: Path, expectedSize: Long): Boolean {
            if (!Files.exists(local)) {
                return false
            }
            val actual = Files.size(local)
            if (actual != expectedSize) {
                // Stale or torn write — drop it and re-fetch.
                Files.deleteIfExists(local)
                return false
            }
            return true
        }

        /** Returns the number of bytes actually transferred so the caller can verify it. */
        private fun downloadTo(fileSystem: TrinoFileSystem, remotePath: Location, target: Path): Long {
            val inputFile = fileSystem.newInputFile(remotePath)
            inputFile.newStream().use { `in` ->
                Files.newOutputStream(target).use { out ->
                    return `in`.transferTo(out)
                }
            }
        }

        /** Per-attempt staging suffix, unique across processes AND threads sharing the tmpdir. */
        private fun uniqueSuffix(): String {
            return "${ProcessHandle.current().pid()}-${java.util.UUID.randomUUID()}"
        }

        private fun cacheKey(remotePath: Location, fileSize: Long): String {
            try {
                val md = MessageDigest.getInstance("SHA-256")
                md.update(remotePath.toString().toByteArray())
                md.update(':'.code.toByte())
                md.update(fileSize.toString().toByteArray())
                return HexFormat.of().formatHex(md.digest()).substring(0, 32)
            }
            catch (e: NoSuchAlgorithmException) {
                throw IllegalStateException("SHA-256 not available", e)
            }
        }
    }
}
