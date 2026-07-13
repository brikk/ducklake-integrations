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

import io.trino.filesystem.FileIterator
import io.trino.filesystem.Location
import io.trino.filesystem.TrinoFileSystem
import io.trino.filesystem.TrinoInput
import io.trino.filesystem.TrinoInputFile
import io.trino.filesystem.TrinoInputStream
import io.trino.filesystem.TrinoOutputFile
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.ByteArrayInputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.Optional
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.max

/**
 * Unit tests for [DucklakeMaterializedFileCache].
 *
 * The headline test is [testConcurrentSameKeyDownloadsNeverOverlap], a concurrency
 * regression for the per-key lock. The bug: the lock entry used to be removed from the
 * map inside the `synchronized` block (in a `finally`), which could hand a stale monitor
 * to a parked thread while a fresh `computeIfAbsent` minted a new monitor — letting two
 * threads run the download critical section at once and race on the shared `.partial` file.
 * The [DownloadTracker] records the maximum number of downloads observed running at the
 * same time; with the lock interned for the JVM lifetime that maximum must stay 1.
 */
class TestDucklakeMaterializedFileCache {
    @Test
    fun testMaterializeDownloadsOnceThenServesFromCache(@TempDir cacheDir: Path) {
        val payload = "duckdb-file-contents".toByteArray()
        val tracker = DownloadTracker()
        val fs = FakeFileSystem(payload, tracker, sleepMillis = 0)
        val cache = DucklakeMaterializedFileCache(cacheDir)

        val first = cache.materialize(fs, REMOTE, payload.size.toLong())
        assertThat(Files.readAllBytes(first)).isEqualTo(payload)

        // Second call for the same key is served from the on-disk copy — no second download.
        val second = cache.materialize(fs, REMOTE, payload.size.toLong())
        assertThat(second).isEqualTo(first)
        assertThat(tracker.totalDownloads.get()).isEqualTo(1)
    }

    @Test
    fun testTruncatedDownloadThrowsAndLeavesNoCachedFile(@TempDir cacheDir: Path) {
        val payload = "short".toByteArray()
        val fs = FakeFileSystem(payload, DownloadTracker(), sleepMillis = 0)
        val cache = DucklakeMaterializedFileCache(cacheDir)

        // Catalog claims the file is larger than what the stream actually delivers.
        assertThatThrownBy { cache.materialize(fs, REMOTE, payload.size.toLong() + 100) }
            .isInstanceOf(IOException::class.java)
            .hasMessageContaining("Truncated download")

        // The corrupt download must not have been moved into place, nor left a stray partial.
        assertThat(Files.list(cacheDir).use { it.count() }).isEqualTo(0L)
    }

    @Test
    fun testConcurrentSameKeyDownloadsNeverOverlap(@TempDir cacheDir: Path) {
        // The stream delivers fewer bytes than the requested size, so every materialize call
        // takes the download path (the truncation guard throws, so isReady() stays false and
        // the next call re-downloads). That keeps the critical section exercised on every
        // iteration across all threads — the conditions under which the old lock-removal bug
        // let two downloads overlap. A small first-read sleep widens the window.
        val payload = ByteArray(64) { it.toByte() }
        val tracker = DownloadTracker()
        val fs = FakeFileSystem(payload, tracker, sleepMillis = 2)
        val cache = DucklakeMaterializedFileCache(cacheDir)

        val threads = 8
        val iterations = 20
        val claimedSize = payload.size.toLong() + 1 // forces the truncation guard every time
        val barrier = CyclicBarrier(threads)
        val workers = (0 until threads).map {
            Thread {
                barrier.await()
                repeat(iterations) {
                    try {
                        cache.materialize(fs, REMOTE, claimedSize)
                    }
                    catch (expected: IOException) {
                        // Truncated download is expected; we are only measuring overlap.
                    }
                }
            }
        }

        workers.forEach { it.start() }
        workers.forEach { it.join() }

        // Sanity: the critical section actually ran many times (the test would be vacuous
        // otherwise), and never more than one download was in flight for the key at once.
        assertThat(tracker.totalDownloads.get()).isEqualTo(threads * iterations)
        assertThat(tracker.maxConcurrent.get()).isEqualTo(1)
    }

    @Test
    fun testTwoJvmsSharingCacheDirDoNotClobberEachOther(@TempDir cacheDir: Path) {
        // Simulate two co-located workers (separate JVMs) by using two cache instances with
        // independent per-key locks pointed at the SAME dir + key. Their JVM-local locks do
        // NOT coordinate, so both may download at once; the process-unique `.partial` staging
        // name is what keeps them from corrupting each other's file. Each must end up with the
        // correct bytes, and the dir must contain exactly the one shared `.db` and no partials.
        val payload = ByteArray(4096) { (it * 31).toByte() }
        val tracker = DownloadTracker()
        val fs = FakeFileSystem(payload, tracker, sleepMillis = 5)
        val jvmA = DucklakeMaterializedFileCache(cacheDir)
        val jvmB = DucklakeMaterializedFileCache(cacheDir)

        val barrier = CyclicBarrier(2)
        val results = java.util.concurrent.ConcurrentLinkedQueue<Path>()
        val errors = java.util.concurrent.ConcurrentLinkedQueue<Throwable>()
        val workers = listOf(jvmA, jvmB).map { cache ->
            Thread {
                try {
                    barrier.await()
                    results.add(cache.materialize(fs, REMOTE, payload.size.toLong()))
                }
                catch (t: Throwable) {
                    errors.add(t)
                }
            }
        }
        workers.forEach { it.start() }
        workers.forEach { it.join() }

        assertThat(errors).isEmpty()
        results.forEach { assertThat(Files.readAllBytes(it)).isEqualTo(payload) }
        // Exactly one shared `.db` remains; every `.partial` staging file was cleaned up.
        val remaining = Files.list(cacheDir).use { s -> s.toList() }.map { it.fileName.toString() }
        assertThat(remaining).hasSize(1)
        assertThat(remaining[0]).endsWith(".db")
    }

    private companion object {
        private val REMOTE: Location = Location.of("memory://bucket/data/file.db")
    }

    /** Records how many fake downloads are running simultaneously and the peak. */
    private class DownloadTracker {
        private val active = AtomicInteger()
        val maxConcurrent = AtomicInteger()
        val totalDownloads = AtomicInteger()

        fun enter() {
            totalDownloads.incrementAndGet()
            val now = active.incrementAndGet()
            maxConcurrent.updateAndGet { m -> max(m, now) }
        }

        fun exit() {
            active.decrementAndGet()
        }
    }

    /** Stream over a fixed payload that brackets the download in [DownloadTracker]. */
    private class TrackingInputStream(
        payload: ByteArray,
        private val tracker: DownloadTracker,
        private val sleepMillis: Long,
    ) : TrinoInputStream() {
        private val delegate = ByteArrayInputStream(payload)
        private var position = 0L
        private var firstRead = true

        init {
            tracker.enter()
        }

        private fun maybeSleep() {
            if (firstRead) {
                firstRead = false
                if (sleepMillis > 0) {
                    Thread.sleep(sleepMillis)
                }
            }
        }

        override fun read(): Int {
            maybeSleep()
            val b = delegate.read()
            if (b >= 0) {
                position++
            }
            return b
        }

        override fun read(b: ByteArray, off: Int, len: Int): Int {
            maybeSleep()
            val n = delegate.read(b, off, len)
            if (n > 0) {
                position += n.toLong()
            }
            return n
        }

        override fun getPosition(): Long = position

        override fun seek(newPosition: Long) {
            throw UnsupportedOperationException("seek not used in these tests")
        }

        override fun close() {
            try {
                delegate.close()
            }
            finally {
                tracker.exit()
            }
        }
    }

    private class FakeInputFile(
        private val location: Location,
        private val payload: ByteArray,
        private val tracker: DownloadTracker,
        private val sleepMillis: Long,
    ) : TrinoInputFile {
        override fun newInput(): TrinoInput = throw UnsupportedOperationException("newInput not used")

        override fun newStream(): TrinoInputStream = TrackingInputStream(payload, tracker, sleepMillis)

        override fun length(): Long = payload.size.toLong()

        override fun lastModified(): Instant = Instant.EPOCH

        override fun exists(): Boolean = true

        override fun location(): Location = location
    }

    /**
     * Minimal [TrinoFileSystem] that only supports [newInputFile]; every other operation
     * throws because [DucklakeMaterializedFileCache] never calls them.
     */
    private class FakeFileSystem(
        private val payload: ByteArray,
        private val tracker: DownloadTracker,
        private val sleepMillis: Long,
    ) : TrinoFileSystem {
        override fun newInputFile(location: Location): TrinoInputFile =
            FakeInputFile(location, payload, tracker, sleepMillis)

        override fun newInputFile(location: Location, length: Long): TrinoInputFile =
            FakeInputFile(location, payload, tracker, sleepMillis)

        override fun newInputFile(location: Location, length: Long, lastModified: Instant): TrinoInputFile =
            FakeInputFile(location, payload, tracker, sleepMillis)

        override fun newOutputFile(location: Location): TrinoOutputFile = unsupported()

        override fun deleteFile(location: Location) = unsupported()

        override fun deleteDirectory(location: Location) = unsupported()

        override fun renameFile(source: Location, target: Location) = unsupported()

        override fun listFiles(location: Location): FileIterator = unsupported()

        override fun directoryExists(location: Location): Optional<Boolean> = unsupported()

        override fun createDirectory(location: Location) = unsupported()

        override fun renameDirectory(source: Location, target: Location) = unsupported()

        override fun listDirectories(location: Location): Set<Location> = unsupported()

        override fun createTemporaryDirectory(
            targetPath: Location,
            temporaryPrefix: String,
            relativePrefix: String,
        ): Optional<Location> = unsupported()

        private fun unsupported(): Nothing =
            throw UnsupportedOperationException("not used by DucklakeMaterializedFileCache")
    }
}
