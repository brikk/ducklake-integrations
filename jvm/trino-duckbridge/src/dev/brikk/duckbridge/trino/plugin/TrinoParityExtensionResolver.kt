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
package dev.brikk.duckbridge.trino.plugin

import io.airlift.log.Logger
import java.io.IOException
import java.net.URL
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.Locale
import java.util.concurrent.atomic.AtomicReference

/**
 * Resolves the `trino_parity.duckdb_extension` binary path at runtime.
 *
 * Resolution order:
 *
 *  1. Callers with an explicit `duckbridge.parity-extension-path` use that directly (handled
 *     by the client/module — not this resolver).
 *  2. Otherwise, look for a classpath resource at
 *     `dev/brikk/duckbridge/trino/plugin/duckdb-extensions/<platform>/trino_parity.duckdb_extension`,
 *     extract it to a stable temp path
 *     (`${java.io.tmpdir}/trino-duckbridge/<platform>/trino_parity.duckdb_extension`), and
 *     return that.
 *  3. If neither is available, return null — the caller decides how to fail.
 *
 * Platform detection from `os.name` / `os.arch`:
 * ```
 * Mac OS X + aarch64/arm64 -> darwin-arm64
 * Mac OS X + x86_64/amd64  -> darwin-amd64
 * Linux    + x86_64/amd64  -> linux-amd64
 * Linux    + aarch64/arm64 -> linux-arm64
 * Windows  + x86_64/amd64  -> windows-amd64
 * ```
 *
 * The Gradle build's `bundleParityExtension` task writes the host platform's binary plus any
 * cross-built linux variants that exist. Multi-platform bundling needs a CI matrix that builds
 * the extension on each target.
 *
 * P3 note (Quack remote): a server-side execution engine evaluates SQL in a separate process,
 * so this resolver — which extracts to the Trino worker's local filesystem — is meaningless
 * there. Operators running a remote DuckDB must set `duckbridge.parity-extension-path` to a
 * path the server can resolve, and the LOAD must be issued server-side.
 */
object TrinoParityExtensionResolver {
    private val log: Logger = Logger.get(TrinoParityExtensionResolver::class.java)

    private const val RESOURCE_DIR: String = "dev/brikk/duckbridge/trino/plugin/duckdb-extensions"
    private const val RESOURCE_FILE: String = "trino_parity.duckdb_extension"

    /** Result holder so the cache can distinguish "not resolved yet" from "resolved to nothing". */
    private class Resolution(val path: String?)

    private val cached: AtomicReference<Resolution?> = AtomicReference()
    private val resolveLock: Any = Any()

    /**
     * Returns the absolute path to the bundled extension for the host platform, extracting on
     * first call. Null when no bundled binary matches the host or extraction fails. Cached per
     * process, guarded by a double-checked synchronized region so parallel test/split runners
     * only run one `Files.copy`.
     */
    fun resolveBundledExtensionPath(): String? {
        cached.get()?.let { return it.path }
        return synchronized(resolveLock) {
            val inner = cached.get()
            if (inner != null) {
                inner.path
            } else {
                val resolved = doResolve()
                cached.set(Resolution(resolved))
                resolved
            }
        }
    }

    /**
     * Resolve a specific platform's bundled binary, regardless of the running host. Used by test
     * fixtures that need to mount the Linux binary into a Linux container even though the JVM runs
     * on macOS. Result is NOT cached — different platforms have different paths.
     */
    fun resolveBundledExtensionPathFor(platform: String): String? {
        return synchronized(resolveLock) {
            val resourcePath = "$RESOURCE_DIR/$platform/$RESOURCE_FILE"
            val url = TrinoParityExtensionResolver::class.java.classLoader.getResource(resourcePath)
            if (url == null) {
                log.info(
                    "trino_parity: no bundled extension for platform %s (resource %s missing)",
                    platform,
                    resourcePath,
                )
                null
            } else {
                extractOrNull(url, platform)
            }
        }
    }

    private fun doResolve(): String? {
        val platform =
            detectPlatform() ?: run {
                log.info(
                    "trino_parity: unsupported host platform (os.name=%s, os.arch=%s); no bundled extension available",
                    System.getProperty("os.name"),
                    System.getProperty("os.arch"),
                )
                return null
            }
        val resourcePath = "$RESOURCE_DIR/$platform/$RESOURCE_FILE"
        val url = TrinoParityExtensionResolver::class.java.classLoader.getResource(resourcePath)
        if (url == null) {
            log.info(
                "trino_parity: no bundled extension on classpath for platform %s (resource %s missing). " +
                    "Operators must set duckbridge.parity-extension-path or rebuild the plugin jar with the " +
                    "extension binary present.",
                platform,
                resourcePath,
            )
            return null
        }
        val extracted = extractOrNull(url, platform)
        if (extracted != null) {
            log.info("trino_parity: extracted bundled extension for platform %s to %s", platform, extracted)
        }
        return extracted
    }

    private fun extractOrNull(url: URL, platform: String): String? =
        try {
            // KEEP THE BASENAME "trino_parity.duckdb_extension" — DuckDB derives the C entrypoint
            // symbol from the filename's stem (`<stem>_duckdb_cpp_init`). Mangling the filename to
            // encode the platform breaks symbol resolution at LOAD. Disambiguate via the parent dir.
            val tempDir = Path.of(System.getProperty("java.io.tmpdir"), "trino-duckbridge", platform)
            Files.createDirectories(tempDir)
            val target = tempDir.resolve(RESOURCE_FILE)
            extractAtomically(url, tempDir, target)
            target.toAbsolutePath().toString()
        } catch (e: IOException) {
            log.warn(e, "trino_parity: failed to extract bundled extension for platform %s", platform)
            null
        }

    /**
     * Extract [url]'s bytes to [target] atomically: stream into a sibling temp file in the same
     * directory, then move it into place (ATOMIC_MOVE where supported). A reader — including a
     * separate JVM sharing `java.io.tmpdir` — therefore only ever observes the complete binary.
     */
    @Throws(IOException::class)
    private fun extractAtomically(url: URL, tempDir: Path, target: Path) {
        val tmp = Files.createTempFile(tempDir, RESOURCE_FILE, ".tmp")
        try {
            url.openStream().use { input ->
                Files.copy(input, tmp, StandardCopyOption.REPLACE_EXISTING)
            }
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            } catch (e: AtomicMoveNotSupportedException) {
                log.debug(e, "trino_parity: atomic move unsupported, falling back to plain replace")
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING)
            }
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    fun detectPlatform(): String? {
        val os = System.getProperty("os.name", "").lowercase(Locale.ROOT)
        val arch = System.getProperty("os.arch", "").lowercase(Locale.ROOT)
        val osPart =
            when {
                os.contains("mac") || os.contains("darwin") -> "darwin"
                os.contains("linux") -> "linux"
                os.contains("windows") -> "windows"
                else -> return null
            }
        val archPart =
            when (arch) {
                "x86_64", "amd64" -> "amd64"
                "aarch64", "arm64" -> "arm64"
                else -> return null
            }
        return "$osPart-$archPart"
    }
}
