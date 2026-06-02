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

import io.airlift.log.Logger
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.Locale
import java.util.Optional
import java.util.concurrent.atomic.AtomicReference

/**
 * Resolves the `trino_parity.duckdb_extension` binary path at runtime.
 *
 *
 * Resolution order:
 *
 *  1. If the caller already has an explicit
 * `ducklake.duckdb.parity-extension-path`, use that.
 *  1. Otherwise, look for a classpath resource at
 * `dev/brikk/ducklake/trino/plugin/duckdb-extensions/&lt;platform&gt;/trino_parity.duckdb_extension`,
 * extract it to a stable temp path
 * (`${java.io.tmpdir}/trino-ducklake/trino_parity-&lt;platform&gt;.duckdb_extension`),
 * and return that.
 *  1. If neither is available, return empty — caller decides how to fail.
 *
 *
 *
 * Platform detection from `os.name` / `os.arch`:
 * <pre>
 * Mac OS X        + aarch64/arm64 -> darwin-arm64
 * Mac OS X        + x86_64/amd64  -> darwin-amd64
 * Linux           + x86_64/amd64  -> linux-amd64
 * Linux           + aarch64/arm64 -> linux-arm64
 * Windows         + x86_64/amd64  -> windows-amd64
</pre> *
 *
 *
 * The bundled binary path is keyed by the platform string. The Gradle
 * build's `bundleParityExtension` task currently writes only the host
 * platform's binary — multi-platform bundling needs a CI matrix that builds
 * the extension on each target. See `TODO-pushdown-duckdb.md`.
 *
 *
 * The Quack execution engine evaluates SQL server-side in a separate
 * process, so this resolver is meaningless there — the server can't read the
 * Trino worker's filesystem. Operators running Quack must set
 * `ducklake.duckdb.parity-extension-path` explicitly to a path the
 * Quack server can resolve.
 */
public class TrinoParityExtensionResolver private constructor() {
    public companion object {
        private val log: Logger = Logger.get(TrinoParityExtensionResolver::class.java)

        private const val RESOURCE_DIR: String = "dev/brikk/ducklake/trino/plugin/duckdb-extensions"
        private const val RESOURCE_FILE: String = "trino_parity.duckdb_extension"

        private val CACHED: AtomicReference<Optional<String>?> = AtomicReference()
        private val RESOLVE_LOCK: Any = Any()

        /**
         * Returns the absolute path to the bundled extension for the host
         * platform, extracting on first call. Empty when no bundled binary
         * matches the host or extraction fails. Cached per process.
         *
         *
         * Thread-safe via a double-checked synchronized region around the
         * extraction step: parallel tests / parallel split runners can all hit
         * this at once, but only one thread runs `Files.copy` for the
         * extension binary. Without the lock, racing copies into the same
         * destination throw IOException on all-but-one thread and the first
         * unlucky thread's empty Optional gets cached for the JVM lifetime.
         */
        @JvmStatic
        internal fun resolveBundledExtensionPath(): Optional<String> {
            val cached = CACHED.get()
            if (cached != null) {
                return cached
            }
            return synchronized(RESOLVE_LOCK) {
                val inner = CACHED.get()
                if (inner != null) {
                    inner
                }
                else {
                    val resolved = doResolve()
                    CACHED.set(resolved)
                    resolved
                }
            }
        }

        /**
         * Resolve a specific platform's bundled binary, regardless of the running
         * host. Used by test fixtures that need to mount the Linux binary into a
         * Linux testcontainer even though the JVM runs on macOS. Result is NOT
         * cached — different platforms have different paths.
         */
        @JvmStatic
        public fun resolveBundledExtensionPathFor(platform: String): Optional<String> {
            return synchronized(RESOLVE_LOCK) {
                val resourcePath = RESOURCE_DIR + "/" + platform + "/" + RESOURCE_FILE
                val url = TrinoParityExtensionResolver::class.java.classLoader.getResource(resourcePath)
                if (url == null) {
                    log.info("trino_parity: no bundled extension for platform %s (resource %s missing)",
                            platform, resourcePath)
                    Optional.empty<String>()
                }
                else {
                    try {
                        val tempDir = Path.of(System.getProperty("java.io.tmpdir"), "trino-ducklake", platform)
                        Files.createDirectories(tempDir)
                        val target = tempDir.resolve(RESOURCE_FILE)
                        url.openStream().use { `in` ->
                            Files.copy(`in`, target, StandardCopyOption.REPLACE_EXISTING)
                        }
                        Optional.of(target.toAbsolutePath().toString())
                    }
                    catch (e: IOException) {
                        log.warn("trino_parity: failed to extract bundled extension for platform %s — %s",
                                platform, e.message)
                        Optional.empty<String>()
                    }
                }
            }
        }

        @JvmStatic
        private fun doResolve(): Optional<String> {
            val platform = detectPlatform()
            if (platform.isEmpty) {
                log.info("trino_parity: unsupported host platform (os.name=%s, os.arch=%s); no bundled extension available",
                        System.getProperty("os.name"), System.getProperty("os.arch"))
                return Optional.empty()
            }
            val resourcePath = RESOURCE_DIR + "/" + platform.get() + "/" + RESOURCE_FILE
            val url = TrinoParityExtensionResolver::class.java.classLoader.getResource(resourcePath)
            if (url == null) {
                log.info("trino_parity: no bundled extension on classpath for platform %s (resource %s missing). Operators must set ducklake.duckdb.parity-extension-path or rebuild the plugin jar with the extension binary present.",
                        platform.get(), resourcePath)
                return Optional.empty()
            }
            try {
                // KEEP THE BASENAME "trino_parity.duckdb_extension" — DuckDB derives
                // the C entrypoint symbol from the filename's stem and looks for
                // `<stem>_duckdb_cpp_init`. Mangling the filename to encode the
                // platform breaks symbol resolution at LOAD time. Disambiguate via
                // the parent dir instead.
                val tempDir = Path.of(System.getProperty("java.io.tmpdir"), "trino-ducklake", platform.get())
                Files.createDirectories(tempDir)
                val target = tempDir.resolve(RESOURCE_FILE)
                // Always replace — the bundled bytes might have changed between
                // plugin reloads; checking content equality would be more work than
                // simply rewriting ~36MB once per JVM.
                // TODO(review:after id=correctness-paritext-non-atomic-copy): non-atomic Files.copy into live extension path can expose partial binary
                url.openStream().use { `in` ->
                    Files.copy(`in`, target, StandardCopyOption.REPLACE_EXISTING)
                }
                log.info("trino_parity: extracted bundled extension for platform %s to %s",
                        platform.get(), target.toAbsolutePath())
                return Optional.of(target.toAbsolutePath().toString())
            }
            // TODO(review:after id=lowtail-paritext-warn-drops-stack): IOException logged with only e.message, discarding stack/cause
            catch (e: IOException) {
                log.warn("trino_parity: failed to extract bundled extension for platform %s — %s",
                        platform.get(), e.message)
                return Optional.empty()
            }
        }

        @JvmStatic
        internal fun detectPlatform(): Optional<String> {
            val os = System.getProperty("os.name", "").lowercase(Locale.ROOT)
            val arch = System.getProperty("os.arch", "").lowercase(Locale.ROOT)
            val osPart: String
            if (os.contains("mac") || os.contains("darwin")) {
                osPart = "darwin"
            }
            else if (os.contains("linux")) {
                osPart = "linux"
            }
            else if (os.contains("windows")) {
                osPart = "windows"
            }
            else {
                return Optional.empty()
            }
            val archPart: String
            when (arch) {
                "x86_64", "amd64" -> archPart = "amd64"
                "aarch64", "arm64" -> archPart = "arm64"
                else -> return Optional.empty()
            }
            return Optional.of(osPart + "-" + archPart)
        }
    }
}
