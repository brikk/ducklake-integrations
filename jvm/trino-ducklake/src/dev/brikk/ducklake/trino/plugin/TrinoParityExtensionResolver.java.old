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

import io.airlift.log.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Resolves the {@code trino_parity.duckdb_extension} binary path at runtime.
 *
 * <p>Resolution order:
 * <ol>
 *   <li>If the caller already has an explicit
 *       {@code ducklake.duckdb.parity-extension-path}, use that.</li>
 *   <li>Otherwise, look for a classpath resource at
 *       {@code dev/brikk/ducklake/trino/plugin/duckdb-extensions/&lt;platform&gt;/trino_parity.duckdb_extension},
 *       extract it to a stable temp path
 *       ({@code ${java.io.tmpdir}/trino-ducklake/trino_parity-&lt;platform&gt;.duckdb_extension}),
 *       and return that.</li>
 *   <li>If neither is available, return empty — caller decides how to fail.</li>
 * </ol>
 *
 * <p>Platform detection from {@code os.name} / {@code os.arch}:
 * <pre>
 *   Mac OS X        + aarch64/arm64 -> darwin-arm64
 *   Mac OS X        + x86_64/amd64  -> darwin-amd64
 *   Linux           + x86_64/amd64  -> linux-amd64
 *   Linux           + aarch64/arm64 -> linux-arm64
 *   Windows         + x86_64/amd64  -> windows-amd64
 * </pre>
 *
 * <p>The bundled binary path is keyed by the platform string. The Gradle
 * build's {@code bundleParityExtension} task currently writes only the host
 * platform's binary — multi-platform bundling needs a CI matrix that builds
 * the extension on each target. See {@code TODO-pushdown-duckdb.md}.
 *
 * <p>The Quack execution engine evaluates SQL server-side in a separate
 * process, so this resolver is meaningless there — the server can't read the
 * Trino worker's filesystem. Operators running Quack must set
 * {@code ducklake.duckdb.parity-extension-path} explicitly to a path the
 * Quack server can resolve.
 */
final class TrinoParityExtensionResolver
{
    private static final Logger log = Logger.get(TrinoParityExtensionResolver.class);

    private static final String RESOURCE_DIR = "dev/brikk/ducklake/trino/plugin/duckdb-extensions";
    private static final String RESOURCE_FILE = "trino_parity.duckdb_extension";

    private static final AtomicReference<Optional<String>> CACHED = new AtomicReference<>();
    private static final Object RESOLVE_LOCK = new Object();

    private TrinoParityExtensionResolver() {}

    /**
     * Returns the absolute path to the bundled extension for the host
     * platform, extracting on first call. Empty when no bundled binary
     * matches the host or extraction fails. Cached per process.
     *
     * <p>Thread-safe via a double-checked synchronized region around the
     * extraction step: parallel tests / parallel split runners can all hit
     * this at once, but only one thread runs {@code Files.copy} for the
     * extension binary. Without the lock, racing copies into the same
     * destination throw IOException on all-but-one thread and the first
     * unlucky thread's empty Optional gets cached for the JVM lifetime.
     */
    static Optional<String> resolveBundledExtensionPath()
    {
        Optional<String> cached = CACHED.get();
        if (cached != null) {
            return cached;
        }
        synchronized (RESOLVE_LOCK) {
            cached = CACHED.get();
            if (cached != null) {
                return cached;
            }
            Optional<String> resolved = doResolve();
            CACHED.set(resolved);
            return resolved;
        }
    }

    /**
     * Resolve a specific platform's bundled binary, regardless of the running
     * host. Used by test fixtures that need to mount the Linux binary into a
     * Linux testcontainer even though the JVM runs on macOS. Result is NOT
     * cached — different platforms have different paths.
     */
    public static Optional<String> resolveBundledExtensionPathFor(String platform)
    {
        synchronized (RESOLVE_LOCK) {
            String resourcePath = RESOURCE_DIR + "/" + platform + "/" + RESOURCE_FILE;
            URL url = TrinoParityExtensionResolver.class.getClassLoader().getResource(resourcePath);
            if (url == null) {
                log.info("trino_parity: no bundled extension for platform %s (resource %s missing)",
                        platform, resourcePath);
                return Optional.empty();
            }
            try {
                Path tempDir = Path.of(System.getProperty("java.io.tmpdir"), "trino-ducklake", platform);
                Files.createDirectories(tempDir);
                Path target = tempDir.resolve(RESOURCE_FILE);
                try (InputStream in = url.openStream()) {
                    Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
                }
                return Optional.of(target.toAbsolutePath().toString());
            }
            catch (IOException e) {
                log.warn("trino_parity: failed to extract bundled extension for platform %s — %s",
                        platform, e.getMessage());
                return Optional.empty();
            }
        }
    }

    private static Optional<String> doResolve()
    {
        Optional<String> platform = detectPlatform();
        if (platform.isEmpty()) {
            log.info("trino_parity: unsupported host platform (os.name=%s, os.arch=%s); no bundled extension available",
                    System.getProperty("os.name"), System.getProperty("os.arch"));
            return Optional.empty();
        }
        String resourcePath = RESOURCE_DIR + "/" + platform.get() + "/" + RESOURCE_FILE;
        URL url = TrinoParityExtensionResolver.class.getClassLoader().getResource(resourcePath);
        if (url == null) {
            log.info("trino_parity: no bundled extension on classpath for platform %s (resource %s missing). Operators must set ducklake.duckdb.parity-extension-path or rebuild the plugin jar with the extension binary present.",
                    platform.get(), resourcePath);
            return Optional.empty();
        }
        try {
            // KEEP THE BASENAME "trino_parity.duckdb_extension" — DuckDB derives
            // the C entrypoint symbol from the filename's stem and looks for
            // `<stem>_duckdb_cpp_init`. Mangling the filename to encode the
            // platform breaks symbol resolution at LOAD time. Disambiguate via
            // the parent dir instead.
            Path tempDir = Path.of(System.getProperty("java.io.tmpdir"), "trino-ducklake", platform.get());
            Files.createDirectories(tempDir);
            Path target = tempDir.resolve(RESOURCE_FILE);
            // Always replace — the bundled bytes might have changed between
            // plugin reloads; checking content equality would be more work than
            // simply rewriting ~36MB once per JVM.
            try (InputStream in = url.openStream()) {
                Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            }
            log.info("trino_parity: extracted bundled extension for platform %s to %s",
                    platform.get(), target.toAbsolutePath());
            return Optional.of(target.toAbsolutePath().toString());
        }
        catch (IOException e) {
            log.warn("trino_parity: failed to extract bundled extension for platform %s — %s",
                    platform.get(), e.getMessage());
            return Optional.empty();
        }
    }

    static Optional<String> detectPlatform()
    {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        String osPart;
        if (os.contains("mac") || os.contains("darwin")) {
            osPart = "darwin";
        }
        else if (os.contains("linux")) {
            osPart = "linux";
        }
        else if (os.contains("windows")) {
            osPart = "windows";
        }
        else {
            return Optional.empty();
        }
        String archPart;
        switch (arch) {
            case "x86_64":
            case "amd64":
                archPart = "amd64";
                break;
            case "aarch64":
            case "arm64":
                archPart = "arm64";
                break;
            default:
                return Optional.empty();
        }
        return Optional.of(osPart + "-" + archPart);
    }
}
