@file:JvmName("DorisTestIdiomKit")

package dev.brikk.ducklake.doris.plugin

import java.util.Optional

/**
 * Doris-test slice idiom kit (PROPOSAL — artifact, not yet wired into call sites).
 *
 * Lifts the two highest-frequency ceremonies visible across the 8 ported
 * `doris-ducklake/test` Kotlin files into small, Java-interop-safe helpers.
 * Only patterns recurring across MANY sites (>= 3 distinct call sites, >= 2
 * files) made the cut; everything localised to a single file stays inline.
 *
 * Rollout disposition is decided by the combine pass (see
 * `.ai/kotlin-port/_idioms/idioms.md`). The prior combine pass dropped every
 * speculative helper that had zero migrated callers — so these are written to
 * earn their keep at real call sites, not to pad a kit.
 *
 * If/when these are wired in, the natural home is the test source set
 * `jvm/doris-ducklake/test/.../doris/plugin/` (same package as the callers, so
 * no import churn and the `internal` connector types stay reachable).
 */
object DorisTestIdiomKit {

    /**
     * The standard 5-key connector-properties map every SPI-surface test builds
     * from an [DuckLakeTestCatalogBootstrap.IsolatedCatalog]:
     * `type` + metadata url/user/password + storage warehouse (the data dir).
     *
     * Replaces the repeated 6-line `mapOf("type" to "ducklake", METADATA_URL to
     * isolated.jdbcUrl(), ...)` literal at 6 sites across
     * `DuckLakeScanPlanProviderTest` (5x) and `DuckLakeConnectorListingTest` (1x).
     *
     * Returns a plain `LinkedHashMap` so callers that need to add extra keys
     * (e.g. the `s3.*` storage-credential test) can keep mutating in place —
     * matching the existing `LinkedHashMap<String, String>()` call site.
     */
    @JvmStatic
    fun isolatedProperties(
        isolated: DuckLakeTestCatalogBootstrap.IsolatedCatalog,
    ): LinkedHashMap<String, String> = LinkedHashMap<String, String>().apply {
        put("type", "ducklake")
        put(DuckLakeConnectorProperties.METADATA_URL, isolated.jdbcUrl())
        put(DuckLakeConnectorProperties.METADATA_USER, isolated.user())
        put(DuckLakeConnectorProperties.METADATA_PASSWORD, isolated.password())
        put(
            DuckLakeConnectorProperties.STORAGE_WAREHOUSE,
            isolated.dataDir().toAbsolutePath().toString(),
        )
    }

    /**
     * Unwraps an [Optional] returned by the metadata SPI, failing the test with
     * a descriptive [AssertionError] when empty.
     *
     * Replaces `optional.orElseThrow { AssertionError(message) }` at 6 sites
     * across `DuckLakeScanPlanProviderTest` (table handles + scan-range path)
     * and `DuckLakeConnectorListingTest` (table handles). Reads as an assertion
     * ("require this handle, or fail") rather than control-flow ceremony.
     *
     * Java-interop-safe: plain static taking `Optional<T>` and a `String`.
     * Kotlin callers may also use the [orFail] extension below.
     */
    @JvmStatic
    fun <T : Any> require(optional: Optional<T>, message: String): T =
        optional.orElseThrow { AssertionError(message) }
}

/**
 * Kotlin-side sugar for [DorisTestIdiomKit.require]: `optional.orFail("...")`.
 * Extension functions are invisible to Java callers, so the `@JvmStatic`
 * companion above remains the interop-safe entry point.
 */
internal fun <T : Any> Optional<T>.orFail(message: String): T =
    orElseThrow { AssertionError(message) }
