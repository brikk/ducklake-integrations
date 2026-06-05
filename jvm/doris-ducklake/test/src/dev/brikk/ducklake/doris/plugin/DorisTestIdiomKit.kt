package dev.brikk.ducklake.doris.plugin

import java.util.Optional

/**
 * Doris-test slice idiom kit.
 *
 * Lifts the two highest-frequency ceremonies across the ported
 * `doris-ducklake/test` Kotlin files into small, Java-interop-safe helpers.
 * Only patterns recurring across many sites (>= 3 distinct call sites, >= 2
 * files) made the cut; everything localised to a single file stays inline.
 *
 * Home is the test source set (same package as the callers, so no import churn
 * and the `internal` connector types stay reachable).
 */
object DorisTestIdiomKit {

    /**
     * The standard 5-key connector-properties map every SPI-surface test builds
     * from an [DuckLakeTestCatalogBootstrap.IsolatedCatalog]:
     * `type` + metadata url/user/password + storage warehouse (the data dir).
     *
     * Returns a plain `LinkedHashMap` so callers that need to add extra keys
     * (e.g. the `s3.*` storage-credential test) can keep mutating in place.
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
     * a descriptive [AssertionError] when empty. Reads as an assertion
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
