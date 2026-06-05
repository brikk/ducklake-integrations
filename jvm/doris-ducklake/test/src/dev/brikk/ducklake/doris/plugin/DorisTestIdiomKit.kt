package dev.brikk.ducklake.doris.plugin

import java.util.Optional

/**
 * Doris-test idiom helpers (reconciled into the unified kit; see
 * `.ai/kotlin-port/_idioms/idioms.md`).
 *
 * Only patterns recurring across >= 3 distinct call sites in >= 2 files with a
 * real readability win survive; everything single-file stays inline. Both
 * helpers below are wired in at live call sites. The earlier zero-caller
 * `@JvmStatic require(Optional<T>, String)` Java twin of [orFail] was dropped by
 * the combine pass — no Java sources consume this kit, so the speculative interop
 * surface earned nothing.
 *
 * Home is the test source set (same package as the callers, so no import churn
 * and the `internal` connector types stay reachable).
 */
object DorisTestIdiomKit {

    /**
     * The standard 5-key connector-properties map every SPI-surface test builds
     * from a [DuckLakeTestCatalogBootstrap.IsolatedCatalog]:
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
}

/**
 * Unwraps an [Optional] from the metadata SPI, failing the test with a
 * descriptive [AssertionError] when empty: `getTableHandle(...).orFail("...")`
 * reads as the assertion it is rather than spelling out the throw-lambda.
 *
 * `internal` Kotlin-only sugar (extension functions are invisible to Java); the
 * tests are Kotlin-only, so no Java entry point is needed.
 */
internal fun <T : Any> Optional<T>.orFail(message: String): T =
    orElseThrow { AssertionError(message) }
