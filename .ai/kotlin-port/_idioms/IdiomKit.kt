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

// ---------------------------------------------------------------------------
// doris-main slice (PROPOSAL — artifact, not yet wired into call sites)
//
// The 13 ported `doris-ducklake/src` production files were surveyed against the
// same combine-pass bar the test slice used: extract only at >= 3 distinct call
// sites spanning >= 2 files where the helper is shorter/clearer than inline.
//
// Exactly ONE ceremony clears that bar: the "cast an opaque SPI handle to our
// concrete type, or throw IllegalArgumentException naming the runtime class"
// dance. Doris hands every per-table SPI call an erased handle (`Object` /
// `ConnectorTableHandle`); the plugin re-narrows it. The port carries TWO
// byte-identical private `asDuckLakeHandle(...)` copies — one in
// `DuckLakeConnectorMetadata` (2 call sites), one in `DuckLakeScanPlanProvider`
// (1 call site) — differing only in the static param type. A single reified
// helper collapses both copies AND erases the `is X` / `javaClass.name`
// ceremony at each call site: `req.table.asDuckLakeHandle()`.
//
// Everything else stayed inline (see idioms.md "What was dropped"):
//   - normalizeFileFormat / lowercase-with-default: single file (ScanPlanProvider);
//     ScanRange's lowercase is a different shape (error, not default). DROP.
//   - Optional.orElseThrow { IllegalState/ArgException(...) } on catalog reads:
//     bespoke multi-line messages, mixed exception types, marginal win. DROP.
//   - Objects.requireNonNull(x, "x"): Java-port residue on already-non-null
//     Kotlin params; a kotlin-idiom cleanup, not a cross-file kit idiom. DROP.
//
// Home if wired in: the production source set
// `jvm/doris-ducklake/src/.../doris/plugin/` (same package as callers — the
// `internal` handle types stay reachable, no import churn).
// ---------------------------------------------------------------------------

/**
 * Narrows an opaque Doris SPI handle (`Object` from `ConnectorScanRequest.table`
 * or a `ConnectorTableHandle` from the metadata SPI) to a concrete plugin handle
 * type [T], throwing [IllegalArgumentException] naming the actual runtime class
 * when it is the wrong type (or null).
 *
 * Replaces the two byte-identical private `asDuckLakeHandle(...)` helpers (one in
 * `DuckLakeConnectorMetadata`, one in `DuckLakeScanPlanProvider`) and the `is X`
 * / `javaClass.name` ceremony at all three call sites. Reified so call sites read
 * `req.table.asDuckLakeHandle()` with no `.class` / `KClass` token — the target
 * type is inferred from the assignment.
 *
 * Kotlin-only (reified inline functions cannot be expressed in Java). The ported
 * call sites are all Kotlin, so this is the natural surface; a Java caller that
 * ever needs the same narrowing would write its own `instanceof` + cast.
 */
internal inline fun <reified T : Any> Any?.asDuckLakeHandle(): T =
    this as? T
        ?: throw IllegalArgumentException(
            "Expected ${T::class.java.simpleName}, got " +
                (this?.javaClass?.name ?: "null"),
        )
