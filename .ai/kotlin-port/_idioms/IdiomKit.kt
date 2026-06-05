/*
 * UNIFIED IDIOM KIT — reconciliation artifact for the whole Kotlin port.
 *
 * This file is NOT compiled. It is the single reconciled record of every idiom
 * helper that survives across all three modules (ducklake-catalog,
 * trino-ducklake, doris-ducklake). The live helpers live in their own
 * module/source-set/package (shown per section below); they cannot live in one
 * shared file because the modules do not depend on each other (catalog cannot
 * see trino, trino cannot see doris) and cross-module duplication is forbidden
 * by the port workflow. So the "unified kit" is this manifest plus the
 * per-module source files it points at. Human rationale: `idioms.md`.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * THE BAR (applied uniformly to every slice)
 *   Extract a helper only when a pattern recurs at >= 3 DISTINCT call sites
 *   spanning >= 2 files AND the helper is shorter/clearer than the inline form
 *   AND it has real callers (no speculative interop surface). Otherwise: inline.
 *
 * RECONCILED VERDICT BY MODULE
 *   ducklake-catalog  : NO helpers. All idioms inline. (combine pass, 2026-06-02)
 *   trino-ducklake    : NO helpers. All idioms inline. (combine pass, 2026-06-02)
 *   doris-ducklake    : 3 helpers survive (below). Each has real callers.
 *
 * Across the 239 catalog+trino ported files, every candidate helper failed the
 * bar — all 17 lowercase(Locale.ROOT) sites, the 3 null-safe message chains, and
 * the 2 first-line sites already read fine inline, and zero callers ever
 * migrated to a helper. The unified verdict for those two modules is therefore
 * "write Kotlin idioms inline." See `combined-kit-notes.json`.
 *
 * Doris is the only module that earned helpers: the SPI surface is erased
 * (Object / ConnectorTableHandle handles) and the test SPI surface repeats a
 * fixed 5-key properties map, so a handful of patterns clear the bar with real,
 * wired-in callers.
 * ─────────────────────────────────────────────────────────────────────────────
 */

// ============================================================================
// doris-ducklake / MAIN  (production)
// Lives at: jvm/doris-ducklake/src/.../doris/plugin/DorisMainIdiomKit.kt
// package dev.brikk.ducklake.doris.plugin
//
// WIRED. 3 call sites: DuckLakeConnectorMetadata (x2), DuckLakeScanPlanProvider (x1).
// ============================================================================

/**
 * Narrows an opaque Doris SPI handle (`Object` from `ConnectorScanRequest.table`
 * or a `ConnectorTableHandle` from the metadata SPI) to a concrete plugin handle
 * type [T], throwing [IllegalArgumentException] naming the actual runtime class
 * when it is the wrong type (or null).
 *
 * Collapses the two byte-identical private `asDuckLakeHandle(...)` helpers and
 * the `is X` / `javaClass.name` ceremony at all three call sites, which become
 * `req.table.asDuckLakeHandle<DuckLakeTableHandle>()` with the target type
 * inferred.
 *
 * Java-interop: Kotlin-only by construction (a reified inline generic cannot be
 * expressed as a callable from Java). All call sites are Kotlin. A Java caller
 * needing the same narrowing writes its own `instanceof` + cast — nothing on the
 * Java-facing SPI regresses.
 */
internal inline fun <reified T : Any> Any?.asDuckLakeHandle(): T =
    this as? T
        ?: throw IllegalArgumentException(
            "Expected ${T::class.java.simpleName}, got " +
                (this?.javaClass?.name ?: "null"),
        )

// ============================================================================
// doris-ducklake / TEST
// Lives at: jvm/doris-ducklake/test/src/.../doris/plugin/DorisTestIdiomKit.kt
// package dev.brikk.ducklake.doris.plugin
//
// WIRED. isolatedProperties: 7 call sites (ScanPlanProvider x6, Listing x1).
//        orFail:            7 call sites (ScanPlanProvider x5, Listing x2).
//
// RECONCILED DROP: the @JvmStatic `require(Optional<T>, String)` Java twin of
// `orFail` had ZERO callers (no Java sources exist in doris-ducklake; every
// caller is Kotlin and uses `orFail`). Per the bar it was speculative interop
// surface and is removed from the live file. `orFail` is the surviving form.
// ============================================================================

object DorisTestIdiomKit {

    /**
     * The standard 5-key connector-properties map every SPI-surface test builds
     * from a `DuckLakeTestCatalogBootstrap.IsolatedCatalog`: `type` +
     * metadata url/user/password + storage warehouse (the data dir).
     *
     * Returns a plain `LinkedHashMap` (not `Map`) so the s3-credentials test can
     * keep `put`-ing extra keys in place. `@JvmStatic` is retained for cheap
     * future Java-FE reach even though current callers are all Kotlin.
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
 * Unwraps an [java.util.Optional] from the metadata SPI, failing the test with a
 * descriptive [AssertionError] when empty: `getTableHandle(...).orFail("...")`
 * reads as the assertion it is rather than spelling out the throw-lambda.
 *
 * Java-interop: `internal` Kotlin-only sugar (extension functions are invisible
 * to Java). Tests are Kotlin-only, so no Java surface is needed.
 */
internal fun <T : Any> java.util.Optional<T>.orFail(message: String): T =
    orElseThrow { AssertionError(message) }

// ============================================================================
// DROPPED CANDIDATES (full audit trail in idioms.md + kit-notes.json)
//
// catalog + trino (combine pass): lowercaseRoot (17 sites, all inline already),
//   safeMessage (3 sites), firstLine/firstLineOf (2 sites) — all below bar or
//   zero migrated callers. Inline wins.
//
// doris-main: Objects.requireNonNull on non-null Kotlin params (kotlin-idiom
//   cleanup, not a kit); Optional.orElseThrow on catalog reads (3 sites but
//   divergent exception types + bespoke messages); normalizeFileFormat (1 file,
//   divergent semantics vs ScanRange.toThrift); double-checked-lock lazy (1
//   file, divergent bodies); requireString (already in DuckLakeConnectorProperties).
//
// doris-test: connectWithIsolated (hides the .use {} resource boundary, varied
//   bodies); populatedFormatDesc (1 file owns it, 2 intents); name/precision
//   TypeMapping accessors (1 file); FakeConnectorContext literal (trivial ctor);
//   require (@JvmStatic) — the zero-caller Java twin of orFail, now removed.
// ============================================================================
