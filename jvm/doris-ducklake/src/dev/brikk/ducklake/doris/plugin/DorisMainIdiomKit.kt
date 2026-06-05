package dev.brikk.ducklake.doris.plugin

/**
 * doris-main slice idiom kit (wired).
 *
 * Narrows an opaque Doris SPI handle (`Object` from `ConnectorScanRequest.table`
 * or a `ConnectorTableHandle` from the metadata SPI) to a concrete plugin handle
 * type [T], throwing [IllegalArgumentException] naming the actual runtime class
 * when it is the wrong type (or null).
 *
 * Collapses the two byte-identical private `asDuckLakeHandle(...)` helpers (one in
 * `DuckLakeConnectorMetadata`, one in `DuckLakeScanPlanProvider`) and the `is X` /
 * `javaClass.name` ceremony at all three call sites. Reified so call sites read
 * `req.table.asDuckLakeHandle()` with the target type inferred.
 *
 * Kotlin-only (reified inline functions cannot be expressed in Java). The call
 * sites are all Kotlin, so this is the natural surface.
 */
internal inline fun <reified T : Any> Any?.asDuckLakeHandle(): T =
    this as? T
        ?: throw IllegalArgumentException(
            "Expected ${T::class.java.simpleName}, got " +
                (this?.javaClass?.name ?: "null"),
        )
