package dev.brikk.ducklake.doris.corpus

import java.math.BigDecimal
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset

/**
 * Renders a Doris mysql-protocol JDBC cell the way the corpus replay
 * runner's DuckDB oracle renders the same logical value, so live-vs-live row
 * comparison ([dev.brikk.ducklake.corpus.GoldenComparator] on branch
 * `ducklake-corpus-test`) can use plain string equality.
 *
 * The comparison target is NOT DuckDB CLI text — it is the oracle's
 * `renderCell` output over DuckDB-JDBC values: Java `toString` for numerics
 * (`Infinity`, not `inf`), `yyyy-MM-dd HH:mm:ss[.ffffff]` timestamps with the
 * microsecond fraction trailing-zero-trimmed and omitted when zero,
 * UTC-normalized `…+00` for zone-aware timestamps, `\xHH` blob escapes, and
 * `\0`-escaped NUL bytes in strings. SQL NULL stays Kotlin `null` (the driver
 * turns it into `NULL`, and `""` into `(empty)` — engines must NOT do that).
 *
 * Doris-specific wrinkles this absorbs:
 *  - Doris BOOLEAN is TINYINT(1) on the wire; depending on connection props
 *    (`tinyInt1isBit`) the driver yields `Boolean` or a `Number` — callers
 *    pass [logicalBoolean] from `ResultSetMetaData` so `1/0` renders
 *    `true`/`false`.
 *  - DECIMALV3 arrives as [BigDecimal]; `toPlainString()` keeps the declared
 *    scale (`1.50`) and never goes scientific — matching DuckDB-JDBC's
 *    BigDecimal rendering of the same DECIMAL(p,s) value.
 *  - VARBINARY arrives as `ByteArray` (not `java.sql.Blob`), rendered with
 *    DuckDB's blob convention: printable ASCII verbatim, `\\` doubled,
 *    everything else `\xHH`.
 *
 * Pure and cluster-free: unit-tested in isolation; the (gated) corpus adapter
 * is just JDBC + this.
 */
object DorisValueNormalizer {

    /**
     * Normalizes one JDBC cell. [logicalBoolean] marks columns whose Doris
     * type is BOOLEAN so numeric `1/0` (driver-dependent) renders as
     * `true`/`false` like the oracle's native booleans.
     */
    fun normalize(value: Any?, logicalBoolean: Boolean = false): String? =
        when (value) {
            null -> null
            is Boolean -> if (value) "true" else "false"
            is String -> value.replace("\u0000", "\\0")
            is Number ->
                if (logicalBoolean) {
                    if (value.toInt() != 0) "true" else "false"
                } else {
                    renderNumber(value)
                }
            is ByteArray -> renderBlob(value)
            else -> renderTemporal(value) ?: value.toString()
        }

    /** Temporal JDBC shapes, or null when [value] is not a temporal. */
    private fun renderTemporal(value: Any): String? =
        when (value) {
            is Timestamp -> renderDateTime(value.toLocalDateTime())
            is LocalDateTime -> renderDateTime(value)
            is OffsetDateTime ->
                renderDateTime(value.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime()) + "+00"
            is java.sql.Date -> value.toLocalDate().toString()
            is LocalDate -> value.toString()
            is java.sql.Time -> renderTime(value.toLocalTime())
            is LocalTime -> renderTime(value)
            else -> null
        }

    /**
     * Numerics: match the oracle's Java `toString` forms. [BigDecimal] is the
     * exception — `toString()` can go scientific (`1E+3`); `toPlainString()`
     * cannot, and scale is preserved on both engines' drivers.
     */
    private fun renderNumber(value: Number): String =
        when (value) {
            is BigDecimal -> value.toPlainString()
            else -> value.toString()
        }

    /** `yyyy-MM-dd HH:mm:ss` + micro fraction, trailing zeros trimmed, omitted when zero. */
    private fun renderDateTime(dt: LocalDateTime): String {
        val base = "%04d-%02d-%02d %02d:%02d:%02d".format(
            dt.year, dt.monthValue, dt.dayOfMonth, dt.hour, dt.minute, dt.second,
        )
        return base + fraction(dt.nano)
    }

    private fun renderTime(t: LocalTime): String =
        "%02d:%02d:%02d".format(t.hour, t.minute, t.second) + fraction(t.nano)

    /** Microsecond fraction, DuckDB style (see class doc). */
    private fun fraction(nano: Int): String {
        if (nano == 0) {
            return ""
        }
        val micros = "%06d".format(nano / MICROS_DIVISOR).trimEnd('0')
        return if (micros.isEmpty()) "" else ".$micros"
    }

    /** DuckDB blob rendering: printable ASCII as-is, backslash doubled, else `\xHH`. */
    private fun renderBlob(bytes: ByteArray): String =
        buildString {
            for (b in bytes) {
                val i = b.toInt() and BYTE_MASK
                when {
                    i == '\\'.code -> append("\\\\")
                    i in PRINTABLE_MIN..PRINTABLE_MAX -> append(i.toChar())
                    else -> append("\\x%02X".format(i))
                }
            }
        }

    private const val MICROS_DIVISOR = 1000
    private const val BYTE_MASK = 0xFF
    private const val PRINTABLE_MIN = 0x20
    private const val PRINTABLE_MAX = 0x7E
}
