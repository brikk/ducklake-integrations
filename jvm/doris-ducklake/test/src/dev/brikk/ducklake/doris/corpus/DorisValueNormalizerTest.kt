package dev.brikk.ducklake.doris.corpus

import java.math.BigDecimal
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset

import org.junit.jupiter.api.Test

import org.assertj.core.api.Assertions.assertThat

/**
 * Pins [DorisValueNormalizer] to the corpus oracle's `renderCell` forms
 * (`GoldenComparator` on branch `ducklake-corpus-test`) — the strings the
 * live-vs-live comparator will see from the DuckDB side for the same logical
 * values. Pure unit tests: the (gated) adapter is JDBC + this, so every
 * rendering rule proven here is one less live-cluster surprise.
 */
internal class DorisValueNormalizerTest {

    @Test
    fun nullStaysNullAndEmptyStringStaysEmpty() {
        // NULL→"NULL" and ""→"(empty)" are DRIVER-side canonicalizations;
        // an engine doing them itself would double-encode.
        assertThat(DorisValueNormalizer.normalize(null)).isNull()
        assertThat(DorisValueNormalizer.normalize("")).isEqualTo("")
    }

    @Test
    fun stringsEscapeNulBytesLikeDuckDbGoldenText() {
        assertThat(DorisValueNormalizer.normalize("a\u0000b")).isEqualTo("a\\0b")
        assertThat(DorisValueNormalizer.normalize("plain")).isEqualTo("plain")
    }

    @Test
    fun booleansRenderTrueFalseIncludingDorisTinyintForm() {
        // Doris BOOLEAN is TINYINT(1) on the mysql protocol: depending on
        // connection props the driver hands back Boolean or a Number.
        assertThat(DorisValueNormalizer.normalize(true)).isEqualTo("true")
        assertThat(DorisValueNormalizer.normalize(false)).isEqualTo("false")
        assertThat(DorisValueNormalizer.normalize(1, logicalBoolean = true)).isEqualTo("true")
        assertThat(DorisValueNormalizer.normalize(0, logicalBoolean = true)).isEqualTo("false")
        // Without the metadata flag a 1 is just a number.
        assertThat(DorisValueNormalizer.normalize(1)).isEqualTo("1")
    }

    @Test
    fun timestampsTrimMicrosecondFractionDuckDbStyle() {
        val base = LocalDateTime.of(2026, 7, 5, 12, 34, 56)
        // zero fraction → omitted entirely
        assertThat(DorisValueNormalizer.normalize(Timestamp.valueOf(base)))
            .isEqualTo("2026-07-05 12:34:56")
        // trailing zeros trimmed: .500000 → .5
        assertThat(DorisValueNormalizer.normalize(base.withNano(500_000_000)))
            .isEqualTo("2026-07-05 12:34:56.5")
        // full micros survive
        assertThat(DorisValueNormalizer.normalize(base.withNano(123_456_000)))
            .isEqualTo("2026-07-05 12:34:56.123456")
        // sub-micro nanos truncate to micros (comparator renders micros only)
        assertThat(DorisValueNormalizer.normalize(base.withNano(999)))
            .isEqualTo("2026-07-05 12:34:56")
    }

    @Test
    fun zoneAwareTimestampsNormalizeToUtcWithPlusZeroZeroSuffix() {
        // The oracle renders OffsetDateTime at UTC then appends "+00".
        val odt = OffsetDateTime.of(2026, 7, 5, 14, 30, 0, 0, ZoneOffset.ofHours(2))
        assertThat(DorisValueNormalizer.normalize(odt)).isEqualTo("2026-07-05 12:30:00+00")
    }

    @Test
    fun datesAndTimesRenderIso() {
        assertThat(DorisValueNormalizer.normalize(LocalDate.of(2026, 7, 5))).isEqualTo("2026-07-05")
        assertThat(DorisValueNormalizer.normalize(java.sql.Date.valueOf("2026-07-05")))
            .isEqualTo("2026-07-05")
        assertThat(DorisValueNormalizer.normalize(LocalTime.of(1, 2, 3))).isEqualTo("01:02:03")
        assertThat(DorisValueNormalizer.normalize(LocalTime.of(1, 2, 3, 450_000_000)))
            .isEqualTo("01:02:03.45")
    }

    @Test
    fun decimalsKeepScaleAndNeverGoScientific() {
        // DECIMALV3(4,2) value 1.5 arrives as BigDecimal scale 2 — matches
        // the oracle's DECIMAL(4,2) rendering "1.50".
        assertThat(DorisValueNormalizer.normalize(BigDecimal("1.50"))).isEqualTo("1.50")
        // BigDecimal.toString would render 1E+3; toPlainString must not.
        assertThat(DorisValueNormalizer.normalize(BigDecimal("1E+3"))).isEqualTo("1000")
        assertThat(DorisValueNormalizer.normalize(BigDecimal("-0.001"))).isEqualTo("-0.001")
    }

    @Test
    fun floatsMatchTheOraclesJavaRenderingsNotDuckDbCliText() {
        // The comparator's DuckDB side goes through JDBC getObject → Java
        // toString, so "Infinity"/"NaN" are the agreed forms — NOT "inf"/"nan".
        assertThat(DorisValueNormalizer.normalize(1.5)).isEqualTo("1.5")
        assertThat(DorisValueNormalizer.normalize(Double.POSITIVE_INFINITY)).isEqualTo("Infinity")
        assertThat(DorisValueNormalizer.normalize(Double.NaN)).isEqualTo("NaN")
        assertThat(DorisValueNormalizer.normalize(1.5f)).isEqualTo("1.5")
    }

    @Test
    fun varbinaryRendersDuckDbBlobEscapes() {
        // printable ASCII verbatim
        assertThat(DorisValueNormalizer.normalize("abc".toByteArray())).isEqualTo("abc")
        // backslash doubled
        assertThat(DorisValueNormalizer.normalize("a\\b".toByteArray())).isEqualTo("a\\\\b")
        // non-printable → \xHH uppercase
        assertThat(DorisValueNormalizer.normalize(byteArrayOf(0x00, 0x1F, 0x7F.toByte())))
            .isEqualTo("\\x00\\x1F\\x7F")
        // high bytes unsigned
        assertThat(DorisValueNormalizer.normalize(byteArrayOf(0xFF.toByte()))).isEqualTo("\\xFF")
    }

    @Test
    fun integersPassThrough() {
        assertThat(DorisValueNormalizer.normalize(42L)).isEqualTo("42")
        assertThat(DorisValueNormalizer.normalize(-7)).isEqualTo("-7")
    }
}
