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

import io.trino.spi.block.Block
import io.trino.spi.type.ArrayType
import io.trino.spi.type.BigintType.BIGINT
import io.trino.spi.type.DecimalType
import io.trino.spi.type.DoubleType.DOUBLE
import io.trino.spi.type.RealType.REAL
import io.trino.spi.type.VarbinaryType.VARBINARY
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.math.BigDecimal

/**
 * Unit tests for `DucklakeInlinedValueConverter.decodeBlobText` — the inverse of
 * DuckDB's `Blob::ToString`. The end-to-end inlined-list round trip is covered by
 * `TestDucklakeCrossEngineTypeAudit.testDuckdbListBlobReadsInTrino`.
 */
class TestDucklakeInlinedValueConverter {
    @Test
    fun testAllNonPrintableBytesDecode() {
        // BLOB '\x00\x01\xFF' is serialized as plain `\x00\x01\xFF` (no quoting — the list-cast
        // LOOKUP_TABLE doesn't flag '\\' so the element stays unquoted in the list literal).
        assertThat(DucklakeInlinedValueConverter.decodeBlobText("\\x00\\x01\\xFF"))
                .containsExactly(0x00.toByte(), 0x01.toByte(), 0xFF.toByte())
    }

    @Test
    fun testEmptyBlobDecodesToEmptyBytes() {
        assertThat(DucklakeInlinedValueConverter.decodeBlobText("")).isEmpty()
    }

    @Test
    fun testMixedPrintableAndEscapedBytes() {
        // BLOB '\x00Foo\xFF' — 'F','o','o' are regular ASCII; 0x00 and 0xFF aren't.
        assertThat(DucklakeInlinedValueConverter.decodeBlobText("\\x00Foo\\xFF"))
                .containsExactly(0x00.toByte(), 0x46.toByte(), 0x6F.toByte(), 0x6F.toByte(), 0xFF.toByte())
    }

    @Test
    fun testBackslashByteIsEmittedAsHexEscape() {
        // Byte 0x5C ('\\') is non-regular in DuckDB's IsRegularCharacter, so it's emitted as \x5C,
        // not as a literal backslash. We must decode it back to a single 0x5C byte.
        assertThat(DucklakeInlinedValueConverter.decodeBlobText("\\x5C"))
                .containsExactly(0x5C.toByte())
    }

    @Test
    fun testApostropheAndDoubleQuoteAreHexEscaped() {
        // Byte 0x27 (') and 0x22 (") are non-regular, emitted as \x27 and \x22.
        assertThat(DucklakeInlinedValueConverter.decodeBlobText("\\x27\\x22"))
                .containsExactly(0x27.toByte(), 0x22.toByte())
    }

    @Test
    fun testLowercaseHexAcceptedToo() {
        // DuckDB emits uppercase hex but we accept lowercase too — Blob::ToBlob (the inverse) does.
        assertThat(DucklakeInlinedValueConverter.decodeBlobText("\\xab\\xcd"))
                .containsExactly(0xAB.toByte(), 0xCD.toByte())
    }

    @Test
    fun testPureAsciiBytes() {
        // Printable ASCII 0x20-0x7E (minus '\\', '\'', '"') are emitted as-is.
        assertThat(DucklakeInlinedValueConverter.decodeBlobText("Hello"))
                .containsExactly('H'.code.toByte(), 'e'.code.toByte(), 'l'.code.toByte(), 'l'.code.toByte(), 'o'.code.toByte())
    }

    @Test
    fun testTruncatedHexEscapeRejected() {
        assertThatThrownBy { DucklakeInlinedValueConverter.decodeBlobText("\\x0") }
                .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { DucklakeInlinedValueConverter.decodeBlobText("\\xZZ") }
                .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun testNonAsciiCharacterRejected() {
        // Any byte ≥ 0x80 would arrive as `\xNN`; a literal high-bit character would indicate
        // malformed input, so reject loudly rather than truncating silently.
        assertThatThrownBy { DucklakeInlinedValueConverter.decodeBlobText("abÿcd") }
                .isInstanceOf(IllegalArgumentException::class.java)
    }

    // ---- non-finite floats / doubles in inlined list text (id=lowtail-inlinedlist-null-token-collision) ----

    @Test
    fun testNonFiniteDoublesInListText() {
        // DuckDB renders non-finite list<double> elements as bare inf/-inf/nan tokens; Java's
        // Double.parseDouble would throw NumberFormatException on those without normalization.
        val block = DucklakeInlinedValueConverter.convertJdbcValue("[1.5, inf, -inf, nan, NULL]", ArrayType(DOUBLE)) as Block
        assertThat(block.positionCount).isEqualTo(5)
        assertThat(DOUBLE.getDouble(block, 0)).isEqualTo(1.5)
        assertThat(DOUBLE.getDouble(block, 1)).isEqualTo(Double.POSITIVE_INFINITY)
        assertThat(DOUBLE.getDouble(block, 2)).isEqualTo(Double.NEGATIVE_INFINITY)
        assertThat(DOUBLE.getDouble(block, 3)).isNaN()
        assertThat(block.isNull(4)).isTrue()
    }

    @Test
    fun testNonFiniteRealsInListText() {
        val block = DucklakeInlinedValueConverter.convertJdbcValue("[inf, -inf, nan]", ArrayType(REAL)) as Block
        assertThat(java.lang.Float.intBitsToFloat(REAL.getInt(block, 0))).isEqualTo(Float.POSITIVE_INFINITY)
        assertThat(java.lang.Float.intBitsToFloat(REAL.getInt(block, 1))).isEqualTo(Float.NEGATIVE_INFINITY)
        assertThat(java.lang.Float.intBitsToFloat(REAL.getInt(block, 2))).isNaN()
    }

    // ---- unquoted NULL token (id=lowtail-inlinedlist-null-token-collision) ----

    @Test
    fun testListNullTokenReadsAsNull() {
        // The bare NULL token is a SQL null. A blob whose bytes spell "NULL" serializes
        // identically (unquoted) and is genuinely indistinguishable at the text level; we
        // deliberately resolve that inherent DuckDB-serialization ambiguity in favour of a
        // genuine null (the common case, also pinned by the list<blob> cross-engine audit).
        val blobBlock = DucklakeInlinedValueConverter.convertJdbcValue("[NULL]", ArrayType(VARBINARY)) as Block
        assertThat(blobBlock.isNull(0)).isTrue()

        val bigintBlock = DucklakeInlinedValueConverter.convertJdbcValue("[NULL]", ArrayType(BIGINT)) as Block
        assertThat(bigintBlock.isNull(0)).isTrue()
    }

    // ---- decimal rescale (id=lowtail-decimal-halfup-silent-rescale) ----

    @Test
    fun testDecimalAtColumnScaleConverts() {
        // Value already at the column scale: a no-op rescale, returns the unscaled long.
        val value = DucklakeInlinedValueConverter.convertJdbcValue(BigDecimal("1.23"), DecimalType.createDecimalType(10, 2))
        assertThat(value).isEqualTo(123L)
    }

    @Test
    fun testDecimalScaleMismatchThrowsLoudlyInsteadOfRounding() {
        // More fractional digits than the column scale used to be silently HALF_UP-rounded
        // (1.23), diverging from a Parquet read. Now it throws like Trino's Decimals SPI does.
        assertThatThrownBy {
            DucklakeInlinedValueConverter.convertJdbcValue(BigDecimal("1.234"), DecimalType.createDecimalType(10, 2))
        }.isInstanceOf(ArithmeticException::class.java)
    }
}
