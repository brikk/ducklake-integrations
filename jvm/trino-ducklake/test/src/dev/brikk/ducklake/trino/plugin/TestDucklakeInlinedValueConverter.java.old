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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@code DucklakeInlinedValueConverter.decodeBlobText} — the inverse of
 * DuckDB's {@code Blob::ToString}. The end-to-end inlined-list round trip is covered by
 * {@code TestDucklakeCrossEngineTypeAudit.testDuckdbListBlobReadsInTrino}.
 */
public class TestDucklakeInlinedValueConverter
{
    @Test
    public void testAllNonPrintableBytesDecode()
    {
        // BLOB '\x00\x01\xFF' is serialized as plain `\x00\x01\xFF` (no quoting — the list-cast
        // LOOKUP_TABLE doesn't flag '\\' so the element stays unquoted in the list literal).
        assertThat(DucklakeInlinedValueConverter.decodeBlobText("\\x00\\x01\\xFF"))
                .containsExactly((byte) 0x00, (byte) 0x01, (byte) 0xFF);
    }

    @Test
    public void testEmptyBlobDecodesToEmptyBytes()
    {
        assertThat(DucklakeInlinedValueConverter.decodeBlobText("")).isEmpty();
    }

    @Test
    public void testMixedPrintableAndEscapedBytes()
    {
        // BLOB '\x00Foo\xFF' — 'F','o','o' are regular ASCII; 0x00 and 0xFF aren't.
        assertThat(DucklakeInlinedValueConverter.decodeBlobText("\\x00Foo\\xFF"))
                .containsExactly((byte) 0x00, (byte) 0x46, (byte) 0x6F, (byte) 0x6F, (byte) 0xFF);
    }

    @Test
    public void testBackslashByteIsEmittedAsHexEscape()
    {
        // Byte 0x5C ('\\') is non-regular in DuckDB's IsRegularCharacter, so it's emitted as \x5C,
        // not as a literal backslash. We must decode it back to a single 0x5C byte.
        assertThat(DucklakeInlinedValueConverter.decodeBlobText("\\x5C"))
                .containsExactly((byte) 0x5C);
    }

    @Test
    public void testApostropheAndDoubleQuoteAreHexEscaped()
    {
        // Byte 0x27 (') and 0x22 (") are non-regular, emitted as \x27 and \x22.
        assertThat(DucklakeInlinedValueConverter.decodeBlobText("\\x27\\x22"))
                .containsExactly((byte) 0x27, (byte) 0x22);
    }

    @Test
    public void testLowercaseHexAcceptedToo()
    {
        // DuckDB emits uppercase hex but we accept lowercase too — Blob::ToBlob (the inverse) does.
        assertThat(DucklakeInlinedValueConverter.decodeBlobText("\\xab\\xcd"))
                .containsExactly((byte) 0xAB, (byte) 0xCD);
    }

    @Test
    public void testPureAsciiBytes()
    {
        // Printable ASCII 0x20-0x7E (minus '\\', '\'', '"') are emitted as-is.
        assertThat(DucklakeInlinedValueConverter.decodeBlobText("Hello"))
                .containsExactly((byte) 'H', (byte) 'e', (byte) 'l', (byte) 'l', (byte) 'o');
    }

    @Test
    public void testTruncatedHexEscapeRejected()
    {
        assertThatThrownBy(() -> DucklakeInlinedValueConverter.decodeBlobText("\\x0"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DucklakeInlinedValueConverter.decodeBlobText("\\xZZ"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testNonAsciiCharacterRejected()
    {
        // Any byte ≥ 0x80 would arrive as `\xNN`; a literal high-bit character would indicate
        // malformed input, so reject loudly rather than truncating silently.
        assertThatThrownBy(() -> DucklakeInlinedValueConverter.decodeBlobText("abÿcd"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
