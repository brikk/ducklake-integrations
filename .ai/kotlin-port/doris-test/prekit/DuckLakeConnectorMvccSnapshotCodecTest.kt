package dev.brikk.ducklake.doris.plugin

import java.time.Instant
import java.util.Optional

import org.apache.doris.connector.api.DorisConnectorException
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

internal class DuckLakeConnectorMvccSnapshotCodecTest {

    @Test
    fun roundTrip() {
        val orig =
            DuckLakeConnectorMvccSnapshot(123456789L, Instant.ofEpochMilli(1700000000000L))
        val codec = DuckLakeConnectorMvccSnapshot.Codec()

        val bytes = codec.encode(orig)
        assertThat(bytes).hasSize(DuckLakeConnectorMvccSnapshot.FRAME_BYTES)

        val back = codec.decode(bytes)
        assertThat(back).isEqualTo(orig)
        assertThat((back as DuckLakeConnectorMvccSnapshot).snapshotId()).isEqualTo(123456789L)
        assertThat(back.commitTime().toEpochMilli()).isEqualTo(1700000000000L)
    }

    @Test
    fun asVersionShape() {
        val s =
            DuckLakeConnectorMvccSnapshot(42L, Instant.ofEpochMilli(1L))
        val v: Optional<ConnectorTableVersion> = s.asVersion()
        assertThat(v).isPresent()
        assertThat(v.get()).isInstanceOf(ConnectorTableVersion.BySnapshotId::class.java)
        assertThat((v.get() as ConnectorTableVersion.BySnapshotId).snapshotId()).isEqualTo(42L)
    }

    @Test
    fun opaqueTokenFormat() {
        val s =
            DuckLakeConnectorMvccSnapshot(42L, Instant.ofEpochMilli(1700000000000L))
        assertThat(s.toOpaqueToken()).isEqualTo("ducklake:42:1700000000000")
    }

    @Test
    fun decodeBadMagicRaises() {
        val bad = ByteArray(DuckLakeConnectorMvccSnapshot.FRAME_BYTES)
        bad.fill(0xFF.toByte())
        val codec = DuckLakeConnectorMvccSnapshot.Codec()
        assertThatThrownBy { codec.decode(bad) }
            .isInstanceOf(DorisConnectorException::class.java)
            .hasMessageContaining("bad magic")
    }

    @Test
    fun decodeWrongLengthRaises() {
        val codec = DuckLakeConnectorMvccSnapshot.Codec()
        assertThatThrownBy { codec.decode(ByteArray(7)) }
            .isInstanceOf(DorisConnectorException::class.java)
            .hasMessageContaining("frame length")
    }

    @Test
    fun decodeUnsupportedFormatVersionRaises() {
        // Hand-built frame with the correct magic but a bogus format version.
        val frame = ByteArray(DuckLakeConnectorMvccSnapshot.FRAME_BYTES)
        frame[0] = 0x44 // 'D'
        frame[1] = 0x4C // 'L'
        frame[2] = 0x41 // 'A'
        frame[3] = 0x4B // 'K'
        frame[4] = 0
        frame[5] = 0
        frame[6] = 0
        frame[7] = 99 // format version 99
        val codec = DuckLakeConnectorMvccSnapshot.Codec()
        assertThatThrownBy { codec.decode(frame) }
            .isInstanceOf(DorisConnectorException::class.java)
            .hasMessageContaining("format version")
    }

    @Test
    fun encodeRejectsForeignSnapshotType() {
        val codec = DuckLakeConnectorMvccSnapshot.Codec()
        val foreign = object : ConnectorMvccSnapshot {
            override fun commitTime(): Instant = Instant.EPOCH

            override fun asVersion(): Optional<ConnectorTableVersion> = Optional.empty()

            override fun toOpaqueToken(): String = "foreign"
        }
        assertThatThrownBy { codec.encode(foreign) }
            .isInstanceOf(DorisConnectorException::class.java)
            .hasMessageContaining("cannot encode")
    }

    @Test
    fun wireFormatMagicAndLayout() {
        // Locks the on-wire layout against accidental drift. Mirror of the
        // wire-format documentation comment on DuckLakeConnectorMvccSnapshot.
        val s =
            DuckLakeConnectorMvccSnapshot(0x0102030405060708L, Instant.ofEpochMilli(0x1112131415161718L))
        val bytes = DuckLakeConnectorMvccSnapshot.Codec().encode(s)

        // magic = 0x444C414B ("DLAK"), big-endian
        assertThat(bytes[0]).isEqualTo(0x44.toByte())
        assertThat(bytes[1]).isEqualTo(0x4C.toByte())
        assertThat(bytes[2]).isEqualTo(0x41.toByte())
        assertThat(bytes[3]).isEqualTo(0x4B.toByte())
        // format version = 1
        assertThat(bytes[4]).isEqualTo(0.toByte())
        assertThat(bytes[5]).isEqualTo(0.toByte())
        assertThat(bytes[6]).isEqualTo(0.toByte())
        assertThat(bytes[7]).isEqualTo(1.toByte())
        // snapshotId big-endian
        assertThat(bytes.copyOfRange(8, 16)).containsExactly(
            0x01.toByte(), 0x02.toByte(), 0x03.toByte(), 0x04.toByte(),
            0x05.toByte(), 0x06.toByte(), 0x07.toByte(), 0x08.toByte(),
        )
        // commitTimeMs big-endian
        assertThat(bytes.copyOfRange(16, 24)).containsExactly(
            0x11.toByte(), 0x12.toByte(), 0x13.toByte(), 0x14.toByte(),
            0x15.toByte(), 0x16.toByte(), 0x17.toByte(), 0x18.toByte(),
        )
    }
}
