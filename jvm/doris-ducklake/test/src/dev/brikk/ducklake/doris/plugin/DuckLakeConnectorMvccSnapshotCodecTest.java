package dev.brikk.ducklake.doris.plugin;

import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DuckLakeConnectorMvccSnapshotCodecTest {

    @Test
    void roundTrip() {
        DuckLakeConnectorMvccSnapshot orig =
                new DuckLakeConnectorMvccSnapshot(123456789L, Instant.ofEpochMilli(1700000000000L));
        DuckLakeConnectorMvccSnapshot.Codec codec = new DuckLakeConnectorMvccSnapshot.Codec();

        byte[] bytes = codec.encode(orig);
        assertThat(bytes).hasSize(DuckLakeConnectorMvccSnapshot.FRAME_BYTES);

        ConnectorMvccSnapshot back = codec.decode(bytes);
        assertThat(back).isEqualTo(orig);
        assertThat(((DuckLakeConnectorMvccSnapshot) back).snapshotId()).isEqualTo(123456789L);
        assertThat(back.commitTime().toEpochMilli()).isEqualTo(1700000000000L);
    }

    @Test
    void asVersionShape() {
        DuckLakeConnectorMvccSnapshot s =
                new DuckLakeConnectorMvccSnapshot(42L, Instant.ofEpochMilli(1L));
        Optional<ConnectorTableVersion> v = s.asVersion();
        assertThat(v).isPresent();
        assertThat(v.get()).isInstanceOf(ConnectorTableVersion.BySnapshotId.class);
        assertThat(((ConnectorTableVersion.BySnapshotId) v.get()).snapshotId()).isEqualTo(42L);
    }

    @Test
    void opaqueTokenFormat() {
        DuckLakeConnectorMvccSnapshot s =
                new DuckLakeConnectorMvccSnapshot(42L, Instant.ofEpochMilli(1700000000000L));
        assertThat(s.toOpaqueToken()).isEqualTo("ducklake:42:1700000000000");
    }

    @Test
    void decodeBadMagicRaises() {
        byte[] bad = new byte[DuckLakeConnectorMvccSnapshot.FRAME_BYTES];
        Arrays.fill(bad, (byte) 0xFF);
        DuckLakeConnectorMvccSnapshot.Codec codec = new DuckLakeConnectorMvccSnapshot.Codec();
        assertThatThrownBy(() -> codec.decode(bad))
                .isInstanceOf(DorisConnectorException.class)
                .hasMessageContaining("bad magic");
    }

    @Test
    void decodeWrongLengthRaises() {
        DuckLakeConnectorMvccSnapshot.Codec codec = new DuckLakeConnectorMvccSnapshot.Codec();
        assertThatThrownBy(() -> codec.decode(new byte[7]))
                .isInstanceOf(DorisConnectorException.class)
                .hasMessageContaining("frame length");
    }

    @Test
    void decodeUnsupportedFormatVersionRaises() {
        // Hand-built frame with the correct magic but a bogus format version.
        byte[] frame = new byte[DuckLakeConnectorMvccSnapshot.FRAME_BYTES];
        frame[0] = 0x44; // 'D'
        frame[1] = 0x4C; // 'L'
        frame[2] = 0x41; // 'A'
        frame[3] = 0x4B; // 'K'
        frame[4] = 0;
        frame[5] = 0;
        frame[6] = 0;
        frame[7] = 99;   // format version 99
        DuckLakeConnectorMvccSnapshot.Codec codec = new DuckLakeConnectorMvccSnapshot.Codec();
        assertThatThrownBy(() -> codec.decode(frame))
                .isInstanceOf(DorisConnectorException.class)
                .hasMessageContaining("format version");
    }

    @Test
    void encodeRejectsForeignSnapshotType() {
        DuckLakeConnectorMvccSnapshot.Codec codec = new DuckLakeConnectorMvccSnapshot.Codec();
        ConnectorMvccSnapshot foreign = new ConnectorMvccSnapshot() {
            @Override
            public Instant commitTime() {
                return Instant.EPOCH;
            }

            @Override
            public Optional<ConnectorTableVersion> asVersion() {
                return Optional.empty();
            }

            @Override
            public String toOpaqueToken() {
                return "foreign";
            }
        };
        assertThatThrownBy(() -> codec.encode(foreign))
                .isInstanceOf(DorisConnectorException.class)
                .hasMessageContaining("cannot encode");
    }

    @Test
    void wireFormatMagicAndLayout() {
        // Locks the on-wire layout against accidental drift. Mirror of the
        // wire-format documentation comment on DuckLakeConnectorMvccSnapshot.
        DuckLakeConnectorMvccSnapshot s =
                new DuckLakeConnectorMvccSnapshot(0x0102030405060708L, Instant.ofEpochMilli(0x1112131415161718L));
        byte[] bytes = new DuckLakeConnectorMvccSnapshot.Codec().encode(s);

        // magic = 0x444C414B ("DLAK"), big-endian
        assertThat(bytes[0]).isEqualTo((byte) 0x44);
        assertThat(bytes[1]).isEqualTo((byte) 0x4C);
        assertThat(bytes[2]).isEqualTo((byte) 0x41);
        assertThat(bytes[3]).isEqualTo((byte) 0x4B);
        // format version = 1
        assertThat(bytes[4]).isEqualTo((byte) 0);
        assertThat(bytes[5]).isEqualTo((byte) 0);
        assertThat(bytes[6]).isEqualTo((byte) 0);
        assertThat(bytes[7]).isEqualTo((byte) 1);
        // snapshotId big-endian
        assertThat(Arrays.copyOfRange(bytes, 8, 16))
                .containsExactly(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08);
        // commitTimeMs big-endian
        assertThat(Arrays.copyOfRange(bytes, 16, 24))
                .containsExactly(0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18);
    }
}
