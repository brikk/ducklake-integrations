package dev.brikk.ducklake.doris.plugin;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;

/**
 * DuckLake-side {@link ConnectorMvccSnapshot}: a {@code (snapshotId, commitTime)}
 * pair pinning a query to a specific DuckLake snapshot.
 *
 * <p>Round-trips across FE&rarr;BE via the {@link Codec} nested type. The
 * wire format is a fixed-size 24-byte binary frame mirroring
 * {@code IcebergConnectorMvccSnapshot}:
 * <pre>
 *   int32   magic       = 0x444C414B  ("DLAK" big-endian)
 *   int32   formatVer   = 1
 *   int64   snapshotId
 *   int64   commitTimeMs (epoch millis)
 * </pre>
 * Decoding rejects any other magic / format version with a
 * {@link DorisConnectorException}.
 */
public final class DuckLakeConnectorMvccSnapshot implements ConnectorMvccSnapshot {

    static final int MAGIC = 0x444C414B; // "DLAK"
    static final int FORMAT_VERSION = 1;
    static final int FRAME_BYTES = 4 + 4 + 8 + 8;

    private final long snapshotId;
    private final Instant commitTime;

    public DuckLakeConnectorMvccSnapshot(long snapshotId, Instant commitTime) {
        this.snapshotId = snapshotId;
        this.commitTime = Objects.requireNonNull(commitTime, "commitTime");
    }

    public long snapshotId() {
        return snapshotId;
    }

    @Override
    public Instant commitTime() {
        return commitTime;
    }

    @Override
    public Optional<ConnectorTableVersion> asVersion() {
        return Optional.of(new ConnectorTableVersion.BySnapshotId(snapshotId));
    }

    @Override
    public String toOpaqueToken() {
        return "ducklake:" + snapshotId + ":" + commitTime.toEpochMilli();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DuckLakeConnectorMvccSnapshot)) {
            return false;
        }
        DuckLakeConnectorMvccSnapshot that = (DuckLakeConnectorMvccSnapshot) o;
        return snapshotId == that.snapshotId && commitTime.equals(that.commitTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, commitTime);
    }

    @Override
    public String toString() {
        return "DuckLakeConnectorMvccSnapshot{snapshotId=" + snapshotId
                + ", commitTime=" + commitTime + '}';
    }

    /**
     * Plugin-private codec registered through
     * {@link DuckLakeConnectorProvider#getMvccSnapshotCodec()}.
     */
    public static final class Codec implements ConnectorMvccSnapshot.Codec {

        @Override
        public byte[] encode(ConnectorMvccSnapshot s) {
            Objects.requireNonNull(s, "snapshot");
            if (!(s instanceof DuckLakeConnectorMvccSnapshot)) {
                throw new DorisConnectorException(
                        "DuckLakeConnectorMvccSnapshot.Codec cannot encode: "
                                + s.getClass().getName());
            }
            DuckLakeConnectorMvccSnapshot ds = (DuckLakeConnectorMvccSnapshot) s;
            ByteArrayOutputStream baos = new ByteArrayOutputStream(FRAME_BYTES);
            try (DataOutputStream out = new DataOutputStream(baos)) {
                out.writeInt(MAGIC);
                out.writeInt(FORMAT_VERSION);
                out.writeLong(ds.snapshotId);
                out.writeLong(ds.commitTime.toEpochMilli());
            } catch (IOException e) {
                throw new DorisConnectorException(
                        "Failed to encode DuckLakeConnectorMvccSnapshot", e);
            }
            return baos.toByteArray();
        }

        @Override
        public ConnectorMvccSnapshot decode(byte[] bytes) {
            Objects.requireNonNull(bytes, "bytes");
            if (bytes.length != FRAME_BYTES) {
                throw new DorisConnectorException(
                        "DuckLake mvcc snapshot frame length must be " + FRAME_BYTES
                                + " bytes, got " + bytes.length);
            }
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
                int magic = in.readInt();
                if (magic != MAGIC) {
                    throw new DorisConnectorException(
                            "DuckLake mvcc snapshot bad magic: 0x"
                                    + Integer.toHexString(magic));
                }
                int formatVer = in.readInt();
                if (formatVer != FORMAT_VERSION) {
                    throw new DorisConnectorException(
                            "DuckLake mvcc snapshot unsupported format version: " + formatVer);
                }
                long snapshotId = in.readLong();
                long commitMs = in.readLong();
                return new DuckLakeConnectorMvccSnapshot(snapshotId, Instant.ofEpochMilli(commitMs));
            } catch (IOException e) {
                throw new DorisConnectorException(
                        "Failed to decode DuckLakeConnectorMvccSnapshot", e);
            }
        }
    }
}
