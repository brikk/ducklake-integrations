package dev.brikk.ducklake.doris.plugin

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.time.Instant
import java.util.Objects
import java.util.Optional

import org.apache.doris.connector.api.DorisConnectorException
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion

/**
 * DuckLake-side [ConnectorMvccSnapshot]: a `(snapshotId, commitTime)`
 * pair pinning a query to a specific DuckLake snapshot.
 *
 * Round-trips across FE&rarr;BE via the [Codec] nested type. The
 * wire format is a fixed-size 24-byte binary frame mirroring
 * `IcebergConnectorMvccSnapshot`:
 * ```
 *   int32   magic       = 0x444C414B  ("DLAK" big-endian)
 *   int32   formatVer   = 1
 *   int64   snapshotId
 *   int64   commitTimeMs (epoch millis)
 * ```
 * Decoding rejects any other magic / format version with a
 * [DorisConnectorException].
 */
class DuckLakeConnectorMvccSnapshot(
    private val snapshotId: Long,
    commitTime: Instant,
) : ConnectorMvccSnapshot {

    private val commitTime: Instant = Objects.requireNonNull(commitTime, "commitTime")

    fun snapshotId(): Long = snapshotId

    override fun commitTime(): Instant = commitTime

    override fun asVersion(): Optional<ConnectorTableVersion> =
        Optional.of(ConnectorTableVersion.BySnapshotId(snapshotId))

    override fun toOpaqueToken(): String =
        "ducklake:" + snapshotId + ":" + commitTime.toEpochMilli()

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other !is DuckLakeConnectorMvccSnapshot) {
            return false
        }
        return snapshotId == other.snapshotId && commitTime == other.commitTime
    }

    override fun hashCode(): Int = Objects.hash(snapshotId, commitTime)

    override fun toString(): String =
        "DuckLakeConnectorMvccSnapshot{snapshotId=" + snapshotId +
            ", commitTime=" + commitTime + '}'

    companion object {
        internal const val MAGIC: Int = 0x444C414B // "DLAK"
        internal const val FORMAT_VERSION: Int = 1
        internal const val FRAME_BYTES: Int = 4 + 4 + 8 + 8
    }

    /**
     * Plugin-private codec registered through
     * [DuckLakeConnectorProvider.getMvccSnapshotCodec].
     */
    class Codec : ConnectorMvccSnapshot.Codec {

        override fun encode(s: ConnectorMvccSnapshot): ByteArray {
            Objects.requireNonNull(s, "snapshot")
            if (s !is DuckLakeConnectorMvccSnapshot) {
                throw DorisConnectorException(
                    "DuckLakeConnectorMvccSnapshot.Codec cannot encode: " +
                        s.javaClass.name,
                )
            }
            val baos = ByteArrayOutputStream(FRAME_BYTES)
            try {
                DataOutputStream(baos).use { out ->
                    out.writeInt(MAGIC)
                    out.writeInt(FORMAT_VERSION)
                    out.writeLong(s.snapshotId)
                    out.writeLong(s.commitTime.toEpochMilli())
                }
            } catch (e: IOException) {
                throw DorisConnectorException(
                    "Failed to encode DuckLakeConnectorMvccSnapshot", e,
                )
            }
            return baos.toByteArray()
        }

        override fun decode(bytes: ByteArray): ConnectorMvccSnapshot {
            Objects.requireNonNull(bytes, "bytes")
            if (bytes.size != FRAME_BYTES) {
                throw DorisConnectorException(
                    "DuckLake mvcc snapshot frame length must be " + FRAME_BYTES +
                        " bytes, got " + bytes.size,
                )
            }
            try {
                DataInputStream(ByteArrayInputStream(bytes)).use { `in` ->
                    val magic = `in`.readInt()
                    if (magic != MAGIC) {
                        throw DorisConnectorException(
                            "DuckLake mvcc snapshot bad magic: 0x" +
                                Integer.toHexString(magic),
                        )
                    }
                    val formatVer = `in`.readInt()
                    if (formatVer != FORMAT_VERSION) {
                        throw DorisConnectorException(
                            "DuckLake mvcc snapshot unsupported format version: " + formatVer,
                        )
                    }
                    val snapshotId = `in`.readLong()
                    val commitMs = `in`.readLong()
                    return DuckLakeConnectorMvccSnapshot(snapshotId, Instant.ofEpochMilli(commitMs))
                }
            } catch (e: IOException) {
                throw DorisConnectorException(
                    "Failed to decode DuckLakeConnectorMvccSnapshot", e,
                )
            }
        }
    }
}
