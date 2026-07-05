package dev.brikk.ducklake.doris.plugin

import dev.brikk.ducklake.catalog.DucklakeCatalog
import org.apache.doris.connector.api.DorisConnectorException
import org.apache.doris.connector.api.handle.ConnectorTransaction
import org.apache.doris.thrift.TIcebergCommitData
import org.apache.thrift.TDeserializer
import org.apache.thrift.TException
import org.apache.thrift.protocol.TBinaryProtocol

/**
 * Connector transaction that turns BE write-result fragments into a DuckLake
 * snapshot commit. Mirrors `MaxComputeConnectorTransaction` (the P4 live template):
 * the engine opens it via `ConnectorMetadata.beginTransaction`, the write plan
 * binds the target table onto it, BE feeds per-file fragments back through
 * [addCommitData], and [commit] writes them to the DuckLake catalog.
 *
 * We decode `TIcebergCommitData` because the connector emits a `TIcebergTableSink`
 * (so BE computes per-field-id column stats from the Parquet footer); these ride
 * the existing `iceberg_commit_datas` report-status channel, routed generically to
 * [addCommitData] by P4's `CommitDataSerializer`. Each fragment maps to a
 * `DucklakeWriteFragment` via [DuckLakeIcebergCommitMapper].
 *
 * **Live** — INSERT is admitted via `supportedWriteOperations()` (P6 write
 * unification: the write-plan-provider's default `{INSERT}`); unpartitioned
 * INSERT is validated end-to-end on a real FE+BE (compose smoke, W2). The commit
 * path is also exercised directly by tests (synthetic fragments → real catalog
 * commit), which is the independent headless oracle for the mapping.
 */
internal class DuckLakeConnectorTransaction(
    private val transactionId: Long,
    private val catalog: DucklakeCatalog,
) : ConnectorTransaction {

    private data class Target(
        val tableId: Long,
        val snapshotId: Long,
        val tableDataDir: String?,
        val partitionId: Long?,
    )

    private val commitData = ArrayList<TIcebergCommitData>()

    @Volatile
    private var target: Target? = null

    /**
     * Bind the resolved target table + its data dir (+ the active DuckLake partition
     * id, if the table is partitioned); called by [DuckLakeWritePlanProvider.planWrite].
     * The data dir relativizes the BE's absolute file paths at commit time, and the
     * partition id is stamped on each committed file's fragment.
     */
    fun bindTarget(tableId: Long, snapshotId: Long, tableDataDir: String? = null, partitionId: Long? = null) {
        target = Target(tableId, snapshotId, tableDataDir, partitionId)
    }

    override fun getTransactionId(): Long = transactionId

    override fun addCommitData(commitFragment: ByteArray) {
        val data = TIcebergCommitData()
        try {
            TDeserializer(TBinaryProtocol.Factory()).deserialize(data, commitFragment)
        } catch (e: TException) {
            throw DorisConnectorException("failed to deserialize DuckLake iceberg commit fragment", e)
        }
        synchronized(this) { commitData.add(data) }
    }

    override fun getUpdateCnt(): Long = synchronized(this) {
        commitData.sumOf { if (it.isSetRowCount) it.rowCount else 0L }
    }

    override fun commit() {
        val bound = target
            ?: throw DorisConnectorException("DuckLake write committed with no bound target table")
        val typeByColumnId = catalog.getTableColumns(bound.tableId, bound.snapshotId)
            .associate { it.columnId to it.columnType }
        val fragments = synchronized(this) {
            commitData.map {
                DuckLakeIcebergCommitMapper.toWriteFragment(it, typeByColumnId, bound.tableDataDir, bound.partitionId)
            }
        }
        if (fragments.isNotEmpty()) {
            catalog.commitInsert(bound.tableId, fragments)
        }
    }

    override fun rollback() {
        // DuckLake has no pre-commit catalog state to undo. Any files BE already
        // wrote are simply never registered in a snapshot (orphaned, swept by GC),
        // so rollback only drops the fragments we accumulated.
        synchronized(this) { commitData.clear() }
    }

    override fun close() {
        // No resources held.
    }
}
