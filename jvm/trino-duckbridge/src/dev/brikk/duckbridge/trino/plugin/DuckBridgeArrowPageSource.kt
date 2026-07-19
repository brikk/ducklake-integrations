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
package dev.brikk.duckbridge.trino.plugin

import io.airlift.log.Logger
import io.trino.spi.Page
import io.trino.spi.connector.ConnectorPageSource
import io.trino.spi.connector.SourcePage
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.duckdb.DuckDBConnection
import org.duckdb.DuckDBResultSet
import java.io.IOException
import java.sql.PreparedStatement

/**
 * T2 Arrow page source: streams `DuckDBResultSet.arrowExportStream` batches from a prepared DuckDB
 * connection and decodes each to a Trino [Page] via [DuckBridgeArrowToPageConverter].
 *
 * The [preparedStatement] is produced by base-jdbc's `JdbcClient.buildSql` on [connection], so the
 * projection and bound parameters are exactly what the default JDBC path would use — no manual SQL
 * rendering. This is the DUCKDB_LOCAL data plane; the loop mirrors the DuckLake connector's
 * `DuckDbFilePageSource`.
 */
class DuckBridgeArrowPageSource(
    private val connection: DuckDBConnection,
    private val preparedStatement: PreparedStatement,
    private val converter: DuckBridgeArrowToPageConverter,
    private val projectedColumnCount: Int,
) : ConnectorPageSource {
    private val allocator: BufferAllocator = RootAllocator()
    private var arrowReader: ArrowReader? = null
    private var finished = false
    private var completedBytes = 0L

    @Deprecated("SPI-deprecated but still abstract in 483", ReplaceWith("getMetrics()"))
    override fun getCompletedBytes(): Long = completedBytes

    @Deprecated("SPI-deprecated but still abstract in 483", ReplaceWith("getMetrics()"))
    override fun getReadTimeNanos(): Long = 0

    override fun isFinished(): Boolean = finished

    override fun getNextSourcePage(): SourcePage? {
        if (finished) {
            return null
        }
        try {
            val reader = ensureReader()
            if (!reader.loadNextBatch()) {
                finished = true
                return null
            }
            val root = reader.vectorSchemaRoot
            val page: Page =
                if (projectedColumnCount == 0) {
                    // Empty projection (e.g. count(*)): base-jdbc's buildSql emits a single dummy
                    // column so positions flow; we only need the row count, not the dummy values.
                    Page(root.rowCount)
                } else {
                    converter.convert(root)
                }
            completedBytes += page.sizeInBytes
            return SourcePage.create(page)
        } catch (e: IOException) {
            throw io.trino.spi.TrinoException(io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR, "DuckBridge Arrow read failed", e)
        }
    }

    private fun ensureReader(): ArrowReader {
        arrowReader?.let { return it }
        val resultSet = preparedStatement.executeQuery() as DuckDBResultSet
        val reader = resultSet.arrowExportStream(allocator, ARROW_BATCH_SIZE) as ArrowReader
        arrowReader = reader
        return reader
    }

    override fun getMemoryUsage(): Long = allocator.allocatedMemory

    override fun close() {
        var suppressed: Throwable? = null
        for (resource in listOfNotNull(arrowReader, preparedStatement, connection, allocator)) {
            try {
                resource.close()
            } catch (@Suppress("TooGenericExceptionCaught") t: Throwable) {
                if (suppressed == null) {
                    suppressed = t
                }
            }
        }
        suppressed?.let { log.warn(it, "Error closing DuckBridge Arrow page source") }
    }

    private companion object {
        private val log: Logger = Logger.get(DuckBridgeArrowPageSource::class.java)
        private const val ARROW_BATCH_SIZE: Long = 1024
    }
}
