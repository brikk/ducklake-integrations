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

import com.google.inject.Inject

import java.util.concurrent.ConcurrentHashMap

import java.util.Objects.requireNonNull

/**
 * Manages transactions for the Ducklake connector.
 * Each transaction maintains its own metadata instance.
 */
open class DucklakeTransactionManager @Inject constructor(metadataFactory: DucklakeMetadataFactory)
{
    private val metadataFactory: DucklakeMetadataFactory = requireNonNull(metadataFactory, "metadataFactory is null")
    private val transactions: MutableMap<DucklakeTransactionHandle, DucklakeMetadata> = ConcurrentHashMap()

    open fun begin(transaction: DucklakeTransactionHandle)
    {
        requireNonNull(transaction, "transaction is null")
        val metadata = metadataFactory.create()
        transactions.put(transaction, metadata)
    }

    open fun getMetadata(transaction: io.trino.spi.connector.ConnectorTransactionHandle): DucklakeMetadata
    {
        @Suppress("UNCHECKED_CAST")
        val metadata = (transactions as Map<Any, DucklakeMetadata>)[transaction]
        if (metadata == null) {
            throw IllegalArgumentException("Unknown transaction: " + transaction)
        }
        return metadata
    }

    open fun commit(transaction: DucklakeTransactionHandle)
    {
        requireNonNull(transaction, "transaction is null")
        val metadata = transactions.remove(transaction)
        if (metadata == null) {
            throw IllegalArgumentException("Unknown transaction: " + transaction)
        }
        // For read-only transactions, nothing to commit
        // Write transactions will be implemented later
    }

    open fun rollback(transaction: DucklakeTransactionHandle)
    {
        requireNonNull(transaction, "transaction is null")
        val metadata = transactions.remove(transaction)
        if (metadata == null) {
            throw IllegalArgumentException("Unknown transaction: " + transaction)
        }
        // For read-only transactions, nothing to rollback
        // Write transactions will be implemented later
    }
}
