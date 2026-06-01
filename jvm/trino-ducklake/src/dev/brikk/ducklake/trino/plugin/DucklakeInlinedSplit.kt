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

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import io.trino.spi.HostAddress
import io.trino.spi.connector.ConnectorSplit

import io.airlift.slice.SizeOf.instanceSize
import java.util.Objects

/**
 * Represents a split for reading DuckLake inlined data from the metadata catalog.
 * Carries only the coordinates needed to query the inlined data table;
 * actual data is read at page source creation time.
 */
class DucklakeInlinedSplit
        : ConnectorSplit
{
    private val tableId: Long
    private val schemaVersion: Long
    private val snapshotId: Long

    @JsonCreator
    constructor(
            @JsonProperty("tableId") tableId: Long,
            @JsonProperty("schemaVersion") schemaVersion: Long,
            @JsonProperty("snapshotId") snapshotId: Long)
    {
        this.tableId = tableId
        this.schemaVersion = schemaVersion
        this.snapshotId = snapshotId
    }

    @JsonProperty("tableId")
    fun tableId(): Long = tableId

    @JsonProperty("schemaVersion")
    fun schemaVersion(): Long = schemaVersion

    @JsonProperty("snapshotId")
    fun snapshotId(): Long = snapshotId

    override fun isRemotelyAccessible(): Boolean
    {
        return true
    }

    override fun getAddresses(): List<HostAddress>
    {
        return listOf()
    }

    override fun getRetainedSizeInBytes(): Long
    {
        return INSTANCE_SIZE.toLong()
    }

    override fun equals(other: Any?): Boolean
    {
        if (this === other) return true
        if (other !is DucklakeInlinedSplit) return false
        return tableId == other.tableId
                && schemaVersion == other.schemaVersion
                && snapshotId == other.snapshotId
    }

    override fun hashCode(): Int
    {
        return Objects.hash(tableId, schemaVersion, snapshotId)
    }

    override fun toString(): String
    {
        return "DucklakeInlinedSplit[tableId=$tableId, schemaVersion=$schemaVersion, snapshotId=$snapshotId]"
    }

    companion object {
        private val INSTANCE_SIZE: Int = instanceSize(DucklakeInlinedSplit::class.java)
    }
}
