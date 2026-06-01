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
import java.util.Objects.requireNonNull

class DucklakeMetadataSplit
        : ConnectorSplit
{
    private val baseTableId: Long
    private val snapshotId: Long
    private val metadataTableType: DucklakeMetadataTableType

    @JsonCreator
    constructor(
            @JsonProperty("baseTableId") baseTableId: Long,
            @JsonProperty("snapshotId") snapshotId: Long,
            @JsonProperty("metadataTableType") metadataTableType: DucklakeMetadataTableType)
    {
        this.baseTableId = baseTableId
        this.snapshotId = snapshotId
        this.metadataTableType = requireNonNull(metadataTableType, "metadataTableType is null")
    }

    @JsonProperty("baseTableId")
    fun baseTableId(): Long = baseTableId

    @JsonProperty("snapshotId")
    fun snapshotId(): Long = snapshotId

    @JsonProperty("metadataTableType")
    fun metadataTableType(): DucklakeMetadataTableType = metadataTableType

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
        if (other !is DucklakeMetadataSplit) return false
        return baseTableId == other.baseTableId
                && snapshotId == other.snapshotId
                && metadataTableType == other.metadataTableType
    }

    override fun hashCode(): Int
    {
        return Objects.hash(baseTableId, snapshotId, metadataTableType)
    }

    override fun toString(): String
    {
        return "DucklakeMetadataSplit[baseTableId=$baseTableId, snapshotId=$snapshotId, metadataTableType=$metadataTableType]"
    }

    companion object {
        private val INSTANCE_SIZE: Int = instanceSize(DucklakeMetadataSplit::class.java)
    }
}
