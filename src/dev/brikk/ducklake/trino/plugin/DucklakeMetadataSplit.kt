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

@JvmRecord
data class DucklakeMetadataSplit @JsonCreator constructor(
        @get:JvmName("baseTableId")
        @param:JsonProperty("baseTableId") val baseTableId: Long,
        @get:JvmName("snapshotId")
        @param:JsonProperty("snapshotId") val snapshotId: Long,
        @get:JvmName("metadataTableType")
        @param:JsonProperty("metadataTableType") val metadataTableType: DucklakeMetadataTableType)
        : ConnectorSplit
{
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

    companion object {
        private val INSTANCE_SIZE: Int = instanceSize(DucklakeMetadataSplit::class.java)
    }
}
