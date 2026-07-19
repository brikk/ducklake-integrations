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
import io.airlift.slice.SizeOf.instanceSize
import io.trino.spi.connector.ConnectorSplit

/**
 * The single split for a change-feed scan. The change feed computes its whole read plan (which
 * data files to read, which positions were deleted at which snapshot, and the update-pairing set)
 * in one place — the page source — so this split carries only the table id for logging; every
 * other input rides on the [ChangeFeedTableHandle] the engine hands to the page source alongside
 * it. v1 is single-split (unpartitioned read); parallelism is a possible later enhancement.
 */
@JvmRecord
data class ChangeFeedSplit @JsonCreator constructor(
        @get:JvmName("tableId")
        @param:JsonProperty("tableId") val tableId: Long) : ConnectorSplit
{
    override fun getRetainedSizeInBytes(): Long = INSTANCE_SIZE.toLong()

    companion object {
        private val INSTANCE_SIZE: Int = instanceSize(ChangeFeedSplit::class.java)
    }
}
