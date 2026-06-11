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
import io.airlift.slice.SizeOf.estimatedSizeOf
import io.airlift.slice.SizeOf.instanceSize
import io.trino.spi.connector.ConnectorSplit

/**
 * One split per lance dataset directory for the lance search table functions
 * (`lance_vector_search` / `lance_fts` / `lance_hybrid_search`). The remaining inputs (search
 * arguments + output layout) ride on the [LanceSearchHandle], which the engine hands to the
 * split processor alongside this split.
 */
@JvmRecord
data class LanceSearchSplit @JsonCreator constructor(
        @get:JvmName("datasetPath")
        @param:JsonProperty("datasetPath") val datasetPath: String) : ConnectorSplit
{
    override fun getRetainedSizeInBytes(): Long =
        INSTANCE_SIZE.toLong() + estimatedSizeOf(datasetPath)

    companion object {
        private val INSTANCE_SIZE: Int = instanceSize(LanceSearchSplit::class.java)
    }
}
