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
package dev.brikk.ducklake.trino.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record DucklakeMergeTableHandle(
        @JsonProperty("tableHandle") DucklakeTableHandle tableHandle,
        @JsonProperty("insertHandle") DucklakeWritableTableHandle insertHandle,
        @JsonProperty("dataFileRanges") List<DataFileRange> dataFileRanges)
        implements ConnectorMergeTableHandle
{
    @JsonCreator
    public DucklakeMergeTableHandle
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(insertHandle, "insertHandle is null");
        requireNonNull(dataFileRanges, "dataFileRanges is null");
        dataFileRanges = List.copyOf(dataFileRanges);
    }

    @Override
    public ConnectorTableHandle getTableHandle()
    {
        return tableHandle;
    }

    public record DataFileRange(
            @JsonProperty("dataFileId") long dataFileId,
            @JsonProperty("rowIdStart") long rowIdStart,
            @JsonProperty("recordCount") long recordCount)
    {
        @JsonCreator
        public DataFileRange {}

        public boolean containsRowId(long rowId)
        {
            return rowId >= rowIdStart && rowId < rowIdStart + recordCount;
        }
    }
}
