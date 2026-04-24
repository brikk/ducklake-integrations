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
import dev.brikk.ducklake.catalog.DucklakeColumn;
import dev.brikk.ducklake.catalog.DucklakePartitionSpec;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record DucklakeWritableTableHandle(
        @JsonProperty("schemaName") String schemaName,
        @JsonProperty("tableName") String tableName,
        @JsonProperty("tableId") long tableId,
        @JsonProperty("columns") List<DucklakeColumnHandle> columns,
        @JsonProperty("allCatalogColumns") List<DucklakeColumn> allCatalogColumns,
        @JsonProperty("tableDataPath") String tableDataPath,
        @JsonProperty("partitionSpec") Optional<DucklakePartitionSpec> partitionSpec,
        @JsonProperty("temporalPartitionEncoding") DucklakeTemporalPartitionEncoding temporalPartitionEncoding)
        implements ConnectorInsertTableHandle, ConnectorOutputTableHandle
{
    @JsonCreator
    public DucklakeWritableTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columns, "columns is null");
        columns = List.copyOf(columns);
        requireNonNull(allCatalogColumns, "allCatalogColumns is null");
        allCatalogColumns = List.copyOf(allCatalogColumns);
        requireNonNull(tableDataPath, "tableDataPath is null");
        requireNonNull(partitionSpec, "partitionSpec is null");
        requireNonNull(temporalPartitionEncoding, "temporalPartitionEncoding is null");
    }

    @Override
    public String toString()
    {
        return schemaName + "." + tableName + "#" + tableId;
    }
}
