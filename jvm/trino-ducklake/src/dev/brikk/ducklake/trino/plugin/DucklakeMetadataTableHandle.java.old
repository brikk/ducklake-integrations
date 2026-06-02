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
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

public record DucklakeMetadataTableHandle(
        @JsonProperty("schemaName") String schemaName,
        @JsonProperty("tableName") String tableName,
        @JsonProperty("baseTableName") String baseTableName,
        @JsonProperty("baseTableId") long baseTableId,
        @JsonProperty("snapshotId") long snapshotId,
        @JsonProperty("metadataTableType") DucklakeMetadataTableType metadataTableType)
        implements ConnectorTableHandle
{
    @JsonCreator
    public DucklakeMetadataTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(baseTableName, "baseTableName is null");
        requireNonNull(metadataTableType, "metadataTableType is null");
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }
}
