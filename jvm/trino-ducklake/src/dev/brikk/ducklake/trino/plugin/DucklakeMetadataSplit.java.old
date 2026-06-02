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
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public record DucklakeMetadataSplit(
        @JsonProperty("baseTableId") long baseTableId,
        @JsonProperty("snapshotId") long snapshotId,
        @JsonProperty("metadataTableType") DucklakeMetadataTableType metadataTableType)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(DucklakeMetadataSplit.class);

    @JsonCreator
    public DucklakeMetadataSplit
    {
        requireNonNull(metadataTableType, "metadataTableType is null");
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return List.of();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
