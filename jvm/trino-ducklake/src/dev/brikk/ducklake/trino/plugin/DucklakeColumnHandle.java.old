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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

/**
 * Handle for a Ducklake column.
 */
public record DucklakeColumnHandle(
        @JsonProperty("columnId") long columnId,
        @JsonProperty("columnName") String columnName,
        @JsonProperty("columnType") Type columnType,
        @JsonProperty("nullable") boolean nullable)
        implements ColumnHandle
{
    private static final int INSTANCE_SIZE = instanceSize(DucklakeColumnHandle.class);

    public static final long ROW_ID_COLUMN_ID = -100;
    public static final String ROW_ID_COLUMN_NAME = "$row_id";

    public static DucklakeColumnHandle rowIdColumnHandle()
    {
        return new DucklakeColumnHandle(ROW_ID_COLUMN_ID, ROW_ID_COLUMN_NAME, BIGINT, false);
    }

    public boolean isRowIdColumn()
    {
        return columnId == ROW_ID_COLUMN_ID;
    }

    @JsonCreator
    public DucklakeColumnHandle
    {
        requireNonNull(columnName, "columnName is null");
        requireNonNull(columnType, "columnType is null");
    }

    @Override
    public String toString()
    {
        return columnName + ":" + columnType;
    }

    public long getRetainedSizeInBytes()
    {
        // columnType is omitted as Type instances are shared by Trino type registry
        return INSTANCE_SIZE
                + sizeOf(columnId)
                + estimatedSizeOf(columnName)
                + sizeOf(nullable);
    }
}
