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

import com.google.inject.Inject
import io.airlift.json.JsonCodec
import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment
import dev.brikk.ducklake.catalog.DucklakeWriteFragment

import java.util.Objects.requireNonNull

open class DucklakeMetadataFactory @Inject constructor(
        catalog: DucklakeCatalog,
        typeConverter: DucklakeTypeConverter,
        snapshotResolver: DucklakeSnapshotResolver,
        fragmentCodec: JsonCodec<DucklakeWriteFragment>,
        deleteFragmentCodec: JsonCodec<DucklakeDeleteFragment>,
        pathResolver: DucklakePathResolver,
        config: DucklakeConfig)
{
    private val catalog: DucklakeCatalog = requireNonNull(catalog, "catalog is null")
    private val typeConverter: DucklakeTypeConverter = requireNonNull(typeConverter, "typeConverter is null")
    private val snapshotResolver: DucklakeSnapshotResolver = requireNonNull(snapshotResolver, "snapshotResolver is null")
    private val fragmentCodec: JsonCodec<DucklakeWriteFragment> = requireNonNull(fragmentCodec, "fragmentCodec is null")
    private val deleteFragmentCodec: JsonCodec<DucklakeDeleteFragment> = requireNonNull(deleteFragmentCodec, "deleteFragmentCodec is null")
    private val pathResolver: DucklakePathResolver = requireNonNull(pathResolver, "pathResolver is null")
    private val temporalPartitionEncoding: DucklakeTemporalPartitionEncoding = requireNonNull(config, "config is null").getTemporalPartitionEncoding()

    open fun create(): DucklakeMetadata
    {
        return DucklakeMetadata(catalog, typeConverter, snapshotResolver, fragmentCodec, deleteFragmentCodec, pathResolver, temporalPartitionEncoding)
    }
}
