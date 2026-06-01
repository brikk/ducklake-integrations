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

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import dev.brikk.ducklake.catalog.DucklakeCatalog;
import dev.brikk.ducklake.catalog.DucklakeDeleteFragment;
import dev.brikk.ducklake.catalog.DucklakeWriteFragment;

import static java.util.Objects.requireNonNull;

public class DucklakeMetadataFactory
{
    private final DucklakeCatalog catalog;
    private final DucklakeTypeConverter typeConverter;
    private final DucklakeSnapshotResolver snapshotResolver;
    private final JsonCodec<DucklakeWriteFragment> fragmentCodec;
    private final JsonCodec<DucklakeDeleteFragment> deleteFragmentCodec;
    private final DucklakePathResolver pathResolver;
    private final DucklakeTemporalPartitionEncoding temporalPartitionEncoding;

    @Inject
    public DucklakeMetadataFactory(
            DucklakeCatalog catalog,
            DucklakeTypeConverter typeConverter,
            DucklakeSnapshotResolver snapshotResolver,
            JsonCodec<DucklakeWriteFragment> fragmentCodec,
            JsonCodec<DucklakeDeleteFragment> deleteFragmentCodec,
            DucklakePathResolver pathResolver,
            DucklakeConfig config)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.typeConverter = requireNonNull(typeConverter, "typeConverter is null");
        this.snapshotResolver = requireNonNull(snapshotResolver, "snapshotResolver is null");
        this.fragmentCodec = requireNonNull(fragmentCodec, "fragmentCodec is null");
        this.deleteFragmentCodec = requireNonNull(deleteFragmentCodec, "deleteFragmentCodec is null");
        this.pathResolver = requireNonNull(pathResolver, "pathResolver is null");
        this.temporalPartitionEncoding = requireNonNull(config, "config is null").getTemporalPartitionEncoding();
    }

    public DucklakeMetadata create()
    {
        return new DucklakeMetadata(catalog, typeConverter, snapshotResolver, fragmentCodec, deleteFragmentCodec, pathResolver, temporalPartitionEncoding);
    }
}
