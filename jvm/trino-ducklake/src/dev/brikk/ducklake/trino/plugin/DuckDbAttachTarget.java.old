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

import java.nio.file.Path;

/**
 * What a {@link DuckDbFilePageSource} ATTACHes inside its per-split DuckDB instance.
 * <p>{@link LocalPath} is the materialize-then-attach path: cache has already pulled
 * the {@code .db} file to a local filesystem path. {@link HttpfsS3} is the streaming
 * path: the page source loads DuckDB's httpfs extension, creates an S3 secret, and
 * ATTACHes the {@code s3://...} URL directly — no full download.
 */
sealed interface DuckDbAttachTarget
{
    record LocalPath(Path path) implements DuckDbAttachTarget {}

    record HttpfsS3(String s3Url, DuckDbS3Config s3Config) implements DuckDbAttachTarget {}
}
