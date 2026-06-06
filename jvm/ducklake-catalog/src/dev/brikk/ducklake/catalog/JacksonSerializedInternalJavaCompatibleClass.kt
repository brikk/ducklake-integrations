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
package dev.brikk.ducklake.catalog

/**
 * Marks a class that is Jackson-serialized for OUR OWN internal use only — ephemeral Trino
 * coordinator<->worker write/delete fragments and DTOs that ride inside Trino handles/splits
 * round-tripped through Trino's `/v1/task` JSON. We control both ends, so the wire shape is ours
 * to change (keep serialize+deserialize symmetric). NOT a cross-system contract.
 *
 * "JavaCompatible" is the load-bearing part: these are serialized by airlift's `ObjectMapper`
 * (used by both our `JsonCodec` and Trino's `/v1/task` server mapper). Airlift's provider registers
 * a FIXED module list — `Jdk8Module`, `JavaTimeModule`, `GuavaModule`, `ParameterNamesModule`,
 * `RecordAutoDetectModule` — and disables every `AUTO_DETECT_*` ("all properties must be explicit").
 * There is NO `jackson-module-kotlin`, and a plugin cannot add one to Trino's mapper (it builds a
 * fresh provider per request from that fixed list). So a marked class MUST stay deserializable by
 * those modules alone, i.e. **Java-record-shaped**: keep `@JvmRecord` (so `RecordAutoDetectModule` +
 * `ParameterNamesModule` map components by name — that's why no `@JsonCreator`/`@JsonProperty` is
 * needed). Adding a Kotlin `data class` without `@JvmRecord` here will fail at runtime (Kotlin's
 * Intrinsics non-null check throws when the module-less mapper passes a null), not at compile time.
 * Pair with class-level `@JsonInclude(NON_NULL)` to keep nullable fields ABSENT on the wire
 * (matching airlift's default `NON_ABSENT` for the old `Optional` fields).
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class JacksonSerializedInternalJavaCompatibleClass
