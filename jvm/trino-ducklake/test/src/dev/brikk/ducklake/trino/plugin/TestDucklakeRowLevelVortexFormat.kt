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

import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode

/**
 * Row-level DELETE/UPDATE/MERGE over `vortex` data files — see
 * [AbstractDucklakeRowLevelFormatTest] for the test matrix. Reads run through the FileScan
 * `read_vortex('path')` path, so position-delete filtering depends on `read_vortex`
 * returning rows in stable file order across scans.
 *
 * Network-gated like [TestDucklakeVortexFormat]: skips when the vortex extension can't be
 * installed. SAME_THREAD: writes to the shared catalog; concurrent commits would race the
 * snapshot retry.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TestDucklakeRowLevelVortexFormat : AbstractDucklakeRowLevelFormatTest() {
    override fun isolatedCatalogName(): String = "rowlevel-vortex"

    override fun format(): String = DucklakeSessionProperties.FORMAT_VORTEX

    override fun assumeFormatAvailable() {
        FormatExtensionAssumptions.assumeDuckDbExtensionAvailable("vortex")
    }
}
