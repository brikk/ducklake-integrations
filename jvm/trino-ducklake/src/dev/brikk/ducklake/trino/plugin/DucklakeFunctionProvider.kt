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
import io.trino.spi.StandardErrorCode.NOT_SUPPORTED
import io.trino.spi.TrinoException
import io.trino.spi.function.FunctionProvider
import io.trino.spi.function.table.ConnectorTableFunctionHandle
import io.trino.spi.function.table.TableFunctionProcessorProvider

/**
 * Dispatches the engine's "who executes this table function?" lookup to the matching
 * processor provider. The only remaining table function is the change feed, which is always
 * planned as a scan via [DucklakeMetadata.applyTableFunction]; the processor path is never taken.
 */
class DucklakeFunctionProvider @Inject constructor() : FunctionProvider {

    override fun getTableFunctionProcessorProvider(
            functionHandle: ConnectorTableFunctionHandle): TableFunctionProcessorProvider {
        if (functionHandle is ChangeFeedHandle) {
            // The change feed is always planned as a scan via DucklakeMetadata.applyTableFunction
            // (it reuses the connector's data-file read path + pushdown-free filtering above the
            // scan); the table-function processor path is never taken. Fail clearly if it ever is.
            throw TrinoException(NOT_SUPPORTED,
                    "The change-feed table function must be planned as a table scan (applyTableFunction)")
        }
        throw IllegalArgumentException("Unknown table function handle: ${functionHandle.javaClass.name}")
    }
}
