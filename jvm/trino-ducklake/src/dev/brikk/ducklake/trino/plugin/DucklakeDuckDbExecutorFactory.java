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
import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Constructs the configured {@link DucklakeDuckDbExecutor} per split. A new
 * executor is returned for each call — executors are per-split state and own a
 * JDBC connection through their lifecycle.
 *
 * <p>The selection happens at catalog-config level via
 * {@link DucklakeConfig#getExecutionEngine()}. Per-split or per-session
 * selection is not exposed (yet) because the choice of engine has operational
 * implications — different deployment topology, different failure modes — that
 * are best made once at catalog configuration time, not per query.
 */
final class DucklakeDuckDbExecutorFactory
{
    private final DucklakeConfig config;

    @Inject
    DucklakeDuckDbExecutorFactory(DucklakeConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    DucklakeDuckDbExecutor create()
    {
        DuckDbTuning tuning = config.toDuckDbTuning();
        // parityExtensionPath semantics differ by engine:
        //   - DUCKDB_LOCAL: local filesystem path the JVM can read directly.
        //   - QUACK: SERVER-SIDE path the Quack DuckDB process can read. The
        //     connector forwards `LOAD '<path>'` over the Quack wrapper, so the
        //     value must point at where the extension lives INSIDE the Quack
        //     container/host, not where it lives on the Trino worker.
        // Single config key is convenient when both processes share a path
        // (mounted at the same in-container location); when they don't, the
        // user has to pick which engine to point it at. Either way, if LOAD
        // fails the executor falls back to the in-tree SQL replay.
        return switch (config.getExecutionEngine()) {
            case DUCKDB_LOCAL -> new InProcessDuckDbExecutor(tuning, config.getDuckdbParityExtensionPath());
            case QUACK -> {
                String host = config.getQuackHost();
                String token = config.getQuackToken();
                if (host == null || host.isBlank()) {
                    throw new TrinoException(CONFIGURATION_INVALID,
                            "ducklake.execution-engine=quack requires ducklake.quack.host");
                }
                if (token == null || token.isBlank()) {
                    throw new TrinoException(CONFIGURATION_INVALID,
                            "ducklake.execution-engine=quack requires ducklake.quack.token");
                }
                yield new QuackDuckDbExecutor(host, config.getQuackPort(), token, tuning,
                        config.getDuckdbParityExtensionPath());
            }
            case SWANLAKE -> throw new TrinoException(NOT_SUPPORTED,
                    "ducklake.execution-engine=swanlake is reserved but not yet implemented. "
                            + "Use 'duckdb_local' or 'quack' for now.");
        };
    }
}
