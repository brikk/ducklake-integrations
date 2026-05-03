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

import com.google.common.collect.ImmutableList;
import dev.brikk.ducklake.catalog.DucklakeColumn;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;

import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static java.util.Objects.requireNonNull;

/**
 * Enforces DuckLake unsigned integer type ranges on the write path.
 *
 * <p>DuckLake stores true unsigned types (uint8, uint16, uint32, uint64) but Trino has no
 * native unsigned integer types. The connector widens unsigned types to the next signed
 * Trino type on read (uint8 → SMALLINT, uint16 → INTEGER, uint32 → BIGINT, uint64 →
 * DECIMAL(20, 0)); without this checker, a write of e.g. SMALLINT 300 into a uint8 column
 * would silently wrap to 44 inside DuckDB because Parquet carries only the physical
 * primitive type, not the logical unsigned bound. See
 * dev-docs/COMPARE-pg_ducklake.md B5.
 *
 * <p>Built once per page sink and short-circuits to a no-op when no column in the table
 * is unsigned (the overwhelmingly common case). When unsigned columns are present, only
 * those channels are scanned — other channels are untouched.
 */
final class DucklakeUnsignedRangeChecker
{
    private static final DucklakeUnsignedRangeChecker NO_OP = new DucklakeUnsignedRangeChecker(List.of());

    private static final long UINT8_MAX = (1L << 8) - 1;
    private static final long UINT16_MAX = (1L << 16) - 1;
    private static final long UINT32_MAX = (1L << 32) - 1;

    private final List<Check> checks;

    /**
     * Build a checker for the given page channel layout. Channels whose DuckLake type is
     * not an unsigned integer are skipped.
     *
     * @param columns the column handles in channel order (position i of any appended Page
     *                is the value for columns.get(i))
     * @param catalogColumnTypeById map from columnId to the raw DuckLake type string
     *                              (e.g. "uint8") — typically derived from
     *                              {@code DucklakeWritableTableHandle.allCatalogColumns()}
     */
    static DucklakeUnsignedRangeChecker build(
            List<DucklakeColumnHandle> columns,
            Map<Long, String> catalogColumnTypeById)
    {
        requireNonNull(columns, "columns is null");
        requireNonNull(catalogColumnTypeById, "catalogColumnTypeById is null");

        ImmutableList.Builder<Check> builder = ImmutableList.builder();
        for (int channel = 0; channel < columns.size(); channel++) {
            DucklakeColumnHandle column = columns.get(channel);
            String ducklakeType = catalogColumnTypeById.get(column.columnId());
            if (ducklakeType == null) {
                continue;
            }
            Check check = checkerFor(channel, column, ducklakeType);
            if (check != null) {
                builder.add(check);
            }
        }
        List<Check> checks = builder.build();
        return checks.isEmpty() ? NO_OP : new DucklakeUnsignedRangeChecker(checks);
    }

    static DucklakeUnsignedRangeChecker build(
            List<DucklakeColumnHandle> columns,
            List<DucklakeColumn> allCatalogColumns)
    {
        requireNonNull(allCatalogColumns, "allCatalogColumns is null");
        return build(columns, allCatalogColumns.stream()
                .collect(java.util.stream.Collectors.toMap(DucklakeColumn::columnId, DucklakeColumn::columnType, (a, _) -> a)));
    }

    private DucklakeUnsignedRangeChecker(List<Check> checks)
    {
        this.checks = checks;
    }

    boolean isNoOp()
    {
        return checks.isEmpty();
    }

    /**
     * Validates a page against all unsigned-column ranges. Throws on the first out-of-range
     * value. Must be called before the page is handed to the Parquet writer — once the
     * writer has consumed the page, the offending value has already been encoded and a
     * throw would leave a half-written row group behind.
     */
    void validate(Page page)
    {
        if (checks.isEmpty()) {
            return;
        }
        for (Check check : checks) {
            check.validate(page.getBlock(check.channel));
        }
    }

    private static Check checkerFor(int channel, DucklakeColumnHandle column, String ducklakeType)
    {
        // DuckLake type strings are lowercase per upstream ducklake_types.cpp. Defensive
        // normalization in case a JDBC driver up-cases the column.
        String normalized = ducklakeType.toLowerCase(java.util.Locale.ROOT);
        return switch (normalized) {
            case "uint8" -> new SmallintUnsignedCheck(channel, column, "uint8", UINT8_MAX);
            case "uint16" -> new IntegerUnsignedCheck(channel, column, "uint16", UINT16_MAX);
            case "uint32" -> new BigintUnsignedCheck(channel, column, "uint32", UINT32_MAX);
            case "uint64" -> {
                // uint64 read maps to DECIMAL(20, 0); anything else means the table schema
                // was mutated out from under us and we should skip rather than throw —
                // this is defensive and shouldn't trigger in practice.
                if (column.columnType() instanceof DecimalType decimalType && !decimalType.isShort()) {
                    yield new Uint64Check(channel, column);
                }
                yield null;
            }
            default -> null;
        };
    }

    private abstract static sealed class Check
    {
        final int channel;
        final DucklakeColumnHandle column;
        final String ducklakeType;

        Check(int channel, DucklakeColumnHandle column, String ducklakeType)
        {
            this.channel = channel;
            this.column = column;
            this.ducklakeType = ducklakeType;
        }

        abstract void validate(Block block);

        TrinoException overflow(Object value, String maxLiteral)
        {
            return new TrinoException(
                    NUMERIC_VALUE_OUT_OF_RANGE,
                    String.format(
                            "Value %s for column \"%s\" is out of range for DuckLake %s (allowed: 0..%s)",
                            value, column.columnName(), ducklakeType, maxLiteral));
        }
    }

    private static final class SmallintUnsignedCheck
            extends Check
    {
        private final long max;

        SmallintUnsignedCheck(int channel, DucklakeColumnHandle column, String ducklakeType, long max)
        {
            super(channel, column, ducklakeType);
            this.max = max;
        }

        @Override
        void validate(Block block)
        {
            int positions = block.getPositionCount();
            for (int i = 0; i < positions; i++) {
                if (block.isNull(i)) {
                    continue;
                }
                short value = SmallintType.SMALLINT.getShort(block, i);
                if (value < 0 || value > max) {
                    throw overflow(value, Long.toString(max));
                }
            }
        }
    }

    private static final class IntegerUnsignedCheck
            extends Check
    {
        private final long max;

        IntegerUnsignedCheck(int channel, DucklakeColumnHandle column, String ducklakeType, long max)
        {
            super(channel, column, ducklakeType);
            this.max = max;
        }

        @Override
        void validate(Block block)
        {
            int positions = block.getPositionCount();
            for (int i = 0; i < positions; i++) {
                if (block.isNull(i)) {
                    continue;
                }
                int value = IntegerType.INTEGER.getInt(block, i);
                if (value < 0 || value > max) {
                    throw overflow(value, Long.toString(max));
                }
            }
        }
    }

    private static final class BigintUnsignedCheck
            extends Check
    {
        private final long max;

        BigintUnsignedCheck(int channel, DucklakeColumnHandle column, String ducklakeType, long max)
        {
            super(channel, column, ducklakeType);
            this.max = max;
        }

        @Override
        void validate(Block block)
        {
            int positions = block.getPositionCount();
            for (int i = 0; i < positions; i++) {
                if (block.isNull(i)) {
                    continue;
                }
                long value = io.trino.spi.type.BigintType.BIGINT.getLong(block, i);
                if (value < 0 || value > max) {
                    throw overflow(value, Long.toString(max));
                }
            }
        }
    }

    private static final class Uint64Check
            extends Check
    {
        // 2^64 - 1. Int128 range for a valid uint64 is: high == 0, any low. Values with
        // high != 0 are either negative (high = -1, i.e. signed interpretation < 0) or
        // exceed 2^64 - 1 (high >= 1). Checking high is a single long compare regardless
        // of whether the caller wrote a decimal > 18446744073709551615 or a negative.
        private static final String MAX_LITERAL = "18446744073709551615";

        private final DecimalType type;

        Uint64Check(int channel, DucklakeColumnHandle column)
        {
            super(channel, column, "uint64");
            this.type = (DecimalType) column.columnType();
        }

        @Override
        void validate(Block block)
        {
            int positions = block.getPositionCount();
            for (int i = 0; i < positions; i++) {
                if (block.isNull(i)) {
                    continue;
                }
                Int128 value = (Int128) type.getObject(block, i);
                if (value.getHigh() != 0) {
                    throw overflow(value.toBigInteger(), MAX_LITERAL);
                }
            }
        }
    }
}
