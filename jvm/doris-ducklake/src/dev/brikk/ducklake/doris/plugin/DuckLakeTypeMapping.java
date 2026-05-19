package dev.brikk.ducklake.doris.plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.doris.connector.api.ConnectorType;

/**
 * Maps DuckLake catalog type strings (as they appear in {@code ducklake_column.column_type})
 * to Doris {@link ConnectorType}. Mirrors the Trino-side {@code DucklakeTypeConverter} but
 * targets {@code ConnectorType} factories ({@code of}, {@code arrayOf}, {@code mapOf},
 * {@code structOf}) instead of Trino's {@code Type} hierarchy.
 *
 * <p>Unsupported / degraded mappings are marked {@code DEGRADED} in comments and mirror the
 * choices the Trino plugin makes — see {@code DucklakeTypeConverter} and the cross-engine
 * compatibility notes in {@code dev-docs/COMPARE-pg_ducklake.md}.
 */
final class DuckLakeTypeMapping {

    private static final Pattern DECIMAL_PATTERN = Pattern.compile("decimal\\((\\d+),\\s*(\\d+)\\)");

    // Microsecond precision is DuckLake's native temporal resolution.
    private static final int MICROS_SCALE = 6;
    // Bounded-length defaults — Doris ConnectorType demands a length for VARBINARY/CHAR.
    private static final int VARBINARY_DEFAULT_LEN = 65535;
    private static final int UUID_BYTE_LEN = 16;

    private DuckLakeTypeMapping() {}

    /**
     * Convert a DuckLake type string to a Doris {@link ConnectorType}.
     *
     * @throws IllegalArgumentException if the type string cannot be parsed
     */
    static ConnectorType fromDucklakeType(String ducklakeType) {
        if (ducklakeType == null) {
            throw new IllegalArgumentException("ducklakeType is null");
        }
        String normalized = ducklakeType.trim().toLowerCase();

        if (normalized.startsWith("list<") && normalized.endsWith(">")) {
            String inner = extractTypeArguments(normalized, "list").trim();
            return ConnectorType.arrayOf(fromDucklakeType(inner));
        }
        if (normalized.startsWith("struct<") && normalized.endsWith(">")) {
            String body = extractTypeArguments(normalized, "struct");
            List<String> parts = splitTopLevelCommas(body);
            List<String> names = new ArrayList<>(parts.size());
            List<ConnectorType> types = new ArrayList<>(parts.size());
            for (String field : parts) {
                int colon = field.indexOf(':');
                if (colon < 0) {
                    throw new IllegalArgumentException(
                            "Invalid struct field (missing ':'): " + field);
                }
                names.add(field.substring(0, colon).trim());
                types.add(fromDucklakeType(field.substring(colon + 1).trim()));
            }
            return ConnectorType.structOf(names, types);
        }
        if (normalized.startsWith("map<") && normalized.endsWith(">")) {
            String body = extractTypeArguments(normalized, "map");
            List<String> parts = splitTopLevelCommas(body);
            if (parts.size() != 2) {
                throw new IllegalArgumentException(
                        "Invalid map type (need 2 args, got " + parts.size() + "): " + ducklakeType);
            }
            return ConnectorType.mapOf(
                    fromDucklakeType(parts.get(0).trim()),
                    fromDucklakeType(parts.get(1).trim()));
        }

        switch (normalized) {
            case "boolean":   return ConnectorType.of("BOOLEAN");

            case "int8":      return ConnectorType.of("TINYINT");
            case "int16":     return ConnectorType.of("SMALLINT");
            case "int32":     return ConnectorType.of("INT");
            case "int64":     return ConnectorType.of("BIGINT");

            // Unsigned integers: promote to the next signed type that holds the full range.
            // uint64 needs DECIMALV3(20,0); uint128 has no numeric fit, degrade to STRING.
            case "uint8":     return ConnectorType.of("SMALLINT");
            case "uint16":    return ConnectorType.of("INT");
            case "uint32":    return ConnectorType.of("BIGINT");
            case "uint64":    return ConnectorType.of("DECIMALV3", 20, 0);

            case "int128":    return ConnectorType.of("DECIMALV3", 38, 0);
            case "uint128":   return ConnectorType.of("STRING");

            case "float32":   return ConnectorType.of("FLOAT");
            case "float64":   return ConnectorType.of("DOUBLE");

            case "date":      return ConnectorType.of("DATEV2");
            // DuckLake stores timestamps at microsecond precision.
            case "timestamp": return ConnectorType.of("DATETIMEV2", MICROS_SCALE, 0);
            case "timestamptz": return ConnectorType.of("TIMESTAMPTZV2", MICROS_SCALE, 0);
            case "timestamp_s": return ConnectorType.of("DATETIMEV2", 0, 0);
            case "timestamp_ms": return ConnectorType.of("DATETIMEV2", 3, 0);
            case "timestamp_ns": return ConnectorType.of("DATETIMEV2", 9, 0);

            // DEGRADED: Doris has no first-class TIME type — read as STRING. Iceberg's
            // type mapping returns UNSUPPORTED here; STRING is more usable for v1.
            case "time":      return ConnectorType.of("STRING");
            case "timetz":    return ConnectorType.of("STRING");

            case "varchar":   return ConnectorType.of("STRING");
            case "blob":      return ConnectorType.of("VARBINARY", VARBINARY_DEFAULT_LEN, 0);
            case "uuid":      return ConnectorType.of("VARBINARY", UUID_BYTE_LEN, 0);

            // DEGRADED placeholders — mirror Trino plugin's choices. No JSON/variant/interval
            // first-class support on the Doris side yet; data is round-trippable as text.
            case "json":
            case "variant":
            case "interval":
                return ConnectorType.of("STRING");

            // DEGRADED: geometry types stored as raw bytes, no spatial functions.
            case "geometry":
            case "point":
            case "linestring":
            case "linestring_z":
            case "linestring z":
            case "polygon":
            case "multipoint":
            case "multilinestring":
            case "multipolygon":
            case "geometrycollection":
                return ConnectorType.of("VARBINARY", VARBINARY_DEFAULT_LEN, 0);

            default: {
                Matcher decimal = DECIMAL_PATTERN.matcher(normalized);
                if (decimal.matches()) {
                    int precision = Integer.parseInt(decimal.group(1));
                    int scale = Integer.parseInt(decimal.group(2));
                    return ConnectorType.of("DECIMALV3", precision, scale);
                }
                throw new IllegalArgumentException("Unsupported DuckLake type: " + ducklakeType);
            }
        }
    }

    private static String extractTypeArguments(String typeString, String prefix) {
        return typeString.substring(prefix.length() + 1, typeString.length() - 1);
    }

    private static List<String> splitTopLevelCommas(String input) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '<') {
                depth++;
            } else if (c == '>') {
                depth--;
            } else if (c == ',' && depth == 0) {
                parts.add(input.substring(start, i).trim());
                start = i + 1;
            }
        }
        parts.add(input.substring(start).trim());
        return parts;
    }
}
