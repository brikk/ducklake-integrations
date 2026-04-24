# Cross-Engine Write Compatibility Issues in DuckLake

## Context

We are implementing a Trino connector for DuckLake. During cross-engine compatibility testing (Trino writes -> DuckDB reads), we encountered several issues that suggest the catalog format implicitly assumes a single-engine write model.

This report documents the issues found. We believe these are worth considering as DuckLake moves toward 1.0 and the spec is updated for independent implementations.

## Issue 1: `default_value_dialect` forces engine coupling

### Problem

`ducklake_column` has a `default_value_dialect` field. DuckDB writes `'duckdb'` here for every column, even columns with no user-defined default (where `default_value = 'NULL'` and `default_value_type = 'literal'`).

When Trino creates a table, what should it write?

- `'trino'`? Then DuckDB may not understand the default expression syntax if it ever needs to evaluate it.
- `'duckdb'`? Then Trino is claiming to speak DuckDB's dialect, which is incorrect for non-trivial defaults.
- `NULL` or empty? DuckDB crashes: `Failed to cast value: Could not convert string '' to INT32`.

For the trivial case (`DEFAULT NULL`), `'duckdb'` works because `'NULL'` is universal. But for any real default expression (e.g., `DEFAULT current_timestamp`, `DEFAULT array_length(col)`), the dialect is engine-specific and non-portable.

### Impact

A catalog written by multiple engines will have columns with mixed `default_value_dialect` values. Any engine reading the catalog must either:
1. Understand all dialects (impractical)
2. Ignore defaults from other dialects (lossy)
3. Refuse to read the table (too restrictive)

### Suggestion

Consider one of:
- A standard expression language for defaults (like Iceberg's approach of storing defaults as literal values, not expressions)
- Clarifying in the spec that `default_value_dialect` is informational and engines should not rely on evaluating defaults from other dialects
- Documenting the expected behavior when an engine encounters a dialect it doesn't understand

## Issue 2: `ducklake_column` metadata requires DuckDB-specific values

### Problem

DuckDB expects specific non-NULL values in `ducklake_column` fields that are not documented in the spec:

| Field | Spec says | DuckDB requires | Notes |
|-------|-----------|-----------------|-------|
| `initial_default` | Not documented | Must be `NULL` | Empty string `''` causes `Could not convert string '' to INT32` crash |
| `default_value` | Not documented | String `'NULL'` (not SQL NULL) | The literal string "NULL" |
| `default_value_type` | Not documented | `'literal'` | Even for columns with no user-defined default |
| `default_value_dialect` | Not documented | `'duckdb'` | See Issue 1 above |
| `column_order` | Not documented | **1-based** | 0-based causes issues |

These fields are present in the `ducklake_column` table but not documented in the spec's column table description. An independent implementation has no way to know these are required or what values to use without reverse-engineering DuckDB's output.

### Suggestion

Document these fields in the spec, including:
- Which are required vs optional
- What values are valid
- Whether `column_order` is 0-based or 1-based
- What `initial_default` means vs `default_value`

## Issue 3: `contains_nan` NULL semantics not documented

### Problem

In `ducklake_table_column_stats` and `ducklake_file_column_stats`, the `contains_nan` column must be `NULL` (not `0` or `false`) for non-floating-point columns. Writing `0` (a valid boolean false) causes DuckDB to crash with an internal assertion failure:

```
INTERNAL Error: Calling GetValueInternal on a value that is NULL
```

in `TransformGlobalStatsRow` -> `GetGlobalTableStats`.

This crash corrupts DuckDB's in-process state and causes all subsequent queries on the same connection to fail.

### Impact

An independent implementation that writes `contains_nan = false` (which is logically correct -- an INTEGER column does not contain NaN) will crash DuckDB.

### Suggestion

- Document that `contains_nan` must be `NULL` (not `0`/`false`) for non-floating-point types
- Consider making DuckDB's `GetGlobalTableStats` handle non-NULL `contains_nan` gracefully rather than crashing

## Issue 4: `ducklake_schema_versions.table_id` not in spec

### Problem

The spec defines `ducklake_schema_versions` with two columns: `begin_snapshot` and `schema_version`. DuckDB's implementation adds a third column `table_id` that records which table triggered the schema version change. This column appears necessary for DuckDB to correctly resolve column schemas for tables.

### Suggestion

Add `table_id` to the spec for `ducklake_schema_versions`.

## Issue 5: DuckDB returns zeros for column values in Trino-written Parquet files — RESOLVED

### Problem

When DuckDB reads Parquet files written by Trino through a DuckLake catalog, metadata operations work correctly (SHOW TABLES, DESCRIBE, row counts) but actual column values are returned as zeros/empty. DuckDB-created Parquet files read correctly through the same catalog.

DuckDB's DuckLake extension uses Parquet `field_id` to map columns to `ducklake_column.column_id`. Trino's standard `ParquetSchemaConverter` does not set `field_id` on the Parquet schema.

### Resolution

Implemented `DucklakeParquetSchemaBuilder` which rebuilds the Parquet `MessageType` with `field_id` annotations from DuckLake `column_id` values. Supports nested types (ROW, ARRAY, MAP) with recursive field_id assignment.

All 10 cross-engine tests now pass, including full column value round-trips.

### Original Suggestion (still relevant for spec)

- Document what Parquet file-level or column-level metadata DuckDB expects for column mapping
- Consider whether column mapping should be by name (Parquet schema) rather than by custom metadata, for engine independence

## Issue 6: Implicit single-writer assumption

### Broader observation

Several of the issues above point to an implicit assumption that a DuckLake catalog is written by a single engine (DuckDB) and read by others. The `default_value_dialect = 'duckdb'` pattern, the undocumented column metadata fields, and the `contains_nan` NULL semantics all work naturally for DuckDB but require reverse-engineering for independent implementations.

As DuckLake moves toward being an open format with multiple independent implementations, it may be worth explicitly addressing:

1. **Is multi-engine write a goal?** If yes, the dialect fields need a cross-engine solution.
2. **If single-writer is expected**, document this clearly so implementors know to focus on read interop only.
3. **If multi-writer is a future goal**, consider adding a `created_by` or `dialect` field at the snapshot level rather than the column level, so each snapshot is internally consistent.

## Environment

- DuckDB: 1.5.1.0 (JDBC)
- DuckLake extension: latest as of 2026-04-06
- Trino: development branch
- Catalog backend: PostgreSQL (via Testcontainers)
- All issues reproducible with the DuckDB JDBC driver
