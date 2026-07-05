# Doris BE iceberg reader rejects position-delete files whose parquet declares Iceberg-required columns as OPTIONAL — even with zero actual nulls

**Affects**: FE built from
[apache/doris#62767](https://github.com/apache/doris/pull/62767) (the
fe-connector SPI branch). BE: `apache/doris:be-4.1.0` (arm64). Stock BE
talks to the PR-branch FE fine for everything else.

**Severity**: blocks Iceberg-compatible position-delete files written
by any parquet producer that defaults to OPTIONAL repetition_type. We
hit this with DuckLake; it generalises beyond DuckLake.

## Summary

The BE iceberg parquet reader selects its column reader based on the
Iceberg schema's nullability contract. For an Iceberg position-delete
file the spec declares both `file_path` and `pos` as `required`, so
the reader instantiates the not-nullable fast path
(`ScalarColumnReader<false, false>`). When the parquet file's
`repetition_type` says `OPTIONAL` for those columns, the reader's
NOT-NULL contract check raises:

```
[INTERNAL_ERROR]Read parquet file s3://…-delete.parquet failed,
reason = [CORRUPTION]Not nullable column has null values in parquet file
```

The error message is misleading: there are no nulls in the data, only
a mismatch between the column's parquet schema declaration (OPTIONAL)
and the consumer schema declaration (REQUIRED). Definition levels for
every row are `1` (= present-and-non-null).

## Repro

DuckLake writes position-delete parquet with `(file_path, pos)`
declared OPTIONAL. The file is otherwise Iceberg-shape-compatible
(field-ids match, column types match, every row is non-null).

1. Set up a DuckLake catalog through the
   [`ducklake-doris` plugin](https://github.com/jminard-brikk/ducklake-integrations/tree/main/jvm/doris-ducklake)
   (or any iceberg-shape table whose delete files are written with
   OPTIONAL columns).
2. Make a `*-delete.parquet` file exist for a data file in the table
   (e.g. issue a DELETE through DuckDB). Confirm via DuckDB:

   ```sql
   SELECT name, type, repetition_type
   FROM parquet_schema('s3://.../…-delete.parquet');
   -- ('file_path', 'BYTE_ARRAY', 'OPTIONAL')
   -- ('pos',       'INT64',      'OPTIONAL')
   ```
3. SELECT from the table through Doris. The plan dispatches the BE's
   iceberg reader (via `iceberg_params.delete_files` carrying the
   `*-delete.parquet` path with `content = POSITION_DELETE`,
   `file_format = FORMAT_PARQUET`).
4. Query fails with the `[CORRUPTION]` message above.

## Observed stack (BE-side)

```
0#  doris::ScalarColumnReader<false, false>::_read_values(
        unsigned long,
        doris::COW<doris::IColumn>::immutable_ptr<doris::IColumn>&,
        std::shared_ptr<doris::IDataType const>&,
        doris::FilterMap&, bool)
1#  doris::ScalarColumnReader<false, false>::read_column_d…
```

The `<false, false>` template parameters are the not-nullable / no-dict
fast path. The reader's null-handling assumption is wrong for a parquet
file that declares OPTIONAL on what the consumer expects to be REQUIRED.

## Root cause

The Iceberg position-delete file spec
([Position-delete files](https://iceberg.apache.org/spec/#position-delete-files))
defines:

- field 2147483546: `file_path: string` — **required**
- field 2147483545: `pos: long` — **required**

When the BE consumes a delete file via the iceberg dispatch, it picks
a column reader based on the Iceberg schema's REQUIRED. The chosen
reader then opens the parquet file, sees `repetition_type = OPTIONAL`,
and raises rather than falling through to the nullable code path.

This is a strictness problem, not a correctness problem. The actual
data is correct: every value is non-null (definition levels are all
`1`), the column types match, the field-ids match, the byte layout
matches what a spec-compliant Iceberg-writer would produce. Only the
parquet metadata's nullability declaration differs.

The Iceberg-Java reference reader degrades gracefully here: it reads
the file successfully when the actual data is non-null, regardless of
the parquet header's nullability declaration. We'd like the Doris BE
to take the same forgiving stance for Iceberg-spec delete-file columns.

## Why it generalises

This isn't a DuckLake-specific issue. Any parquet writer that defaults
to `OPTIONAL` repetition_type and doesn't explicitly mark
non-nullable columns as REQUIRED hits the same gate. That includes:

- DuckDB (any extension that emits parquet via DuckDB's writer)
- Spark in some configurations
- Polars / Arrow-based writers depending on settings
- Hand-written parquet writers that don't carefully match the Iceberg
  spec's REQUIRED semantics

A fix on the BE side benefits any future Iceberg-shape data source.

## Suggested fix

In the BE iceberg position-delete-file column dispatch, prefer the
nullable column reader path when the parquet schema disagrees with
the Iceberg-spec nullability. Two implementation options:

1. **Widen the not-nullable fast path's contract.** When
   `ScalarColumnReader<false, false>` opens a column page and sees
   OPTIONAL in the parquet metadata, check the definition-level stream:
   if every level is the maximum (= present), continue as if the
   column is REQUIRED. If a definition level of 0 ever shows up
   (an actual null), then surface the existing `[CORRUPTION]` error.
2. **Route Iceberg-spec'd delete-file columns through the nullable
   reader unconditionally.** Detect by field-id (2147483546 for
   `file_path`, 2147483545 for `pos`) and always pick the nullable
   path for these columns. Slightly slower in the common case but
   trivially correct.

Option 1 is the principled fix; option 2 is a smaller change
specifically for delete files. Either works for our use case.

## Why we're filing this

We're shipping a DuckLake → Doris connector at
[jvm/doris-ducklake](https://github.com/jminard-brikk/ducklake-integrations/tree/main/jvm/doris-ducklake).
The FE plumbing for position deletes is done and tested (38 unit tests
green) — wire bytes reach the BE correctly via
`iceberg_params.delete_files`. End-to-end correctness is gated on the
BE reader either (a) accepting OPTIONAL-with-no-nulls, or (b) the
DuckDB-side fix to mark these columns REQUIRED. We're filing both
upstreams in parallel; the BE-side fix has wider impact since it
benefits any Iceberg-shape writer that defaults to OPTIONAL.

## Companion report

A parallel report has been sent to the DuckLake / DuckDB team asking
for `REQUIRED` to be declared on the position-delete parquet columns:
see `REPORT-ducklake-position-delete-parquet-nullability.md` in this
repo. Both fixes are useful; the BE-side fix is more broadly
applicable.

## Contact

Filed by the DuckLake-on-Doris connector team at
`jvm/doris-ducklake/` in the
[ducklake-integrations](https://github.com/jminard-brikk/ducklake-integrations)
repo (private). Happy to help repro, test a fix, or co-author a PR.
