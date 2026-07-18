#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["trino==0.338.0"]
# ///
"""Prepare and benchmark equivalent Parquet and DuckDB TPC-H tables in Trino."""

from __future__ import annotations

import argparse
import contextlib
import datetime
import decimal
import hashlib
import json
import math
import os
import re
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from trino.dbapi import connect  # pyright: ignore[reportMissingImports]


TPCH_TABLES = (
    "region",
    "nation",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
)

QUERY_NAMES = ("q1", "q6", "q3", "q5")
VARIANT_NAMES = ("parquet", "duckdb_embedded", "duckdb_quack")


@dataclass(frozen=True)
class Variant:
    name: str
    catalog: str
    table_prefix: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--server",
        default=os.environ.get("TRINO_SERVER", "http://localhost:9080"),
        help="Trino coordinator URL",
    )
    parser.add_argument("--user", default="format-benchmark")
    parser.add_argument(
        "--source-schema",
        default="sf10",
        help="Built-in tpch schema used as the deterministic CTAS source",
    )
    parser.add_argument(
        "--schema",
        help="Destination DuckLake schema (default: format_bench_<source-schema>)",
    )
    parser.add_argument("--catalog", default="ducklake")
    parser.add_argument("--quack-catalog", default="quack")
    parser.add_argument(
        "--duckdb-read-mode",
        choices=("httpfs", "materialize", "auto"),
        default="httpfs",
    )
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--queries", nargs="+", choices=QUERY_NAMES, default=list(QUERY_NAMES))
    parser.add_argument("--variants", nargs="+", choices=VARIANT_NAMES, default=list(VARIANT_NAMES))
    parser.add_argument("--prepare", action="store_true", help="Create missing format tables")
    parser.add_argument("--benchmark", action="store_true", help="Run timed queries")
    parser.add_argument("--reset", action="store_true", help="Drop benchmark tables before preparation")
    parser.add_argument("--output", type=Path, help="Write detailed JSON results to this path")
    args = parser.parse_args()
    if not args.prepare and not args.benchmark:
        args.prepare = True
        args.benchmark = True
    if args.rounds < 1:
        parser.error("--rounds must be at least 1")
    if args.benchmark and "parquet" not in args.variants:
        parser.error("--benchmark requires the parquet correctness baseline")
    args.schema = args.schema or f"format_bench_{args.source_schema}"
    for value in (args.source_schema, args.schema, args.catalog, args.quack_catalog):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            parser.error(f"Unsafe SQL identifier: {value!r}")
    return args


class Runner:
    def __init__(self, server: str, user: str) -> None:
        parsed = urlparse(server)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError(f"Invalid Trino server URL: {server}")
        self.host = parsed.hostname
        self.port = parsed.port or (443 if parsed.scheme == "https" else 80)
        self.http_scheme = parsed.scheme
        self.user = user

    def execute(
        self,
        sql: str,
        *,
        catalog: str | None = None,
        schema: str | None = None,
        session_properties: dict[str, str] | None = None,
    ) -> tuple[list[list[Any]], dict[str, Any], int]:
        connection = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=catalog,
            schema=schema,
            http_scheme=self.http_scheme,
            session_properties=session_properties,
        )
        cursor = connection.cursor()
        started = time.perf_counter_ns()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            client_elapsed_ms = (time.perf_counter_ns() - started) // 1_000_000
            return rows, dict(cursor.stats), client_elapsed_ms
        finally:
            # Preserve the query/connection failure if cancellation also fails.
            with contextlib.suppress(Exception):
                cursor.close()
            with contextlib.suppress(Exception):
                connection.close()


def q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def destination(catalog: str, schema: str, prefix: str, table: str) -> str:
    return f"{q(catalog)}.{q(schema)}.{q(prefix + table)}"


def source(source_schema: str, table: str) -> str:
    return f"tpch.{q(source_schema)}.{q(table)}"


def table_map(variant: Variant, schema: str) -> dict[str, str]:
    return {
        table: destination(variant.catalog, schema, variant.table_prefix, table)
        for table in TPCH_TABLES
    }


def queries(tables: dict[str, str]) -> dict[str, str]:
    return {
        "q1": f"""
            SELECT returnflag, linestatus,
                   sum(quantity) AS sum_qty,
                   sum(extendedprice) AS sum_base_price,
                   sum(extendedprice * (1 - discount)) AS sum_disc_price,
                   sum(extendedprice * (1 - discount) * (1 + tax)) AS sum_charge,
                   avg(quantity) AS avg_qty,
                   avg(extendedprice) AS avg_price,
                   avg(discount) AS avg_disc,
                   count(*) AS count_order
            FROM {tables['lineitem']}
            WHERE shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
            GROUP BY returnflag, linestatus
            ORDER BY returnflag, linestatus
        """,
        "q6": f"""
            SELECT sum(extendedprice * discount) AS revenue
            FROM {tables['lineitem']}
            WHERE shipdate >= DATE '1994-01-01'
              AND shipdate < DATE '1995-01-01'
              AND discount BETWEEN 0.05 AND 0.07
              AND quantity < 24
        """,
        "q3": f"""
            SELECT l.orderkey,
                   sum(l.extendedprice * (1 - l.discount)) AS revenue,
                   o.orderdate,
                   o.shippriority
            FROM {tables['customer']} c
            JOIN {tables['orders']} o ON c.custkey = o.custkey
            JOIN {tables['lineitem']} l ON l.orderkey = o.orderkey
            WHERE c.mktsegment = 'BUILDING'
              AND o.orderdate < DATE '1995-03-15'
              AND l.shipdate > DATE '1995-03-15'
            GROUP BY l.orderkey, o.orderdate, o.shippriority
            ORDER BY revenue DESC, o.orderdate
            LIMIT 10
        """,
        "q5": f"""
            SELECT n.name,
                   sum(l.extendedprice * (1 - l.discount)) AS revenue
            FROM {tables['customer']} c
            JOIN {tables['orders']} o ON c.custkey = o.custkey
            JOIN {tables['lineitem']} l ON l.orderkey = o.orderkey
            JOIN {tables['supplier']} s
              ON l.suppkey = s.suppkey AND c.nationkey = s.nationkey
            JOIN {tables['nation']} n ON s.nationkey = n.nationkey
            JOIN {tables['region']} r ON n.regionkey = r.regionkey
            WHERE r.name = 'ASIA'
              AND o.orderdate >= DATE '1994-01-01'
              AND o.orderdate < DATE '1995-01-01'
            GROUP BY n.name
            ORDER BY revenue DESC
        """,
    }


def prepare(
    runner: Runner,
    catalog: str,
    schema: str,
    source_schema: str,
    reset: bool,
) -> list[dict[str, Any]]:
    runner.execute(f"CREATE SCHEMA IF NOT EXISTS {q(catalog)}.{q(schema)}")
    formats = (("parquet", "parquet_"), ("duckdb", "duckdb_"))
    if reset:
        for _, prefix in reversed(formats):
            for table in reversed(TPCH_TABLES):
                runner.execute(f"DROP TABLE IF EXISTS {destination(catalog, schema, prefix, table)}")

    rows, _, _ = runner.execute(f"SHOW TABLES FROM {q(catalog)}.{q(schema)}")
    existing = {row[0] for row in rows}
    preparation: list[dict[str, Any]] = []
    for file_format, prefix in formats:
        for table in TPCH_TABLES:
            table_name = prefix + table
            if table_name in existing:
                print(f"SKIP existing {catalog}.{schema}.{table_name}", flush=True)
                continue
            sql = f"""
                CREATE TABLE {destination(catalog, schema, prefix, table)}
                WITH (data_file_format = '{file_format}')
                AS SELECT * FROM {source(source_schema, table)}
            """
            print(f"CREATE {file_format:7} {table:8} from tpch.{source_schema}", flush=True)
            result_rows, stats, client_ms = runner.execute(sql)
            output_rows = int(result_rows[0][0]) if result_rows else 0
            record = compact_stats(stats, client_ms)
            record.update(
                {
                    "format": file_format,
                    "table": table,
                    "outputRows": output_rows,
                }
            )
            preparation.append(record)
            print_stats(record)
    return preparation


def collect_storage_stats(
    runner: Runner,
    catalog: str,
    schema: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for file_format, prefix in (("parquet", "parquet_"), ("duckdb", "duckdb_")):
        for table in TPCH_TABLES:
            files_table = destination(catalog, schema, prefix, table)[:-1] + "$files\""
            sql = f"""
                SELECT count(*) AS file_count,
                       coalesce(sum(record_count), 0) AS record_count,
                       coalesce(sum(file_size_bytes), 0) AS file_size_bytes
                FROM {files_table}
            """
            rows, _, _ = runner.execute(sql)
            records.append(
                {
                    "format": file_format,
                    "table": table,
                    "fileCount": int(rows[0][0]),
                    "recordCount": int(rows[0][1]),
                    "fileSizeBytes": int(rows[0][2]),
                }
            )
    return records


def compact_stats(stats: dict[str, Any], client_elapsed_ms: int) -> dict[str, Any]:
    keys = (
        "queryId",
        "wallTimeMillis",
        "elapsedTimeMillis",
        "cpuTimeMillis",
        "planningTimeMillis",
        "physicalInputTimeMillis",
        "processedRows",
        "processedBytes",
        "physicalInputBytes",
        "physicalWrittenBytes",
        "internalNetworkInputBytes",
        "peakMemoryBytes",
        "spilledBytes",
        "totalSplits",
    )
    result = {key: stats.get(key, 0) for key in keys}
    result["clientElapsedMillis"] = client_elapsed_ms
    return result


def print_stats(record: dict[str, Any]) -> None:
    print(
        "  "
        f"elapsed={record.get('elapsedTimeMillis', 0) / 1000:.3f}s "
        f"cpu={record.get('cpuTimeMillis', 0) / 1000:.3f}s "
        f"input={record.get('physicalInputBytes', 0) / (1024 ** 3):.3f}GiB "
        f"peak={record.get('peakMemoryBytes', 0) / (1024 ** 3):.3f}GiB "
        f"spill={record.get('spilledBytes', 0) / (1024 ** 3):.3f}GiB "
        f"id={record.get('queryId')}",
        flush=True,
    )


def values_equal(left: Any, right: Any) -> bool:
    if isinstance(left, (float, decimal.Decimal)) and isinstance(right, (float, decimal.Decimal)):
        return math.isclose(float(left), float(right), rel_tol=1e-9, abs_tol=1e-6)
    if isinstance(left, (datetime.date, datetime.datetime, datetime.time)):
        return str(left) == str(right)
    return left == right


def results_equal(left: list[list[Any]], right: list[list[Any]]) -> bool:
    return len(left) == len(right) and all(
        len(left_row) == len(right_row)
        and all(values_equal(left_value, right_value) for left_value, right_value in zip(left_row, right_row))
        for left_row, right_row in zip(left, right)
    )


def result_digest(rows: list[list[Any]]) -> str:
    return hashlib.sha256(repr(rows).encode("utf-8")).hexdigest()[:16]


def benchmark(
    runner: Runner,
    catalog: str,
    quack_catalog: str,
    schema: str,
    read_mode: str,
    rounds: int,
    selected_queries: list[str],
    selected_variants: list[str],
) -> list[dict[str, Any]]:
    all_variants = (
        Variant("parquet", catalog, "parquet_"),
        Variant("duckdb_embedded", catalog, "duckdb_"),
        Variant("duckdb_quack", quack_catalog, "duckdb_"),
    )
    variants = tuple(variant for variant in all_variants if variant.name in selected_variants)
    sql_by_variant = {
        variant.name: queries(table_map(variant, schema))
        for variant in variants
    }
    baselines: dict[str, list[list[Any]]] = {}
    records: list[dict[str, Any]] = []

    for round_number in range(1, rounds + 1):
        # Preserve a Parquet-first cold pass, then rotate to reduce steady-state order bias.
        ordered_variants = list(variants)
        if round_number > 1:
            shift = (round_number - 1) % len(ordered_variants)
            ordered_variants = ordered_variants[shift:] + ordered_variants[:shift]
        for query_name in selected_queries:
            for variant in ordered_variants:
                sql = sql_by_variant[variant.name][query_name]
                print(f"RUN round={round_number} query={query_name} variant={variant.name}", flush=True)
                session_properties = None
                if variant.name.startswith("duckdb_"):
                    session_properties = {f"{variant.catalog}.duckdb_read_mode": read_mode}
                rows, stats, client_ms = runner.execute(
                    sql,
                    catalog=variant.catalog,
                    schema=schema,
                    session_properties=session_properties,
                )
                if variant.name == "parquet":
                    baselines.setdefault(query_name, rows)
                elif query_name in baselines and not results_equal(baselines[query_name], rows):
                    raise RuntimeError(
                        f"Result mismatch for {query_name}/{variant.name}: "
                        f"parquet={result_digest(baselines[query_name])}, "
                        f"actual={result_digest(rows)}"
                    )
                record = compact_stats(stats, client_ms)
                record.update(
                    {
                        "round": round_number,
                        "query": query_name,
                        "variant": variant.name,
                        "resultRows": len(rows),
                        "resultDigest": result_digest(rows),
                    }
                )
                records.append(record)
                print_stats(record)
    return records


def print_summary(records: list[dict[str, Any]]) -> None:
    if not records:
        return
    print("\nMEDIAN SERVER ELAPSED TIME", flush=True)
    print(f"{'query':<8} {'variant':<20} {'seconds':>10} {'cpu_s':>10} {'input_GiB':>12}")
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for record in records:
        groups.setdefault((record["query"], record["variant"]), []).append(record)
    for (query_name, variant), group in sorted(groups.items()):
        wall = statistics.median(float(row["elapsedTimeMillis"]) for row in group) / 1000
        cpu = statistics.median(float(row["cpuTimeMillis"]) for row in group) / 1000
        input_gib = statistics.median(float(row["physicalInputBytes"]) for row in group) / (1024 ** 3)
        print(f"{query_name:<8} {variant:<20} {wall:>10.3f} {cpu:>10.3f} {input_gib:>12.3f}")


def main() -> int:
    args = parse_args()
    runner = Runner(args.server, args.user)
    print(
        f"server={args.server} source=tpch.{args.source_schema} "
        f"destination={args.catalog}.{args.schema}",
        flush=True,
    )
    info_rows, _, _ = runner.execute(
        "SELECT node_id, node_version, coordinator, state FROM system.runtime.nodes"
    )
    print(f"nodes={info_rows}", flush=True)

    preparation: list[dict[str, Any]] = []
    if args.prepare:
        preparation = prepare(
            runner,
            args.catalog,
            args.schema,
            args.source_schema,
            args.reset,
        )

    storage = collect_storage_stats(runner, args.catalog, args.schema)
    for file_format in ("parquet", "duckdb"):
        selected = [row for row in storage if row["format"] == file_format]
        print(
            f"STORAGE {file_format}: files={sum(row['fileCount'] for row in selected)} "
            f"rows={sum(row['recordCount'] for row in selected)} "
            f"size={sum(row['fileSizeBytes'] for row in selected) / (1024 ** 3):.3f}GiB",
            flush=True,
        )

    benchmark_records: list[dict[str, Any]] = []
    if args.benchmark:
        benchmark_records = benchmark(
            runner,
            args.catalog,
            args.quack_catalog,
            args.schema,
            args.duckdb_read_mode,
            args.rounds,
            args.queries,
            args.variants,
        )
        print_summary(benchmark_records)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "server": args.server,
            "sourceSchema": args.source_schema,
            "destinationCatalog": args.catalog,
            "destinationSchema": args.schema,
            "duckdbReadMode": args.duckdb_read_mode,
            "variants": args.variants,
            "preparation": preparation,
            "storage": storage,
            "benchmark": benchmark_records,
        }
        args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        print(f"WROTE {args.output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
