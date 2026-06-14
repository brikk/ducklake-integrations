# Community Extensions for Function Pushdown — Summary Index

DuckDB community extensions (and one core extension — `inet`) that could
supply functions DuckDB lacks natively but Trino exposes. Cross-reference
with [RESEARCH-function-mapping.md](RESEARCH-function-mapping.md) when
wiring pushdown rules.

For the **per-extension catalog detail** — every function and aggregate
each extension publishes, with descriptions and gap callouts — see
[`archive/RESEARCH-function-community-extensions-detail.md`](archive/RESEARCH-function-community-extensions-detail.md).
That file is preserved verbatim from the original survey and should be
refreshed if a community extension publishes new functions.

## Which extension covers which Trino gap

| Trino-only operation | Extension | Function / construct |
|---|---|---|
| `sha512(varbinary)` | crypto | `crypto_hash('sha2-512', x)` |
| `hmac_md5/sha1/sha256/sha512` | crypto | `crypto_hmac(algo, key, msg)` |
| `xxhash64(varbinary)` | hashfuncs | `xxh64(x)` |
| `murmur3(varbinary)` (128-bit) | hashfuncs | `murmurhash3_x64_128(x)` / `murmurhash3_128(x)` |
| `theta_sketch_*` family | datasketches | `datasketch_theta*` (build/estimate/union/intersect/a_not_b) |
| `tdigest_agg` + quantile reads | datasketches | `datasketch_tdigest*` |
| `qdigest_agg` (closest substitute) | datasketches | `datasketch_kll*` / `datasketch_quantiles*` / `datasketch_req*` |
| `approx_most_frequent` | datasketches | `datasketch_frequent_items*` |
| `url_extract_protocol/host/port/path/query/fragment` | netquack | `extract_schema/host/port/path/query_string/fragment` |
| `url_extract_parameter(url, name)` | netquack | join through `extract_query_parameters(url)` table function |
| `soundex(char)` | splink_udfs | `soundex(s)` |
| IPADDRESS / IPPREFIX type, subnet `contains(network, address)` | **inet (core)** | `INET` type + `>>=` / `<<=` operators; `network`, `netmask`, `broadcast`, `host` helpers |
| IP validators / classifiers / arithmetic | netquack | `is_valid_ip`, `is_private_ip`, `ip_version`, `ip_to_int`, `int_to_ip`, `ipcalc(cidr)` |

## Still uncovered by any extension surveyed

Open gaps in DuckDB → Trino parity that remain candidates for a custom
DuckDB extension (Rust template: <https://github.com/duckdb/extension-template-rs>)
if we ever ship strict Trino parity:

- **Hashes**: `crc32`, `spooky_hash_v2_32/64`
- **Strings**: `word_stem`, `luhn_check`, `to_base64url`, `to_base32`,
  `split_to_map`, 3-arg `strpos`, `regexp_count`, `regexp_position`
- **Numeric**: `from_base`, `beta_cdf`, `inverse_beta_cdf`, `normal_cdf`,
  `inverse_normal_cdf`, `t_cdf`, `t_pdf`, `format_number`, `parse_data_size`
- **Date/time**: `parse_duration`, `human_readable_seconds`,
  `timezone_hour`, `timezone_minute`
- **Array**: `array_remove`, `array_union`, `array_except`, `ngrams`
  (array form), `combinations`, `shuffle`
- **Binary**: `from_big_endian_32/64`, `to_big_endian_32/64`,
  `from_ieee754_32/64`, `to_ieee754_32/64`, binary `reverse`, binary `substr`
- **Aggregates**: `listagg WITHIN GROUP`, `map_agg`, `multimap_agg`,
  `checksum`

## When integrating findings into `RESEARCH-function-mapping.md`

1. **IP address section** — replace any "DuckDB has no IP type" claim with
   a reference to the core `inet` extension and the `>>=` operator. The
   subnet `contains(network, address)` row should become ✅ pushable as
   `network >>= address` when the connector loads `inet`.
2. **Hash section** — `xxhash64` and `murmur3` (128-bit) are pushable when
   `hashfuncs` is loaded. `sha512` and the `hmac_*` family become pushable
   when `crypto` is loaded.
3. **Aggregate / sketches** — `theta_sketch_*`, `tdigest_agg`,
   `approx_most_frequent` become pushable when `datasketches` is loaded.
   State serialization is not wire-compatible with Trino's — only
   computed-value paths are safe to push.
4. **URL section** — `url_extract_*` scalars become pushable when `netquack`
   is loaded. `url_extract_parameter` requires the table-function form.
5. **String section** — `soundex` becomes pushable when `splink_udfs` is
   loaded.

## Extension availability snapshot

As of the last refresh (2026-05-28), the following extensions were
404-on-`extensions.duckdb.org` for DuckDB 1.5.3 and need to be re-probed
on subsequent runs: `crypto`, `hashfuncs`, `netquack`. Catalog availability
typically catches up within a few weeks of a DuckDB point release. The
`inet` core extension is always available.
