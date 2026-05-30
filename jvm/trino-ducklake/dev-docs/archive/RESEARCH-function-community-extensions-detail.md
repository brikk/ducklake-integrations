# Community Extensions for Function Pushdown

DuckDB community extensions (and one core extension — `inet`) that could supply functions DuckDB lacks natively but Trino exposes. Each section lists the extension's published functions with a short description, then calls out any Trino-only operations the extension could fill in. Cross-reference with [RESEARCH-function-mapping.md](RESEARCH-function-mapping.md) when wiring pushdown rules.

**Status flag legend (Trino-relevant matches column):**
- ✅ direct functional substitute, push-safe pending semantics verification
- ⚠️ partial / requires wrapping or argument reshaping
- ❌ no usable substitute in this extension

---

## Summary index: which extension covers which Trino gap

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

**Still uncovered by any extension surveyed (open gaps in DuckDB → Trino parity):**
- Hashes: `crc32`, `spooky_hash_v2_32/64`
- Strings: `word_stem`, `luhn_check`, `to_base64url`, `to_base32`, `split_to_map`, 3-arg `strpos`, `regexp_count`, `regexp_position`
- Numeric: `from_base`, `beta_cdf`, `inverse_beta_cdf`, `normal_cdf`, `inverse_normal_cdf`, `t_cdf`, `t_pdf`, `format_number`, `parse_data_size`
- Date/time: `parse_duration`, `human_readable_seconds`, `timezone_hour`, `timezone_minute`
- Array: `array_remove`, `array_union`, `array_except`, `ngrams` (array form), `combinations`, `shuffle`
- Binary: `from_big_endian_32/64`, `to_big_endian_32/64`, `from_ieee754_32/64`, `to_ieee754_32/64`, binary `reverse`, binary `substr`
- Aggregates: `listagg WITHIN GROUP`, `map_agg`, `multimap_agg`, `checksum`

These remain candidates for a custom DuckDB extension (Rust template: https://github.com/duckdb/extension-template-rs) if/when we decide to ship Trino parity.

---

## Bitfilters
https://duckdb.org/community_extensions/extensions/bitfilters

Probabilistic membership-test data structures (bloom, xor, fuse, quotient filters).

### Aggregates
| Function | Description |
|---|---|
| `binary_fuse16_filter(UBIGINT) -> BLOB` | Build a Binary Fuse 16-bit filter (~0.0015% FP rate). |
| `binary_fuse8_filter(UBIGINT) -> BLOB` | Build a Binary Fuse 8-bit filter (~0.4% FP rate). |
| `bitfilters_duckdb_bloom_filter_create(VARCHAR, INTEGER, UBIGINT) -> BLOB` | Build a DuckDB-compatible bloom filter from pre-hashed values. |
| `quotient_filter(INTEGER, INTEGER, ANY) -> BLOB` | Build or merge Quotient filters. |
| `xor16_filter(UBIGINT) -> BLOB` | Build an Xor16 filter (16-bit fingerprints). |
| `xor8_filter(UBIGINT) -> BLOB` | Build an Xor8 filter (8-bit fingerprints). |

### Scalars
| Function | Description |
|---|---|
| `binary_fuse16_filter_contains(BLOB, UBIGINT) -> BOOLEAN` | Membership test against a Binary Fuse 16 filter. |
| `binary_fuse8_filter_contains(BLOB, UBIGINT) -> BOOLEAN` | Membership test against a Binary Fuse 8 filter. |
| `bitfilters_duckdb_bloom_filter_probe(VARCHAR, BLOB, ANY, ...) -> BOOLEAN` | Probe a DuckDB-compatible bloom filter for one or more values. |
| `bitfilters_duckdb_hash(VARCHAR, ANY, ...) -> UBIGINT` | Compute the DuckDB-internal hash value matching the built-in for a given version. |
| `quotient_filter_contains(BLOB, ANY) -> BOOLEAN` | Membership test against a Quotient filter. |
| `xor16_filter_contains(BLOB, UBIGINT) -> BOOLEAN` | Membership test against an Xor16 filter. |
| `xor8_filter_contains(BLOB, UBIGINT) -> BOOLEAN` | Membership test against an Xor8 filter. |

### Trino-relevant matches
❌ None. Trino exposes no equivalent SQL-surface bloom/xor/fuse primitives, and `bitfilters_duckdb_hash` is the DuckDB-internal hash (UBIGINT), not a cryptographic or named non-cryptographic hash.

---

## Clamp
https://duckdb.org/community_extensions/extensions/clamp

Numeric range / wrap utilities.

| Function | Description |
|---|---|
| `clamp(value, min, max)` | Clamp a numeric value into `[min, max]`. |
| `clip(value, min, max)` | Alias for `clamp`. |
| `saturate(value)` | Clamp to `[0.0, 1.0]`. |
| `clamp01(value)` | Alias for `saturate`. |
| `wrap(value, min, max)` | Modular wrap into `[min, max)`. |
| `pingpong(value, min, max)` | Oscillate back and forth between `min` and `max`. |
| `fract(value)` | Fractional part of a number. |

### Trino-relevant matches
❌ None on the target list. (Trino also has no native `clamp`; both engines would gain symmetry from this extension, but it's not closing a Trino-only gap.)

---

## Crypto
https://duckdb.org/community_extensions/extensions/crypto

Cryptographic hashes, HMACs, and CSPRNG (OpenSSL-backed).

### Scalars
| Function | Description |
|---|---|
| `crypto_hash(algorithm VARCHAR, value) -> VARCHAR` | Compute a cryptographic hash. Supports `blake3`, `sha2-256`, `sha2-512`, `sha3-256`, `sha3-512`, `md5`, `sha1`. |
| `crypto_hmac(algorithm VARCHAR, key, message) -> VARCHAR` | Compute HMAC for all the above algorithms except `blake3`. |
| `crypto_random_bytes(length) -> BLOB` | CSPRNG bytes via `RAND_bytes()`. |

### Aggregates
| Function | Description |
|---|---|
| `crypto_hash_agg(algorithm VARCHAR, column ORDER BY ...) -> VARCHAR` | Aggregate hash over rows. Requires `ORDER BY` for determinism. |

### Trino-relevant matches
- ✅ Trino `sha512(x)` → `crypto_hash('sha2-512', x)` (output is hex VARCHAR; cast if VARBINARY required).
- ✅ Trino `hmac_md5(k, m)` → `crypto_hmac('md5', k, m)`.
- ✅ Trino `hmac_sha1(k, m)` → `crypto_hmac('sha1', k, m)`.
- ✅ Trino `hmac_sha256(k, m)` → `crypto_hmac('sha2-256', k, m)`.
- ✅ Trino `hmac_sha512(k, m)` → `crypto_hmac('sha2-512', k, m)`.
- ❌ No `crc32`, `xxhash64`, `spooky_hash_v2_*`, or `murmur3` (those are non-cryptographic; see **Hashfuncs**).

---

## DataSketches
https://duckdb.org/community_extensions/extensions/datasketches

Apache DataSketches port: CPC, HLL, KLL, Quantiles, REQ, T-Digest, Theta, Frequent Items.

### CPC (compressed probabilistic counting)
| Function | Description |
|---|---|
| `datasketch_cpc(k, data)` agg | Build a CPC sketch. |
| `datasketch_cpc_union(k, sketch)` agg | Merge CPC sketches. |
| `datasketch_cpc_estimate(sketch)` | Estimated distinct count. |
| `datasketch_cpc_describe(sketch)` | String representation. |
| `datasketch_cpc_is_empty(sketch)` | Empty test. |
| `datasketch_cpc_lower_bound(sketch, std_dev)` / `datasketch_cpc_upper_bound(sketch, std_dev)` | Confidence bounds. |

### HyperLogLog
| Function | Description |
|---|---|
| `datasketch_hll(k, data)` agg | Build HLL sketch. |
| `datasketch_hll_union(k, sketch)` agg | Merge HLL sketches. |
| `datasketch_hll_estimate(sketch)` | Estimated distinct count. |
| `datasketch_hll_describe(sketch, include_summary, include_detail)` | Describe sketch. |
| `datasketch_hll_is_empty(sketch)` / `datasketch_hll_is_compact(sketch)` | State checks. |
| `datasketch_hll_lg_config_k(sketch)` | log2 K value. |
| `datasketch_hll_lower_bound(sketch, std_dev)` / `datasketch_hll_upper_bound(sketch, std_dev)` | Confidence bounds. |

### KLL quantile sketch
| Function | Description |
|---|---|
| `datasketch_kll(k, data)` / `datasketch_kll(k, sketch)` agg | Build / merge KLL sketch. |
| `datasketch_kll_quantile(sketch, rank, inclusive)` | Quantile at rank. |
| `datasketch_kll_rank(sketch, item, inclusive)` | Rank of item. |
| `datasketch_kll_cdf(sketch, points, inclusive)` | CDF. |
| `datasketch_kll_pmf(sketch, points, inclusive)` | PMF. |
| `datasketch_kll_describe(sketch, include_levels, include_items)` | Describe sketch. |
| `datasketch_kll_is_empty(sketch)` / `datasketch_kll_is_estimation_mode(sketch)` | State checks. |
| `datasketch_kll_k(sketch)` | K value. |
| `datasketch_kll_min_item(sketch)` / `datasketch_kll_max_item(sketch)` | Extremes. |
| `datasketch_kll_n(sketch)` | Total items processed. |
| `datasketch_kll_num_retained(sketch)` | Retained count. |
| `datasketch_kll_normalized_rank_error(sketch, is_pmf)` | Error metric. |

### Quantiles sketch
| Function | Description |
|---|---|
| `datasketch_quantiles(k, data)` / `datasketch_quantiles(k, sketch)` agg | Build / merge. |
| `datasketch_quantiles_quantile(sketch, rank, inclusive)` | Quantile. |
| `datasketch_quantiles_rank(sketch, item, inclusive)` | Rank. |
| `datasketch_quantiles_cdf(sketch, points, inclusive)` | CDF. |
| `datasketch_quantiles_pmf(sketch, points, inclusive)` | PMF. |
| `datasketch_quantiles_describe(sketch, include_levels, include_items)` | Describe. |
| `datasketch_quantiles_is_empty(sketch)` / `datasketch_quantiles_is_estimation_mode(sketch)` | State. |
| `datasketch_quantiles_k(sketch)` | K value. |
| `datasketch_quantiles_min_item(sketch)` / `datasketch_quantiles_max_item(sketch)` | Extremes. |
| `datasketch_quantiles_n(sketch)` | Item count. |
| `datasketch_quantiles_num_retained(sketch)` | Retained count. |
| `datasketch_quantiles_normalized_rank_error(sketch, is_pmf)` | Error metric. |

### REQ sketch
| Function | Description |
|---|---|
| `datasketch_req(k, data)` / `datasketch_req(k, sketch)` agg | Build / merge. |
| `datasketch_req_quantile(sketch, rank, inclusive)` | Quantile. |
| `datasketch_req_rank(sketch, item, inclusive)` | Rank. |
| `datasketch_req_cdf(sketch, points, inclusive)` | CDF. |
| `datasketch_req_pmf(sketch, points, inclusive)` | PMF. |
| `datasketch_req_describe(sketch, include_levels, include_items)` | Describe. |
| `datasketch_req_is_empty(sketch)` / `datasketch_req_is_estimation_mode(sketch)` | State. |
| `datasketch_req_k(sketch)` | K value. |
| `datasketch_req_min_item(sketch)` / `datasketch_req_max_item(sketch)` | Extremes. |
| `datasketch_req_n(sketch)` | Item count. |
| `datasketch_req_num_retained(sketch)` | Retained count. |

### T-Digest
| Function | Description |
|---|---|
| `datasketch_tdigest(k, data)` / `datasketch_tdigest(k, sketch)` agg | Build / merge. |
| `datasketch_tdigest_quantile(sketch, rank)` | Quantile. |
| `datasketch_tdigest_rank(sketch, item)` | Rank. |
| `datasketch_tdigest_cdf(sketch, points)` | CDF. |
| `datasketch_tdigest_pmf(sketch, points)` | PMF. |
| `datasketch_tdigest_describe(sketch, include_centroids)` | Describe. |
| `datasketch_tdigest_is_empty(sketch)` | Empty test. |
| `datasketch_tdigest_k(sketch)` | K value. |
| `datasketch_tdigest_total_weight(sketch)` | Sum of item weights. |

### Theta sketch (set operations)
| Function | Description |
|---|---|
| `datasketch_theta(column)` / `datasketch_theta(k, column)` / `datasketch_theta(sketch)` / `datasketch_theta(k, sketch)` agg | Build / merge. |
| `datasketch_theta_estimate(sketch)` | Distinct count. |
| `datasketch_theta_union(a, b)` | Set union. |
| `datasketch_theta_intersect(a, b)` | Set intersection. |
| `datasketch_theta_a_not_b(a, b)` | Set difference. |
| `datasketch_theta_describe(sketch)` | Describe. |
| `datasketch_theta_is_empty(sketch)` / `datasketch_theta_is_estimation_mode(sketch)` | State. |
| `datasketch_theta_num_retained(sketch)` | Retained count. |
| `datasketch_theta_get_theta(sketch)` | Sampling probability. |
| `datasketch_theta_get_seed(sketch)` | Seed hash. |
| `datasketch_theta_lower_bound(sketch, std_dev)` / `datasketch_theta_upper_bound(sketch, std_dev)` | Confidence bounds. |

### Frequent Items
| Function | Description |
|---|---|
| `datasketch_frequent_items(column)` / `datasketch_frequent_items(lg_max_k, column)` / `datasketch_frequent_items(sketch)` / `datasketch_frequent_items(lg_max_k, sketch)` agg | Build / merge. |
| `datasketch_frequent_items_estimate(sketch, item)` | Estimated frequency. |
| `datasketch_frequent_items_lower_bound(sketch, item)` / `datasketch_frequent_items_upper_bound(sketch, item)` | Frequency bounds. |
| `datasketch_frequent_items_get_frequent(sketch, error_type)` | List frequent items with estimates and bounds. |
| `datasketch_frequent_items_epsilon(sketch)` | Relative error. |
| `datasketch_frequent_items_is_empty(sketch)` | Empty test. |
| `datasketch_frequent_items_num_active(sketch)` | Tracked-item count. |
| `datasketch_frequent_items_total_weight(sketch)` | Sum of counts. |

### Trino-relevant matches
- ✅ `theta_sketch_*` → `datasketch_theta*` family. State serialization differs from Trino's (not interchangeable on the wire), but every set/union/intersect/cardinality operation has a direct counterpart.
- ✅ `tdigest_agg` (+ `value_at_quantile`, `quantile_at_value`) → `datasketch_tdigest` aggregate plus `datasketch_tdigest_quantile` / `_cdf` / `_rank`.
- ⚠️ `qdigest_agg` — no exact qdigest port; KLL is the modern replacement Apache recommends (`datasketch_kll*`) or use `datasketch_quantiles*` / `datasketch_req*`.
- ✅ `approx_most_frequent(buckets, value, capacity)` → `datasketch_frequent_items(lg_max_k, column)` + `datasketch_frequent_items_get_frequent(sketch, error_type)`.
- ❌ No `listagg WITHIN GROUP`, `map_agg`, `multimap_agg`, `checksum`.

---

## Decimal Arithmetic
https://duckdb.org/community_extensions/extensions/decimal_arithmetic

High-precision decimal helpers.

| Function | Description |
|---|---|
| `decimal_avg(...)` agg | Average over decimal values without intermediate downcast. |
| `decimal_div(...)` | Division on decimal types preserving precision. |
| `round_ceil(...)` | Round decimal upward to integer. |
| `round_down(...)` | Round toward zero (down) to integer. |
| `round_floor(...)` | Floor to integer. |
| `round_up(...)` | Round away from zero (up) to integer. |

Upstream docs publish names without parameter signatures.

### Trino-relevant matches
❌ None. Trino's `ceiling`, `floor`, `round` are native; decimal-aware variants here don't fill a Trino gap.

---

## faiss
https://duckdb.org/community_extensions/extensions/faiss

Vector similarity search via Facebook AI Similarity Search (FAISS).

### Table functions
| Function | Description |
|---|---|
| `faiss_create` | Initialize a new FAISS index with dimensionality and index type. |
| `faiss_add` | Insert vectors into an existing index. |
| `faiss_load` | Load an index from storage. |
| `faiss_save` | Persist an index to disk. |
| `faiss_destroy` | Remove an index from memory. |
| `faiss_manual_train` | Manually train an index. |
| `faiss_to_gpu` | Move a supported index to GPU. |
| `faiss_create_params` | Generate parameter configurations for index creation. |
| `__faiss_create_mask` | Internal mask creation for filtering. |

### Scalar functions
| Function | Description |
|---|---|
| `faiss_search` | k-nearest-neighbour search. |
| `faiss_search_filter` | Filtered vector search with constraint expressions. |
| `faiss_search_filter_set` | Vector search with multiple filter conditions. |

### Trino-relevant matches
❌ None. Pure vector ANN; no overlap with the Trino gap list.

---

## Fuzzy Complete
https://duckdb.org/community_extensions/extensions/fuzzycomplete

Fuzzy-matching autocompletion. Per the community-extensions page it registers **no SQL functions, table functions, pragmas, or types** for queries — it is a client/IDE-level feature only.

### Trino-relevant matches
❌ None — no SQL surface to push to.

---

## Geosilo
https://duckdb.org/community_extensions/extensions/geosilo

Compact geometry encoding and lightweight accessors.

### Scalars
| Function | Description |
|---|---|
| `ST_Area`, `ST_Length`, `ST_Perimeter`, `ST_NPoints` | Standard geometry measures. |
| `ST_GeometryType` | Geometry type. |
| `ST_IsEmpty` | Empty test. |
| `ST_X` / `ST_Y` | Coordinate extraction. |
| `ST_XMax` / `ST_XMin` / `ST_YMax` / `ST_YMin` | Bounding-box accessors. |
| `geosilo_encode` | Encode geometry using delta-encoded coordinate compression. |
| `geosilo_decode` | Decode delta-encoded geometry data. |
| `geosilo_metadata` | Metadata about encoded geometries. |

### Types
| Type | Description |
|---|---|
| `GEOSILO` | Compact geometry encoding (~3-4× smaller than WKB). |

### Trino-relevant matches
❌ None on the current target list. Could be considered later if/when Trino ST_* parity becomes a goal.

---

## Hashfuncs
https://duckdb.org/community_extensions/extensions/hashfuncs

Non-cryptographic hash families (MurmurHash3, xxHash variants, RapidHash).

| Function | Description |
|---|---|
| `murmurhash3_32(input[, seed])` | 32-bit MurmurHash3. |
| `murmurhash3_128(input[, seed])` | 128-bit MurmurHash3 (x86 variant). |
| `murmurhash3_x64_128(input[, seed])` | 128-bit MurmurHash3 (x64 variant). |
| `xxh32(input[, seed])` | 32-bit xxHash (XXH32). |
| `xxh64(input[, seed])` | 64-bit xxHash (XXH64). |
| `xxh3_64(input[, seed])` | 64-bit xxHash3 (faster for short inputs). |
| `xxh3_128(input[, seed])` | 128-bit xxHash3. |
| `xxh3_128_hex(input[, seed])` | 128-bit xxHash3 as a 32-char lowercase hex string. |
| `rapidhash(input[, seed])` | 64-bit RapidHash (very fast for any input size). |

### Trino-relevant matches
- ✅ Trino `xxhash64(varbinary)` → `xxh64(x)`. Direct algorithm match.
- ✅ Trino `murmur3(varbinary)` (128-bit) → `murmurhash3_x64_128(x)` (or `murmurhash3_128(x)`).
- ❌ No `crc32`, `spooky_hash_v2_*`, no SHA family, no HMAC.

---

## JSONata
https://duckdb.org/community_extensions/extensions/jsonata

JSONata expression language for JSON querying/transformation inside SQL.

| Function | Description |
|---|---|
| `jsonata(...)` | Run a JSONata expression against JSON input; supports query and transformation. Detailed signatures live in query.farm's docs. |

### Trino-relevant matches
❌ None directly. In principle a JSONata program could emulate simple array/string ops, but there is no first-class substitute for any specific Trino-only function.

---

## lastra
https://duckdb.org/community_extensions/extensions/lastra

Reader for the Lastra columnar time-series format.

| Function | Description |
|---|---|
| `read_lastra(filename)` table | Reads Lastra columnar time-series files with per-column codec selection and row-group timestamp statistics. |

### Trino-relevant matches
❌ None. File-format reader.

---

## Lindel
https://duckdb.org/community_extensions/extensions/lindel

Space-filling curve linearization / delinearization (Z-order, Hilbert, Morton).

| Function | Description |
|---|---|
| `hilbert_encode(array)` | Encode an array of ints/floats into a single UBIGINT via Hilbert mapping. |
| `hilbert_decode(UBIGINT, ndim, is_float, is_signed)` | Inverse of `hilbert_encode`. |
| `morton_encode(array)` | Encode an array of ints/floats into a UBIGINT via Morton (Z-order). |
| `morton_decode(UBIGINT, ndim, is_float, is_signed)` | Inverse of `morton_encode`. |

### Trino-relevant matches
❌ None.

---

## LSH
https://duckdb.org/community_extensions/extensions/lsh

Locality-sensitive hashing for approximate similarity / nearest-neighbour search.

| Function | Description |
|---|---|
| `lsh_min(string-or-shingles, ...)` | 64-bit band hashes from MinHash signatures. |
| `lsh_min32(string-or-shingles, ...)` | 32-bit MinHash band hashes. |
| `lsh_euclidean(points, ...)` | 64-bit band hashes from Euclidean LSH signatures. |
| `lsh_euclidean32(points, ...)` | 32-bit Euclidean LSH band hashes. |
| `lsh_jaccard(s1, s2, ngram)` | Estimate Jaccard similarity with an explicit n-gram parameter. |

### Trino-relevant matches
❌ None. LSH band hashes are not interchangeable with content-hash families.

---

## Marisa
https://duckdb.org/community_extensions/extensions/marisa

Static, space-efficient trie for fast string lookups, prefix search, and predictive text.

| Function | Description |
|---|---|
| `marisa_trie(column)` agg | Build a Marisa trie from a column of strings. |
| `marisa_lookup(trie, key)` | Exact lookup against a trie. |
| `marisa_predictive(trie, prefix)` | Predictive (prefix-completion) matches. |
| `marisa_common_prefix(trie, key)` | Common-prefix matches. |

### Trino-relevant matches
❌ None.

---

## Markdown
https://duckdb.org/community_extensions/extensions/markdown

Markdown parsing, extraction, and rendering.

### Table functions
| Function | Description |
|---|---|
| `read_markdown(path-or-glob)` | Read Markdown files with glob support and metadata extraction. |
| `read_markdown_blocks(path-or-glob)` | Parse files into block-level elements (`duck_block`). |
| `read_markdown_sections(path-or-glob, ...)` | Parse files into a hierarchical section tree. |

### Scalars
| Function | Description |
|---|---|
| `duck_block_to_md(block)` | Convert a single block / inline element to Markdown. |
| `duck_blocks_to_md(list)` | Convert a list of elements into a complete Markdown document. |
| `duck_blocks_to_sections(list)` | Reorganise blocks into hierarchical sections. |
| `md_extract_code_blocks(md)` | Extract code blocks with language identification. |
| `md_extract_images(md)` | Extract images with alt text and metadata. |
| `md_extract_links(md)` | Extract hyperlinks (text, URL, title). |
| `md_extract_metadata(md)` | Parse frontmatter metadata into a MAP. |
| `md_extract_section(md, ...)` / `md_extract_sections(md, ...)` | Retrieve one or many sections. |
| `md_extract_table_rows(md)` | Markdown table rows as structured data. |
| `md_extract_tables_json(md)` | Tables as structured JSON. |
| `md_section_breadcrumb(md, ...)` | Navigation breadcrumbs for sections. |
| `md_stats(md)` | Word count, reading time, etc. |
| `md_to_html(md)` | Convert Markdown to HTML. |
| `md_to_text(md)` | Convert Markdown to plain text. |
| `md_valid(md)` | Validate Markdown syntax/structure. |
| `value_to_md(value)` | Convert an arbitrary value to its Markdown representation. |

### Trino-relevant matches
❌ None. Document-processing only.

---

## NetQuack
https://duckdb.org/community_extensions/extensions/netquack

Domain / URI / IP / web-path parsing and analysis.

### URI / domain scalars
| Function | Description |
|---|---|
| `extract_domain(url)` | Main domain from a URL. |
| `extract_host(url)` | Hostname from a URL. |
| `extract_path(url)` | Path component from a URL. |
| `extract_query_string(url)` | Query string from a URL. |
| `extract_schema(url)` | Schema/protocol from a URL. |
| `extract_subdomain(url)` | Subdomain from a URL. |
| `extract_tld(url)` | Top-level domain. |
| `extract_port(url)` | Port number from a URL. |
| `extract_extension(url)` | File extension from a URL. |
| `extract_fragment(url)` | Fragment (after `#`). |
| `normalize_url(url)` | RFC 3986 normalization. |
| `domain_depth(domain)` | Number of dot-separated levels. |
| `is_valid_url(url)` / `is_valid_domain(domain)` | Validators. |

### IP scalars
| Function | Description |
|---|---|
| `is_valid_ip(addr)` | Validate IPv4 / IPv6. |
| `is_private_ip(addr)` | Test if address is in a private / reserved range. |
| `ip_version(addr)` | Returns 4 / 6 / NULL. |
| `ip_to_int(addr)` | IPv4 → 32-bit unsigned integer. |
| `int_to_ip(n)` | Integer → IPv4 notation. |

### Other scalars
| Function | Description |
|---|---|
| `get_tranco_rank(domain)` / `get_tranco_rank_category(domain)` | Tranco ranking. |
| `base64_encode(s)` / `base64_decode(s)` | Standard Base64 codec. |
| `update_tranco()` | Refresh Tranco data. |

### Table functions
| Function | Description |
|---|---|
| `extract_query_parameters(url)` | URL query parameters as rows. |
| `ipcalc(cidr)` | IP/network info from CIDR. |
| `extract_path_segments(url)` | Path segments as rows. |
| `netquack_version()` | Extension version. |

### Trino-relevant matches
- ✅ Trino `url_extract_protocol` → `extract_schema`.
- ✅ Trino `url_extract_host` → `extract_host`.
- ✅ Trino `url_extract_port` → `extract_port`.
- ✅ Trino `url_extract_path` → `extract_path`.
- ✅ Trino `url_extract_query` → `extract_query_string`.
- ✅ Trino `url_extract_fragment` → `extract_fragment`.
- ⚠️ Trino `url_extract_parameter(url, name)` → join through `extract_query_parameters(url)` table function (not a scalar; pushdown shape changes).
- ⚠️ IP helpers (`is_valid_ip`, `is_private_ip`, `ip_version`, `ipcalc`) operate on VARCHAR, useful alongside the core `inet` type but not a substitute for it.

---

## Splink UDFs
https://duckdb.org/community_extensions/extensions/splink_udfs

Phonetic, text-normalization, and address-matching functions for record linkage; also faster edit-distance implementations.

### Added functions
| Function | Kind | Description |
|---|---|---|
| `soundex(s)` | scalar | Soundex phonetic encoding. |
| `double_metaphone(s)` | scalar | Double-Metaphone phonetic encoding (stronger than Soundex). |
| `ngrams(s, n)` | scalar | Character n-grams from a string (returns string n-grams, **not** an array of array slices like Trino's `ngrams`). |
| `strip_diacritics(s)` | scalar | Remove diacritics. |
| `unaccent(s)` | scalar | Remove accents. |
| `find_address(...)` | scalar | Address match against a suffix-trie corpus. |
| `build_suffix_trie(...)` | aggregate | Build a suffix trie. |

### Overloaded functions
| Function | Description |
|---|---|
| `damerau_levenshtein(a, b)` | Faster Damerau-Levenshtein. |
| `levenshtein(a, b)` | Faster Levenshtein. |

### Trino-relevant matches
- ✅ Trino `soundex(char)` → `soundex(s)`. Direct match.
- ⚠️ Trino array `ngrams(array, n)` is *different* from Splink's string `ngrams(s, n)`. Don't conflate.
- ❌ No `word_stem`, `luhn_check`, base64-url/base32, `split_to_map`, 3-arg `strpos`, `regexp_count`, `regexp_position`.

---

## Vindex
https://duckdb.org/community_extensions/extensions/vindex

Vector-index extension (HNSW, IVF, DiskANN, SPANN). Strictly vector-search oriented.

| Function | Kind | Description |
|---|---|---|
| `hnsw_compact_index` | pragma | Compact HNSW indexes. |
| `pragma_hnsw_index_info` | table | HNSW index metadata. |
| `pragma_vindex_diskann_index_info` | table | DiskANN index metadata. |
| `pragma_vindex_hnsw_index_info` | table | HNSW index details. |
| `pragma_vindex_ivf_index_info` | table | IVF index metadata. |
| `pragma_vindex_spann_index_info` | table | SPANN index metadata. |
| `vindex_compact_index` | pragma | Compact a vector index. |
| `vindex_index_scan` | table | ANN search. |
| `vindex_join` | table macro | Vector similarity join. |
| `vindex_match` | table macro | Match vectors against an index. |
| `vss_join` / `vss_match` | table macro | Aliases for vector similarity join / match. |

Uses core DuckDB distance helpers `array_distance`, `array_cosine_distance`, `array_negative_inner_product`.

### Trino-relevant matches
❌ None.

---

## Inet (CORE extension — *not* community)
https://duckdb.org/docs/stable/core_extensions/inet

**Important:** The original Trino mapping doc claimed "DuckDB has no IP type" — that is **incorrect**. The core `inet` extension provides a unified `INET` type plus operators and helpers. Both IPv4 and IPv6 (with optional CIDR netmask) are stored in the same column.

### Data type
| Type | Description |
|---|---|
| `INET` | Single type covering IPv4 and IPv6 with optional CIDR (e.g. `192.168.0.0/16`, `::1/128`). Distinct from Postgres in that there is no separate `cidr`/`ipv4`/`ipv6` type — CIDR info lives inside the same `INET` value. |

### Scalars
| Function | Description |
|---|---|
| `host(INET)` | Host component (address with netmask stripped). |
| `netmask(INET)` | Network mask for the address's network. |
| `network(INET)` | Network portion (bits beyond the netmask zeroed). |
| `broadcast(INET)` | Broadcast address for the network. |
| `html_escape(VARCHAR)` | HTML-escape special characters (ships with this extension despite being unrelated to networking). |
| `html_unescape(VARCHAR)` | Inverse of `html_escape`. |

### Operators
| Operator | Description |
|---|---|
| `+` (INET, INTEGER) | Increment the address by N. |
| `-` (INET, INTEGER) | Decrement the address by N. |
| `<<=` | Subnet-contained-by-or-equal-to. |
| `>>=` | Subnet-contains-or-equal. |
| `<`, `<=`, `=`, `>=`, `>` | Natural ordering; IPv4 always sorts before IPv6. |

### Capabilities
- IPv4 and IPv6 in a single column.
- CIDR notation (`addr/prefix_len`) preserved in the value.
- Address arithmetic (increment/decrement).
- Subnet containment / overlap tests via `<<=` and `>>=`.
- Network calculation helpers.

### Trino-relevant matches
- ✅ Trino `IPADDRESS` type → DuckDB `INET` type. Note Trino additionally has `IPPREFIX`; in DuckDB the prefix/CIDR is embedded in the same `INET` value rather than a separate type.
- ✅ Trino `contains(network, address)` (subnet check) → DuckDB `network >>= address` operator. Push as an operator, not a function call.
- ⚠️ No first-class scalar named `ip_prefix(addr, len)`; use a literal CIDR or cast.
- ⚠️ No scalar `is_subnet_of` — only the operator form `>>=`/`<<=`.
- ⚠️ No `ipv4_at_bit_position` or IPv4/IPv6 family discriminator. Use NetQuack's `ip_version` on the textual form or a string-cast inspection.

---

## Notes for the Trino mapping document

When updating [RESEARCH-function-mapping.md](RESEARCH-function-mapping.md):

1. **IP address section** — replace the "DuckDB has no IP type" claim with a reference to the core `inet` extension and the `>>=` operator. The subnet `contains(network, address)` row should become ✅ pushable as `network >>= address` when the connector loads `inet`.
2. **Hash section** — `xxhash64` and `murmur3` (128-bit) are pushable when `hashfuncs` is loaded. `sha512` and the `hmac_*` family become pushable when `crypto` is loaded.
3. **Aggregate / sketches** — `theta_sketch_*`, `tdigest_agg`, `approx_most_frequent` become pushable when `datasketches` is loaded. State serialization is not wire-compatible with Trino's — only computed-value paths are safe to push.
4. **URL section** — `url_extract_*` scalars become pushable when `netquack` is loaded. `url_extract_parameter` requires the table-function form.
5. **String section** — `soundex` becomes pushable when `splink_udfs` is loaded.

The remaining open Trino-only functions (`crc32`, `spooky_hash_v2_*`, `word_stem`, `luhn_check`, `to_base64url`, `to_base32`, base/from-base conversion, statistical CDFs, `parse_duration`, `human_readable_seconds`, big-endian / IEEE-754 byte coding, `listagg WITHIN GROUP`, `map_agg`, `multimap_agg`, `checksum`) have no community-extension cover. They would need a custom DuckDB extension if Trino parity becomes a hard goal.
