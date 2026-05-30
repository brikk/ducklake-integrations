# REPORT: Hash function NULL handling + round-6 extension availability

**Date:** 2026-05-28
**DuckDB version:** 1.5.3
**Probe:** `ProbeHashNullHandling` (deleted after this commit). Tested NULL handling for built-in DuckDB hash functions, the planned round-6b/6c/6d extension functions (`crypto`, `hashfuncs`, `netquack`), and confirmed sanity on the already-shipped round-4 encoding macros.

## TL;DR

1. **DuckDB built-in `md5`, `sha1`, `sha256` propagate NULL correctly** — `md5(NULL) → NULL`, `sha256('a' || NULL || 'c') → NULL`. ✅ Trino-aligned. These can ship as round-6b WITHOUT the `crypto` extension. Same wrap pattern as round 4: `unhex(md5(b))` returns BLOB matching Trino's VARBINARY.

2. **`crypto`, `hashfuncs`, `netquack` extensions returned HTTP 404** from `extensions.duckdb.org` for DuckDB 1.5.3 / osx_arm64. **Most likely cause: DuckDB 1.5.3 is a recent release and the community extensions catalog hasn't built for it yet on most/any platform.** Not necessarily a platform-specific gap — `inet` was listed as a candidate (i.e. it has built for 1.5.3) but `crypto` / `hashfuncs` / `netquack` haven't. Re-probe in a few days / weeks; the catalog backfills as maintainers cut builds. Round-6b-ext (sha512 + HMACs), round-6c (xxhash64, murmur3), and round-6d (url_extract_*) are queued and blocked only on the catalog catching up.

3. **DuckDB's variadic `hash(value, ...)` does NOT propagate NULL.** `hash(NULL)` returns a stable UBIGINT (`13787848793156543929`), not NULL. `hash('a', NULL, 'c')` returns a different stable UBIGINT than `hash('a', '', 'c')`. **DuckDB treats NULL as a distinguished sentinel inside hash composition** — directly opposite to Trino's null-propagation. Moot for pushdown (Trino has no variadic hash), but worth knowing.

## Findings detail

### Core DuckDB hash NULL propagation — ✅ aligned with Trino

| Function | Input | DuckDB result | Trino-expected |
|---|---|---|---|
| `md5(NULL)` | NULL | **NULL** | NULL |
| `md5('')` | empty | `d41d8cd98f00b204e9800998ecf8427e` | same hex (cast via `unhex` for VARBINARY) |
| `md5('a' \|\| NULL \|\| 'c')` | NULL-containing concat | **NULL** | NULL (`\|\|` propagates) |
| `sha256(NULL)` | NULL | **NULL** | NULL |
| `sha256('')` | empty | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | same hex |

These can ship via `unhex(md5(...))` / `unhex(sha1(...))` / `unhex(sha256(...))` for BLOB-typed output that matches Trino's VARBINARY return.

### DuckDB's variadic `hash(value, ...)` — NULL is a sentinel, not propagated

| SQL | Result (UBIGINT) | Observation |
|---|---|---|
| `hash(NULL)` | `13787848793156543929` | NULL hashed as a value, not propagated |
| `hash('a')` | `12561829011207016135` | baseline |
| `hash('a', NULL)` | `12149695185283952434` | NULL in trailing position changes hash |
| `hash('a', NULL, 'c')` | `451469780638546153` | NULL middle, changes hash |
| `hash('a', 'b', 'c')` | `6066993828470297781` | all non-NULL baseline |
| `hash('a', '', 'c')` | `8161848143140746562` | distinct from NULL middle — empty string is its own value |

DuckDB distinguishes NULL from empty string in variadic `hash()`. Trino has no equivalent multi-arg form on `xxhash64` / `md5` / `sha*`, so this surface isn't pushdown-relevant — but if a user has data with NULLs and writes a wrapped expression like `xxhash64(col1 || col2)`, the concatenation step `col1 || col2` will be NULL whenever either operand is NULL, and the wrapping hash will then be NULL. That matches Trino's `xxhash64(col1 || col2)` behaviour.

### Round-4 encoding macros — sanity check

All NULL-propagate correctly: `hex(NULL)`, `unhex(NULL)`, `to_base64(NULL)`, `from_base64(NULL)`, `url_encode(NULL)`, `url_decode(NULL)`, `length(NULL)`. ✅

### Extension availability — round-6 blocker

Attempted installs against the DuckDB community extension repo on **osx_arm64 / DuckDB 1.5.3**:

| Extension | Status | Notes |
|---|---|---|
| `crypto` | ❌ HTTP 404 | Not yet built for 1.5.3. Candidate extensions reported (i.e. things that DO have 1.5.3 builds for this platform): `uc_catalog`, `core_functions`, `postgres_scanner`, `vortex`, `parquet`. |
| `hashfuncs` | ❌ HTTP 404 | Not yet built. Candidates: `core_functions`, `aws`, `json`, `motherduck`, `azure`. |
| `netquack` | ❌ HTTP 404 | Not yet built. Candidates: `quack`, `inet`, `delta`, `motherduck`, `ducklake`. |
| `inet` | ⚠️ not probed; listed as a candidate in the netquack 404 response | Has a 1.5.3 build for our platform — verify before relying on it. |

**The DuckDB community-extensions repo publishes per (extension × DuckDB version × platform).** DuckDB 1.5.3 is a recent release; the catalog backfills as maintainers cut builds. The 404s are almost certainly "the maintainer hasn't pushed a 1.5.3 build yet" rather than "the extension doesn't support our platform" — the same extensions are presumed to ship for 1.5.x in general, just not yet for 1.5.3 specifically.

Mitigation strategy:

1. **Re-probe periodically** — the catalog state changes over days/weeks as maintainers cut builds. A small probe job (one-off cron or a manual run) reports availability so we know when to unblock each round.
2. **Ship the core-only subset first** — round 6b-core (`md5`/`sha1`/`sha256` via `unhex(...)` wrap) needs zero new dependencies and is shipped. Same pattern unblocks any other core-DuckDB-resident functionality we identify.
3. **When extensions catch up**, round 6b-ext / 6c / 6d are pre-staged in this doc + the TODO; promotion is just "uncomment the macros, add fixtures, run probe, ship."
4. **Optional fallback if extensions don't catch up**: pin DuckDB to a slightly older patch version (e.g. 1.5.2) for the connector if that version has the extensions built. Cost: missing whatever DuckDB 1.5.3 fixed. Worth evaluating only if the wait stretches beyond a few weeks.

## Related: `concat` / `concat_ws` NULL behaviour

Probed via `ProbeConcatNullHandling` (deleted after this commit). Same shape question as multi-arg hashes — does NULL inside a multi-arg concat-style call propagate or get silently skipped?

| Function | Input shape | DuckDB result | Trino-expected | Verdict |
|---|---|---|---|---|
| `concat('a', NULL, 'c', 'd')` | middle NULL | `'acd'` (NULL skipped) | NULL | ❌ **DIVERGENT** |
| `concat(NULL, 'b', 'c')` | first NULL | `'bc'` | NULL | ❌ DIVERGENT |
| `concat(NULL, NULL)` | all NULL | `''` (empty string) | NULL | ❌ DIVERGENT |
| `concat_ws('\|', 'a', NULL, 'c', 'd')` | NULL element | `'a\|c\|d'` (skipped) | `'a\|c\|d'` (skipped) | ✅ aligned |
| `concat_ws('\|', NULL, 'b', 'c')` | first NULL element | `'b\|c'` | `'b\|c'` | ✅ aligned |
| `concat_ws(NULL, 'a', 'b')` | NULL separator | NULL | NULL | ✅ aligned (both NULL-propagate sep) |
| `concat_ws('\|', NULL, NULL)` | all NULL elements | `''` (empty string) | `''` | ✅ aligned |
| `'a' \|\| NULL \|\| 'c'` (operator form) | NULL middle | NULL | NULL | ✅ aligned (both `\|\|` propagate) |

**Takeaway:**
- `concat(...)` confirmed divergent on every NULL-bearing case. **Stays off `PUSHABLE_FUNCTIONS`** (already excluded). If a query needs NULL-propagating concat in pushdown, use `||` (operator), which IS aligned.
- `concat_ws(...)` confirmed aligned across all NULL shapes. Our existing `trino_concat_ws/{2..5}` macros (shipped round 2) remain safe.
- The `concat` divergence is exactly the same shape as DuckDB's variadic `hash(value, ...)`: NULL gets silently absorbed into the result rather than propagated. The shared pattern reinforces "do not push DuckDB functions that take variadic mixed-NULL inputs unless you've verified the engine drops the NULL the same way Trino does."

## Revised round-6 plan recommendation

| Round | Scope | Blocker? |
|---|---|---|
| **6a** | Core DuckDB extras: `sign`, `bit_length`, `pi()`, maybe 2-arg `truncate` | None. Cheap wins. |
| **6b-core** | Core DuckDB hash macros: `trino_md5`, `trino_sha1`, `trino_sha256` via `unhex(...)` wrap | None. **Add this immediately** — no extension needed, NULL behaviour verified aligned. |
| **6b-ext** | `trino_sha512`, `trino_hmac_md5/sha1/sha256/sha512` via `crypto` extension | Verify `crypto` is published for production platform. If not, defer. |
| **6c** | `trino_xxhash64`, `trino_murmur3` via `hashfuncs` | Same — verify `hashfuncs` published. Plus byte-order verification per earlier TODO note. |
| **6d** | `trino_url_extract_*` via `netquack` | Same — verify published. |

The 6b-core split unblocks meaningful immediate progress (4 new pushable functions, no platform risk) while we triage the community-extension availability for the larger plan.
