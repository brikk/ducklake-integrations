# Operating rules for agents in this repo

## Consult before big workarounds (HARD RULE)
Do NOT invent large workarounds, architectural crutches, or "clever" bypasses
without checking with the user FIRST. Today we had to undo several doozies
(a temp-Parquet-on-shared-storage inlined-data path that can't work on a
distributed FE/BE cluster; a wrong "JDK-25 ABI break" diagnosis with an
assume-skip; an over-engineered warehouse-dot-dir scratch scheme when the OS
temp dir / a real fix was the answer). When you hit a wall:
1. State the problem and the options plainly.
2. Say which is a real fix vs a crutch, and what each costs.
3. Ask before implementing anything that (a) writes to shared storage,
   (b) assumes FE and BE share a filesystem, (c) skips/disables tests,
   (d) masquerades as another format/engine, or (e) degrades a type/semantic.

## Never blame the harness/system to turn a red bar green
If trino / pg_ducklake / DuckDB pass a test we fail, the bug is almost
certainly ours. Root-cause it (reproduce, read the actual code paths) before
skip-listing. A skip reason must be TRUE and specific, never "environment
variance" hand-waving.

## Prefer failing loud over silently wrong
Guards throw `DorisConnectorException` (which the corpus classifies as an
engine-skip) rather than return wrong rows. Never silently over/under-return.

## Verify against the real thing
The Doris FE runs JDK 17 with a curated jar set (`output/fe/lib`). Test-JVM
classpath issues (e.g. the fat `~/.m2` fe-thrift shadowing parquet-format) are
NOT runtime issues — prove which you're looking at before working around it.

## Testing discipline
- Per-corpus-dir runs (~1 min) are the verification loop; NEVER the full corpus
  unless explicitly asked.
- Commit at meaningful intervals; do NOT push until asked.
- Keep `dev-docs/CRUTCHES-AND-SHORTCUTS.md` current — every non-production-clean
  path goes there.

See `jvm/doris-ducklake/dev-docs/` for the plan, friction log, and crutch
inventory.
