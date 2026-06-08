# DuckLake-on-Doris — WRITE-path TODO

The write path is **post read-v1** — not started, sequenced after the read side
is solid. Sibling todos:
- 📖 [`ducklake-doris-todo.md`](./ducklake-doris-todo.md) — READ path (active)
- 🔬 [`ducklake-doris-todo-research.md`](./ducklake-doris-todo-research.md) — research

## Where we stand

Nothing implemented. But two big enablers already exist:
- **The catalog substrate is done & proven.** `DucklakeCatalog` (in
  `ducklake-catalog`) already has every commit/DDL primitive — `commitInsert`,
  `commitDelete`, `commitMerge`, `commitAddFiles`, `createSchema`/`dropSchema`,
  `createTable`/`dropTable`, `addColumn`/`dropColumn`/`renameColumn`,
  `create/drop/renameView`. `trino-ducklake` exercises all of them, so the
  metadata-commit half of writes is a reuse, not a build.
- **The Doris SPI has a write surface**: `ConnectorMetadata` extends
  `ConnectorWriteOps` (one of its six sub-interface ops, all defaulting to
  no-ops). So the hooks exist.

## The central unknown — W0 (research spike, do first)

- [ ] **How does a Doris *external-catalog* connector write data?** The P-series
  read connectors (trino, hudi, jdbc) are read-only — **no P-series plugin
  exercises `ConnectorWriteOps`**, so the write contract is unproven by example.
  Map: which `ConnectorWriteOps` methods exist; whether the **Doris BE** writes
  the Parquet data files (and hands the connector file metadata to commit), or the
  connector is expected to write; how INSERT/sink results flow FE↔BE; whether DDL
  (`CREATE TABLE`) on a plugin catalog is even routed to the connector. Compare
  against how `trino-ducklake` does it in-JVM (`DucklakePageSink` /
  `DucklakeMergeSink`) — but expect Doris's model to differ (BE-side execution).
  **Output:** a feasibility note + which write ops are reachable on the current SPI.

## Phased plan (provisional, pending W0)

- [ ] **W1 — DDL (cheapest).** `CREATE/DROP SCHEMA`, `CREATE/DROP TABLE` →
  pure catalog metadata via `catalog.createSchema`/`createTable`/etc. No data
  movement, no BE involvement. Likely the first reachable write.
- [ ] **W2 — INSERT (append).** BE writes Parquet data files; connector commits
  them to the DuckLake catalog via `catalog.commitInsert`. Hinges on the W0
  BE→FE write handshake.
- [ ] **W3 — CTAS.** W1 DDL + W2 INSERT composed.
- [ ] **W4 — DELETE / UPDATE (merge-on-read).** Write position-delete files +
  `catalog.commitDelete`/`commitMerge`. **Gated** on the read-side delete blocker
  (BE OPTIONAL-column position-delete rejection — see READ todo Step 7) being
  resolved, since written deletes must also be readable.
- [ ] **W5 — MERGE.** Full upsert via the delete+insert fragment path.
- [ ] **Cross-engine round-trip tests.** Doris-written table read back by DuckDB
  (and vice-versa) against the same metadata DB — the integrity check that the
  commit semantics match DuckLake's.

## Reference

`trino-ducklake`'s write classes are the semantic template (not the execution
template): `DucklakePageSinkProvider`/`DucklakePageSink` (INSERT),
`DucklakeMergeSink` (DELETE/UPDATE/MERGE), and the `catalog.commit*` calls each
makes. Doris reuses the **same `catalog.commit*` calls**; only the
data-file-production half differs (Doris BE vs in-JVM writer).
