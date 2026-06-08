# DuckLake-on-Doris — RESEARCH TODO

Open questions, feasibility studies, and "remember this" ideas — **not scheduled
work**. Implementation tracks live in the sibling todos:
- 📖 [`ducklake-doris-todo.md`](./ducklake-doris-todo.md) — READ path
- ✍️ [`ducklake-doris-todo-write.md`](./ducklake-doris-todo-write.md) — WRITE path

Production-blocker coordination with the Doris team (SPI_READY_TYPES removal,
BE delete-file nullability, etc.) stays in the READ todo's *Upstream coordination*
section — those are tracked asks, not open research.

---

## Backlog

- [ ] **Doris as a DuckLake catalog backend (speculative).** DuckLake's catalog
  metadata today lives in Postgres / SQLite / DuckDB. Could **Doris itself** also
  serve as that catalog backend — host the `ducklake_*` metadata tables — so a
  Doris-centric deployment needs no separate OLTP store? Two things to settle
  before this is more than an idea:
  1. **Migration dialect.** DuckLake's catalog schema + migrations are written in
     (largely) ANSI/standard SQL for those engines. Doris's DDL/DML is MySQL-ish
     with OLAP semantics, **not ANSI** — the migration scripts won't run as-is, so
     it'd need a Doris-dialect port of the DuckLake catalog DDL/migrations.
  2. **Semantics validity for the role.** Doris is a columnar OLAP engine; a
     catalog backend needs OLTP-shaped guarantees — atomic single-row
     INSERT/UPDATE/DELETE on small metadata tables, unique/primary-key enforcement,
     and serializable/consistent commit semantics for snapshot creation under
     concurrency. Open question whether Doris's transaction + UPDATE/DELETE +
     constraint model is strong enough to be a *valid* catalog store (vs. merely
     syntactically loadable).
  Captured 2026-06-08 as a "remember this" idea — feasibility research only, well
  after the read-path v1 lands.
