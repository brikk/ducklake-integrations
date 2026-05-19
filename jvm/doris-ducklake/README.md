# doris-ducklake

⚠️ **Pre-alpha. Do not use.** This plugin targets the unreleased
`fe-connector` SPI from [apache/doris#62767](https://github.com/apache/doris/pull/62767),
which is still in active review and changes shape on every rebase. It
also depends on a one-line FE patch (`SPI_READY_TYPES` whitelist) that
is not in any Doris release, so a stock Doris build cannot load it.
The plugin is incomplete (read path only, no pushdown, no writes, no
position deletes yet — see [`ducklake-doris-todo.md`](./ducklake-doris-todo.md)),
unstable (regularly broken by upstream SPI churn), and unsupported. If
you are looking for a working DuckLake adapter for an OLAP engine
today, use [`trino-ducklake`](../trino-ducklake/) instead. This
directory exists so the integration is ready the day the upstream SPI
lands; treat anything you find here as a worktree, not a release.
