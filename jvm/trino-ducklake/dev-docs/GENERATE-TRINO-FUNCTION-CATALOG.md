# Generating the Trino built-in function catalog (for the sqlglot Trino dialect)

**Audience:** the agent maintaining the DuckDB→{Trino,Doris} sqlglot Kotlin port (a separate
project). This is the recipe to produce a full, authoritative, version-matched list of Trino
built-in functions **with signatures**, to feed the port's Trino dialect (which functions exist,
their argument arities/types, scalar vs aggregate vs window).

Repeat this per targeted Trino version — the catalog changes across releases.

## What you get

A row per function **overload**: `name | return type | argument types | function type | deterministic | description`.
For Trino **481** this is **746 overloads / 319 distinct names** (597 scalar, 129 aggregate,
16 window, 4 table).

## Source of truth: `SHOW FUNCTIONS`

Do **not** scrape the jar's `@ScalarFunction`/`@SqlType` annotations (unresolved generics,
`@TypeParameter`, operator functions) or the docs (prose, not structured). `SHOW FUNCTIONS` is the
engine's own **resolved** registry and is trivially regenerable.

---

## Method A — simplest: any running Trino (CLI or JDBC)

Point at a Trino server of the version you target.

**CLI:**
```bash
trino --server <host>:8080 --catalog system --schema runtime \
      --output-format TSV_HEADER \
      --execute "SHOW FUNCTIONS" > trino-functions.tsv
```

**JDBC (io.trino:trino-jdbc, matches any project):**
```kotlin
java.sql.DriverManager.getConnection("jdbc:trino://<host>:8080/system/runtime", "any", null)
    .use { c ->
        c.createStatement().executeQuery("SHOW FUNCTIONS").use { rs ->
            val n = rs.metaData.columnCount
            while (rs.next()) {
                println((1..n).joinToString("\t") { rs.getString(it)?.replace('\t', ' ') ?: "" })
            }
        }
    }
```

## Method B — version-pinned, no running server (in-JVM harness)

Use when you want the catalog to track an exact Gradle-pinned Trino version reproducibly (no
external server). Needs `io.trino:trino-main` + `io.trino:trino-testing` + JUnit 5 on the **test**
classpath, with the Trino version pinned to your target (e.g. `481`). This is what
`trino-ducklake` used to generate the current `trino-functions.tsv`.

```kotlin
import io.trino.testing.DistributedQueryRunner
import io.trino.testing.TestingSession.testSessionBuilder
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

internal class DumpTrinoFunctions {
    @Test
    fun dump() {
        val session = testSessionBuilder().setCatalog("system").setSchema("runtime").build()
        val runner = DistributedQueryRunner.builder(session).build()
        try {
            val result = runner.execute(session, "SHOW FUNCTIONS")
            val n = result.types.size                      // 6 columns on Trino 481
            val lines = result.materializedRows.map { row ->
                (0 until n).joinToString("\t") {
                    row.getField(it)?.toString()?.replace('\t', ' ')?.replace('\n', ' ') ?: ""
                }
            }
            Files.write(Path.of("trino-functions.tsv"), lines)
        } finally {
            runner.close()
        }
    }
}
```
Run: `./gradlew :<module>:test --tests "*.DumpTrinoFunctions"` then collect `trino-functions.tsv`
from the module's working dir. (It's a throwaway generator — don't keep it in the test suite.)

---

## Output schema (Trino 481 = 6 columns, in order)

| # | column | notes |
|---|--------|-------|
| 0 | function name | group overloads by this |
| 1 | return type | may be parametric (`timestamp(p)`, `varchar(x)`) or a type var (`E`, `T`) |
| 2 | argument types | comma-separated; same parametric/type-var forms |
| 3 | function type | `scalar` / `aggregate` / `window` / `table` |
| 4 | deterministic | `true` / `false` |
| 5 | description | free text (sometimes empty) |

Example rows:
```
date_trunc	timestamp(p)	varchar(x), timestamp(p)	scalar	true	Truncate to the specified precision in the session timezone
concat	varchar	varchar	scalar	true	Concatenates given strings
approx_percentile	array(double)	double, array(double)	aggregate	true
row_number	bigint		window	true	...
```

## How to interpret it for the transpiler (important caveats)

1. **Overloads:** multiple rows per name. For transpilation you mostly need *name existence* +
   *arity variants*; group by column 0.
2. **Parametric / type-variable args** appear literally (`varchar(x)`, `timestamp(p)`,
   `decimal(p,s)`, `E`, `T`, `K`, `V`, `unknown`). Treat as "any" for mapping.
3. **Variadic is NOT flagged.** This `SHOW FUNCTIONS` has no "Variable Arity" column, so variadics
   (`concat`, `greatest`, `least`, `coalesce`, …) show only 1–2-arg overloads even though the parser
   accepts N. **Do not hard-enforce a max arity from this list — it is a lower bound.**
4. **Operators are NOT here.** `::`, `||`, `%`, `+`, `-`, `CAST`, `TRY_CAST`, `IS NULL`, lambdas,
   `[]` subscript, etc. are grammar-level, not functions. Handle them in the port's
   operator/expression mapping, not from this catalog.
5. **`date_trunc` sanity check** (a known corpus case): row is `date_trunc(varchar(x), timestamp(p))`
   → **unit is the FIRST arg**, and the unit string is **case-insensitive** in Trino (`'MONTH'` and
   `'month'` both match — the matcher ASCII-lowercases). Don't reorder args or force unit case for
   the Trino dialect.
6. **`QUALIFY` is not a function and not supported by Trino's grammar** — it must be rewritten to a
   subquery for Trino (verified: `QuerySpecification` has where/having/groupBy/window, no qualify).
   Watch the `SELECT *, <alias>` double-projection trap when the outer star already carries the
   pushed-down window alias.

## Optional: TSV → JSON grouped by name

```bash
python3 - <<'PY'
import csv, json, collections
out = collections.defaultdict(list)
with open("trino-functions.tsv") as f:
    for r in csv.reader(f, delimiter="\t"):
        if len(r) < 5: continue
        name, ret, args, ftype, det = r[0], r[1], r[2], r[3], r[4]
        out[name].append({"returnType": ret,
                          "argTypes": [a.strip() for a in args.split(",") if a.strip()],
                          "functionType": ftype, "deterministic": det == "true"})
json.dump(out, open("trino-functions.json", "w"), indent=2, sort_keys=True)
print(len(out), "functions")
PY
```

## Regenerating on a Trino version bump

Bump the Trino dependency (Method B) or point at the new server (Method A), re-run, re-diff against
the committed catalog. New/removed/re-signatured functions are exactly what the port's Trino dialect
needs to track.
