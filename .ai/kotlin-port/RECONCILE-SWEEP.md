# Port closeout — reconciliation sweep

> Run once, now, on the freshly-merged green `main`. Goal: prove nothing fell through the
> port. The 73 confirmed findings in `adversarial-review-result.json` must reconcile exactly
> into BEFORE (fixed) + DURING (kit-applied) + AFTER (in-tree markers). If it balances, the
> port effort is provably complete and you can stop. If it doesn't, you want to know NOW,
> on warm context, not weeks from now chasing a finding that quietly vanished.
>
> The driving agent owns gradle/verification (reliable channel). All steps below are
> grep/count/read — no builds needed except the final green confirm.

## Pass criteria (the whole point)
```
BEFORE (5) + DURING (kit correctness sites) + AFTER (TODO(review:after) markers) == 73 findings
```
…with every finding ID in the JSON accounted for in exactly one bucket. No finding ID
appears in zero buckets (lost) or two buckets (double-counted).

---

## Step 1 — Inventory the AFTER markers in tree
```
cd jvm
grep -rn "TODO(review:after" --include=*.kt --include=*.java . | grep -v '\.java\.old' | sort
grep -rc "TODO(review:after" --include=*.kt --include=*.java . | grep -v ':0$' | grep -v '\.java\.old'
# total count:
grep -rn "TODO(review:after" --include=*.kt --include=*.java . | grep -v '\.java\.old' | wc -l
```
Expected ≈ 50 (worker reported 50). Capture the exact number and, if the markers carry
finding IDs/keys, capture the set of IDs.

## Step 2 — Inventory DURING (kit-applied correctness) sites
These were applied during kit-apply, logged in `kit-notes.json`. Cross-check the notes against
the actual tree so the audit trail matches reality:
```
cat .ai/kotlin-port/... /kit-notes.json   # whatever path it's at; find it:
find . -name kit-notes.json
grep -rn "TODO(kit" --include=*.kt . | grep -v '\.java\.old'   # should be ~empty: kit sweep consumed them
```
The DURING set per PLAN.md was: Locale.ROOT lowercase (2), null-safe SQLException.message (2),
resource leaks→use{} (DuckDbFileWriter, DucklakeMergeSink, ParquetWriter), retained-size
double-count (DucklakeSplit, DucklakeColumnHandle). Confirm each shows in kit-notes.json as APPLIED.
Any `TODO(kit …)` still in tree = a marker the kit sweep MISSED — investigate.

## Step 3 — Confirm BEFORE (5) are on main
B1 DucklakeStatTypes + endianness; B2 divide/modulo no-push; B3a union+end-snapshot;
B3b disable-pushdown-on-deletes; B4 temporal parent-period; B5 Arrow join-before-close.
```
git log --oneline main | grep -iE "type handling|divide|delete|temporal|arrow|before" | head
ls jvm/ducklake-catalog/src/dev/brikk/ducklake/catalog/DucklakeStatTypes.*   # .kt now (ported)
```

## Step 4 — Reconcile against the 73 findings
```
# How many findings total, and their IDs/keys:
python3 -c "import json; d=json.load(open('.ai/kotlin-port/adversarial-review-result.json')); \
print('findings:', len(d) if isinstance(d,list) else len(d.get('findings',d)))"
```
Build the three buckets (BEFORE ids, DURING ids, AFTER-marker ids) and assert:
- union of the three == full finding-id set (nothing lost),
- intersection of any two == empty (nothing double-counted).
If finding IDs aren't embedded in the markers, do it by description-matching and note any
finding that can't be confidently placed — those are the ones to eyeball.

## Step 5 — .java.old ↔ .kt counterpart audit (rides along cheaply)
Every ported `.java.old` should have a live `.kt` sibling; every `.kt` that replaced Java
should have its `.java.old`. Orphans = a rename that lost its replacement, or a stale leftover.
```
cd jvm
# .java.old with NO .kt sibling (BAD — lost replacement):
for f in $(find . -name '*.java.old'); do k="${f%.java.old}.kt"; [ -f "$k" ] || echo "ORPHAN .java.old (no .kt): $f"; done
# sanity: count them
find . -name '*.java.old' | wc -l
find . -name '*.kt' | grep -v '\.java\.old' | wc -l
# generated/ must still be .java (never ported):
find . -path '*/generated/*' -name '*.kt' | head   # expect empty
```

## Step 6 — Final green confirm (authoritative, from on-disk XML)
```
./gradlew :ducklake-catalog:test :trino-ducklake:test
# then read XML, not stdout:
python3 - <<'PY'
import glob, xml.etree.ElementTree as ET
for label,base in [("catalog","ducklake-catalog"),("trino","trino-ducklake")]:
    s=t=f=e=k=0
    for x in glob.glob(f"jvm/{base}/build/test-results/test/TEST-*.xml"):
        r=ET.parse(x).getroot(); s+=1
        t+=int(r.attrib.get('tests',0)); f+=int(r.attrib.get('failures',0))
        e+=int(r.attrib.get('errors',0)); k+=int(r.attrib.get('skipped',0))
    print(f"{label}: suites={s} tests={t} failures={f} errors={e} skipped={k}")
PY
```
Expected: catalog 108/0/0, trino 826/0/0 (or current counts — just 0 failures/0 errors).
Never `gradle clean`.

---

## Outcome
- **Balances + green** → port effort is provably complete. STOP here. Capture to memory:
  "Kotlin port done & on main; AFTER backlog = ~50 in-tree TODO(review:after) markers keyed
  to adversarial-review-result.json." Then AFTER work is a deliberate NEW initiative, own branch,
  slice by slice (suggested first slice: SPI/contract gaps — listTables omits views,
  getMemoryUsage/getCompletedBytes — correctness-adjacent, before the efficiency tail).
- **Doesn't balance** → list the unaccounted finding IDs. Each is either (a) silently fixed
  during the port and just not marked (add the marker or note it), or (b) genuinely dropped
  (re-plant the marker / re-open). Don't start AFTER work until the ledger is clean.

## One caution carried from the port (don't relearn it)
**A negative result from a fan-out search (Explore "no matches") is a hypothesis, not a finding.**
During kit-apply, Explore reported "no matches" for Optional patterns; a real `grep -rn` found
203 across 26 files. For any counting/enumeration step above, drive it off real grep, NOT off
an Explore subagent's summary.
