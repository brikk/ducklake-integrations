export const meta = {
  name: 'kotlin-port',
  description: 'Port one DuckLake JVM slice to Kotlin in two independently-verified steps: (1) faithful file-by-file translation with before/after test verification, then (2) build a shared idiom kit from the ported Kotlin, apply it, and verify again. Verification granularity is tuned per slice (catalog runs full suites freely; trino narrows to the tests actually touched). Converted .java is renamed .java.old (never deleted). The kit accumulates across slices; an optional combine pass reconciles one unified kit and re-applies it.',
  whenToUse: 'Run once per slice in order: catalog-test -> catalog-main -> trino-test -> trino-main, then slice="combine". Each run does faithful-port+verify then kit+apply+verify. args.stopAfter="port" stops after translation; "kit" stops before applying the kit. Mutates the tree and runs Gradle (incremental, never clean).',
  phases: [
    { title: 'Map', detail: 'discover the slice files (excludes generated/, build/)' },
    { title: 'Baseline', detail: 'TEST slices: run the full Java suite once, capture per-class counts + pass/fail' },
    { title: 'Port', detail: 'faithful file-by-file translation (NO shared helpers yet)' },
    { title: 'Verify', detail: 'tuned per slice: full suite (catalog), per-class aggregate (trino-test), small-batch + big-one-at-a-time (trino-main)' },
    { title: 'Kit', detail: 'ONE agent builds-or-extends the shared idiom kit from the ported Kotlin' },
    { title: 'Apply', detail: 'rewrite the ported files to use the kit helpers (behavior-preserving)' },
    { title: 'ReVerify', detail: 'verify again that the idiom refactor preserved behavior' },
  ],
}

const base = p => p.split('/').pop()
const dir = p => p.split('/').slice(0, -1).join('/')
const stem = p => base(p).replace(/\.(java|kt)$/, '')
const fqcn = p => { const m = p.match(/(dev\/brikk\/.*)\.java$/); return m ? m[1].replace(/\//g, '.') : null }
const chunk = (a, n) => { const o = []; for (let i = 0; i < a.length; i += n) o.push(a.slice(i, i + n)); return o }

// ---------------------------------------------------------------------------
const REPO = '/Users/jminard/DEV/brikk/repos/ducklake-integrations'
const JVM = `${REPO}/jvm`
const REVIEW = `${REPO}/.ai/kotlin-port`
const IDIOM_DIR = `${REVIEW}/_idioms`
const IDIOM_KIT = `${IDIOM_DIR}/IdiomKit.kt`
const IDIOM_DOC = `${IDIOM_DIR}/idioms.md`
const KIT_NOTES = `${IDIOM_DIR}/kit-notes.json`

// fast: re-running the full module suite is cheap (catalog). twoTier: trino-main batching strategy.
const SLICES = {
  'catalog-test': { roots: [`${JVM}/ducklake-catalog/test`, `${JVM}/ducklake-catalog/testFixtures`], kind: 'test', module: 'ducklake-catalog', testTask: ':ducklake-catalog:test', fast: true,
    note: 'Catalog tests + fixtures, converted against still-Java catalog main via interop. Each test class must reproduce its Java baseline.' },
  'catalog-main': { roots: [`${JVM}/ducklake-catalog/src`], kind: 'main', module: 'ducklake-catalog', testTask: ':ducklake-catalog:test', fast: true,
    note: 'jOOQ-heavy core. NEVER touch ducklake-catalog/generated (jOOQ codegen — stays Java, calls fine from Kotlin). Oracle = the already-Kotlin catalog tests; full re-runs are cheap.' },
  'trino-test': { roots: [`${JVM}/trino-ducklake/test`], kind: 'test', module: 'trino-ducklake', testTask: ':trino-ducklake:test', fast: false,
    note: 'Trino connector tests (heavy cross-engine/DuckDB integration — slow). Verify by the specific class being worked on, not the whole suite.' },
  'trino-main': { roots: [`${JVM}/trino-ducklake/src`], kind: 'main', module: 'trino-ducklake', testTask: ':trino-ducklake:test', fast: false, twoTier: true,
    note: 'Trino connector plugin. SPI nullability matters — read extracted SPI source. Small value/state types batch together; big files go one at a time with a full verify after.' },
}
const ORDER = ['catalog-test', 'catalog-main', 'trino-test', 'trino-main']

const sliceName = (args && args.slice) || 'catalog-test'
const stopAfter = (args && args.stopAfter) || null
const isCombine = sliceName === 'combine'
const SMALL_LOC = (args && args.smallLoc) || 120   // threshold for trino-main's small-file batch

const DEPS_REF = `${JVM}/trino-ducklake/build/deps-reference`
const depsHint = `\n\nDEPENDENCY SOURCE (best-effort): library sources may be extracted under ${DEPS_REF}. If present, READ them for exact Trino SPI / Arrow / DuckDB nullability (Kotlin T vs T?). NEVER scan $HOME or ~/.gradle.`

const INTEROP_RULES = `
PORT CONTRACT — each ported file is a DROP-IN REPLACEMENT (Gradle compiles Java+Kotlin from the SAME flat dirs; most callers + jOOQ-generated code stay Java):
- Same package, same public type name(s); public signatures stay call-compatible from Java.
- MANDATORY interop: @JvmStatic (companion statics), const val/@JvmField (constants), @JvmOverloads (default args), @JvmName.
- Nullability matches real contracts (verify against extracted source). Optional<T> -> T?.
- Records -> data classes; builders/static factories -> companion factory + named/default args; try/finally close -> use{}.
- REPLACE A FILE: write Foo.kt in the SAME dir as Foo.java, then: mv Foo.java Foo.java.old (NEVER delete; .old is ignored by compilation, kept for diffing). Never touch /generated/.
- BEHAVIOR-PRESERVING. No bug fixes, no logic refactor. Suspected bug? record it; don't fix it.
`.trim()

const FAITHFUL_BAR = `
THIS IS THE FAITHFUL TRANSLATION STEP — a clean, close 1:1 port. Use LANGUAGE-LEVEL Kotlin idioms freely (?./?:/require, use{}, data class, when, named args, val), but do NOT invent project-wide/cross-cutting helpers or extensions yet — that is a deliberate LATER step (the idiom kit). No bespoke abstractions here.
`.trim()

const GRADLE_GUIDE = (module) =>
  `GRADLE (from ${JVM} as BASH, NEVER 'clean'): ./gradlew <task> --console=plain. Per-class results: parse ${JVM}/${module}/build/test-results/test/TEST-*.xml (<testsuite> tests/failures/errors/skipped). Class passes iff failures=0 and errors=0.`

// ===========================================================================
// COMBINE MODE
// ===========================================================================
if (isCombine) {
  phase('Kit')
  const COMBINE_SCHEMA = { type: 'object', additionalProperties: false,
    properties: { summary: { type: 'string' }, helperCount: { type: 'integer' }, kitSource: { type: 'string' }, changed: { type: 'boolean' } }, required: ['summary', 'helperCount', 'kitSource', 'changed'] }
  const unified = await agent(
    `Reconcile the accumulated idiom material into ONE unified kit for the whole codebase. Read ${IDIOM_KIT}, ${KIT_NOTES}, and grep the ported Kotlin across both modules. ` +
    `Merge near-duplicate helpers, generalize where a catalog and a trino helper are the same idea, drop unused ones. Keep small + Java-interop-safe. Write ${IDIOM_KIT}, refresh ${IDIOM_DOC}, return.`,
    { label: 'combine:kit', phase: 'Kit', schema: COMBINE_SCHEMA })
  log(`Unified kit: ${unified.helperCount} helpers, changed=${unified.changed}`)
  if (stopAfter === 'kit' || !unified.changed) return { mode: 'combine', kit: { path: IDIOM_KIT, helpers: unified.helperCount, changed: unified.changed }, note: unified.changed ? 'stopped before re-apply' : 'kit unchanged' }

  phase('Apply')
  const ktMap = await agent(
    `List every ported Kotlin file under both modules (exclude /generated/, /build/, and ${IDIOM_KIT}). find ${JVM}/ducklake-catalog ${JVM}/trino-ducklake -name '*.kt' -not -path '*/generated/*' -not -path '*/build/*'. Return {path,loc} each.`,
    { label: 'combine:discover', phase: 'Apply', model: 'sonnet', schema: { type: 'object', additionalProperties: false, properties: { files: { type: 'array', items: { type: 'object', additionalProperties: false, properties: { path: { type: 'string' }, loc: { type: 'integer' } }, required: ['path', 'loc'] } } }, required: ['files'] } })
  const allKt = ((ktMap && ktMap.files) || []).map(f => f.path).filter(p => p && p !== IDIOM_KIT)
  await parallel(chunk(allKt, 5).map((b, i) => () => agent(
    `Re-apply the UNIFIED idiom kit (${IDIOM_KIT}) to these Kotlin files — replace remaining hand-rolled instances of a kit-covered pattern with the helper. Behavior-preserving; back up each file to ${REVIEW}/_combine-prekit/ before editing.\n${b.map(p => '  - ' + p).join('\n')}`,
    { label: `combine:apply-${i + 1}`, phase: 'Apply' }).catch(() => null)))

  phase('ReVerify')
  const reverify = await agent(
    `Compile both modules and run both suites to confirm the unified-kit re-apply preserved behavior. From ${JVM} (NO clean): ./gradlew :ducklake-catalog:test :trino-ducklake:test --console=plain. Report green/regressions; minor kit fixes only, else flag.`,
    { label: 'combine:reverify', phase: 'ReVerify', schema: { type: 'object', additionalProperties: false, properties: { allGreen: { type: 'boolean' }, regressions: { type: 'array', items: { type: 'string' } }, minorFixesApplied: { type: 'array', items: { type: 'string' } }, notes: { type: 'string' } }, required: ['allGreen', 'regressions', 'minorFixesApplied', 'notes'] } })
  return { mode: 'combine', kit: { path: IDIOM_KIT, helpers: unified.helperCount }, reapplied: allKt.length, reverify }
}

// ===========================================================================
// PER-SLICE MODE
// ===========================================================================
const slice = SLICES[sliceName]
if (!slice) { log(`Unknown slice "${sliceName}". Valid: ${Object.keys(SLICES).join(', ')}, combine`); return { error: 'unknown slice', valid: [...Object.keys(SLICES), 'combine'] } }
const OUT = `${REVIEW}/${sliceName}`
const GG = GRADLE_GUIDE(slice.module)
log(`Slice: ${sliceName} (${slice.kind})${slice.twoTier ? ' two-tier' : ''}${slice.fast ? ' fast' : ' slow'}. stopAfter=${stopAfter || 'none'}`)

phase('Map')
const MANIFEST_SCHEMA = { type: 'object', additionalProperties: false, properties: { files: { type: 'array', items: { type: 'object', additionalProperties: false, properties: { path: { type: 'string' }, loc: { type: 'integer' } }, required: ['path', 'loc'] } } }, required: ['files'] }
const mapResult = await agent(
  `Enumerate every hand-written Java file (+loc) for the "${sliceName}" slice under:\n${slice.roots.map(r => '  - ' + r).join('\n')}\nEXCLUDE /generated/ and /build/. Use:\n  for r in ${slice.roots.join(' ')}; do find "$r" -name '*.java' -not -path '*/generated/*' -not -path '*/build/*'; done | while read f; do echo "$(wc -l < "$f") $f"; done\nEnumerate only.`,
  { label: 'map:discover', phase: 'Map', model: 'sonnet', schema: MANIFEST_SCHEMA })
const files = ((mapResult && mapResult.files) || []).filter(f => f && f.path && !/\/(generated|build)\//.test(f.path))
if (files.length === 0) { log('No Java files for this slice — aborting.'); return { error: 'no files', slice: sliceName } }
log(`Discovered ${files.length} Java files.`)

const EQUIV_SCHEMA = { type: 'object', additionalProperties: false, properties: { equivalent: { type: 'string', enum: ['yes', 'no', 'uncertain'] }, severity: { type: 'string', enum: ['none', 'low', 'medium', 'high'] }, divergences: { type: 'array', items: { type: 'string' } }, notes: { type: 'string' } }, required: ['equivalent', 'severity', 'divergences', 'notes'] }

let portRecords = []
let baseMap = {}

// ---------------------------------------------------------------------------
// STEP 1 — FAITHFUL PORT + VERIFY
// ---------------------------------------------------------------------------
if (slice.kind === 'test') {
  phase('Baseline')
  const BASELINE_SCHEMA = { type: 'object', additionalProperties: false, properties: { overall: { type: 'object', additionalProperties: false, properties: { tests: { type: 'integer' }, failures: { type: 'integer' }, errors: { type: 'integer' }, skipped: { type: 'integer' } }, required: ['tests', 'failures', 'errors', 'skipped'] }, classes: { type: 'array', items: { type: 'object', additionalProperties: false, properties: { fqcn: { type: 'string' }, tests: { type: 'integer' }, failures: { type: 'integer' }, errors: { type: 'integer' }, skipped: { type: 'integer' } }, required: ['fqcn', 'tests', 'failures', 'errors', 'skipped'] } } }, required: ['overall', 'classes'] }
  const baseline = await agent(
    `Capture the PRE-PORT (pure-Java) test baseline for ${slice.module}. ${GG}\nRun ./gradlew ${slice.testTask} --console=plain (no clean; fine if some fail — capture exact outcome). Parse all TEST-*.xml into per-class {fqcn,tests,failures,errors,skipped} + overall. Write ${OUT}/baseline.json, return it.`,
    { label: 'baseline:java-suite', phase: 'Baseline', schema: BASELINE_SCHEMA })
  for (const c of (baseline && baseline.classes) || []) baseMap[c.fqcn] = c
  log(`Java baseline: ${baseline && baseline.overall && baseline.overall.tests} tests / ${(baseline && baseline.classes || []).length} classes.`)

  phase('Port')
  const CONVERT_SCHEMA = { type: 'object', additionalProperties: false, properties: { kotlinPath: { type: 'string' }, fqcn: { type: 'string' }, isRunnableTest: { type: 'boolean' }, compiles: { type: 'boolean' }, notes: { type: 'string' } }, required: ['kotlinPath', 'fqcn', 'isRunnableTest', 'compiles', 'notes'] }
  const RUN_SCHEMA = { type: 'object', additionalProperties: false, properties: { ran: { type: 'boolean' }, tests: { type: 'integer' }, failures: { type: 'integer' }, errors: { type: 'integer' }, skipped: { type: 'integer' }, equivalent: EQUIV_SCHEMA.properties.equivalent, severity: EQUIV_SCHEMA.properties.severity, divergences: EQUIV_SCHEMA.properties.divergences, notes: { type: 'string' } }, required: ['ran', 'tests', 'failures', 'errors', 'skipped', 'equivalent', 'severity', 'divergences', 'notes'] }

  for (let i = 0; i < files.length; i++) {                  // sequential — slower but safer
    const f = files[i]; const fc = fqcn(f.path)
    log(`[${i + 1}/${files.length}] ${base(f.path)}`)
    const conv = await agent(
      `Faithfully convert ONE Java test/fixture file to Kotlin (slice ${sliceName}). NOTE: ${slice.note}\nFile: ${f.path}\nExpected fqcn: ${fc}\nRead it; write <BaseName>.kt in the SAME dir; then: mv "${f.path}" "${f.path}.old". Verify incremental compile (./gradlew ${slice.module}:compileTestKotlin ${slice.module}:compileTestJava --console=plain, no clean) and fix compile errors you introduced. isRunnableTest=false for abstract bases/fixtures/helpers with no @Test.\n\n${INTEROP_RULES}\n\n${FAITHFUL_BAR}${depsHint}`,
      { label: `port:${base(f.path)}`, phase: 'Port', schema: CONVERT_SCHEMA }).catch(() => null)
    if (!conv) { portRecords.push({ file: f.path, status: 'convert-error', flagged: true }); continue }
    if (!conv.isRunnableTest) { portRecords.push({ file: f.path, kotlinPath: conv.kotlinPath, fqcn: conv.fqcn, kind: 'support', compiles: conv.compiles, flagged: !conv.compiles, notes: conv.notes }); continue }
    const bl = baseMap[conv.fqcn] || baseMap[fc] || null
    const run = await agent(
      `${conv.fqcn} was just converted to Kotlin. Run ONLY that class and compare to its Java baseline. ${GG}\n1. ./gradlew ${slice.testTask} --tests "${conv.fqcn}" --console=plain (no clean); parse its TEST-*.xml.\n2. Java baseline for this class: ${bl ? JSON.stringify(bl) : 'NOT FOUND (note this)'}. Same count? same pass/fail?\n3. Read ${f.path}.old vs the .kt: same assertions (none dropped/weakened)? List divergences.\nPolicy: KEEP the Kotlin + .java.old regardless; record the verdict (severity high if counts/outcome diverge or assertions lost).`,
      { label: `verify:${base(f.path)}`, phase: 'Port', schema: RUN_SCHEMA }).catch(() => null)
    const matches = !!(run && bl && run.tests === bl.tests && (run.failures + run.errors) === (bl.failures + bl.errors))
    portRecords.push({ file: f.path, kotlinPath: conv.kotlinPath, fqcn: conv.fqcn, kind: 'test', compiles: conv.compiles, baseline: bl, after: run && { tests: run.tests, failures: run.failures, errors: run.errors, skipped: run.skipped }, matchesBaseline: matches, equivalent: run && run.equivalent, severity: run && run.severity, divergences: run && run.divergences, notes: run && run.notes, flagged: !matches || (run && run.equivalent !== 'yes') })
  }

  phase('Verify')
  let agg
  if (slice.fast) {                                          // catalog: cheap to re-run the whole suite
    agg = await agent(
      `Confirm the WHOLE ${slice.module} test suite (now Kotlin) reproduces the Java baseline. ${GG}\n./gradlew ${slice.testTask} --console=plain; compare overall + per-class to ${OUT}/baseline.json. Report any class whose count or pass/fail changed.`,
      { label: 'verify:full-suite', phase: 'Verify', schema: { type: 'object', additionalProperties: false, properties: { tests: { type: 'integer' }, matchesBaseline: { type: 'boolean' }, regressions: { type: 'array', items: { type: 'string' } }, notes: { type: 'string' } }, required: ['tests', 'matchesBaseline', 'regressions', 'notes'] } }).catch(() => null)
  } else {                                                   // trino: per-class runs already covered every class — aggregate, no redundant full run
    const ts = portRecords.filter(r => r.kind === 'test')
    const regressions = ts.filter(r => !r.matchesBaseline || r.equivalent !== 'yes').map(r => r.fqcn)
    agg = { tests: ts.reduce((s, r) => s + ((r.after && r.after.tests) || 0), 0), matchesBaseline: regressions.length === 0, regressions, notes: 'aggregated from per-class before/after runs (no redundant full-suite run for the slow trino slice)' }
  }
  log(`Verify: matches=${agg && agg.matchesBaseline} regressions=${agg && (agg.regressions || []).length}`)
  portRecords.fullVerify = agg

} else {
  // ---- MAIN SLICE: shared port + equivalence stages ----
  const PORT_SCHEMA = { type: 'object', additionalProperties: false, properties: { kotlinPath: { type: 'string' }, publicApiChanged: { type: 'boolean' }, apiCompatNotes: { type: 'string' }, risks: { type: 'array', items: { type: 'string' } } }, required: ['kotlinPath', 'publicApiChanged', 'apiCompatNotes', 'risks'] }
  const portStage = f => agent(
    `Faithfully port ONE Java source file to Kotlin (slice ${sliceName}). NOTE: ${slice.note}\nFile: ${f.path}\nWrite <BaseName>.kt in the SAME dir, then: mv "${f.path}" "${f.path}.old". Don't delete anything; don't touch /generated/. Read neighbours/callers/interfaces as needed. Do NOT run Gradle (a later step compiles).\n\n${INTEROP_RULES}\n\n${FAITHFUL_BAR}${depsHint}`,
    { label: `port:${base(f.path)}`, phase: 'Port', schema: PORT_SCHEMA }).then(r => ({ file: f.path, port: r })).catch(() => ({ file: f.path, port: null }))
  const equivStage = ({ file, port }) => {
    if (!port) return Promise.resolve({ file, status: 'port-error', flagged: true })
    return agent(`Equivalence review (report-only). Read ${file}.old and ${port.kotlinPath}; confirm behavior-identical Kotlin (logic, branches, error handling, nullability, numeric/overflow, ordering). List divergences. Do NOT edit.`,
      { label: `equiv:${base(file)}`, phase: 'Port', schema: EQUIV_SCHEMA })
      .then(v => ({ file, kotlinPath: port.kotlinPath, publicApiChanged: port.publicApiChanged, apiCompatNotes: port.apiCompatNotes, risks: port.risks, equivalent: v.equivalent, severity: v.severity, divergences: v.divergences, flagged: v.equivalent !== 'yes' || port.publicApiChanged }))
      .catch(() => ({ file, kotlinPath: port.kotlinPath, status: 'equiv-error', flagged: true }))
  }

  if (!slice.twoTier) {                                      // catalog-main: port all in parallel, full suite verify (cheap)
    phase('Port')
    portRecords = (await pipeline(files, portStage, equivStage)).filter(Boolean)
    log(`Ported ${portRecords.length} files; equivalence-flagged=${portRecords.filter(r => r.flagged).length}`)
    phase('Verify')
    portRecords.fullVerify = await agent(
      `${slice.module} main source just ported to Kotlin (originals alongside as *.java.old). Compile + run the already-Kotlin test suite (oracle). ${GG}\n1. ./gradlew ${slice.module}:compileKotlin ${slice.module}:compileJava --console=plain. Fix MINOR port-caused issues (missing @Jvm*, nullability, import); record each. No large refactors.\n2. ./gradlew ${slice.testTask} --console=plain — should stay green. Regressions: minor fix or add to unresolved[] and continue (don't revert; .java.old stays).`,
      { label: 'verify:suite', phase: 'Verify', schema: { type: 'object', additionalProperties: false, properties: { compiles: { type: 'boolean' }, suiteGreen: { type: 'boolean' }, minorFixesApplied: { type: 'array', items: { type: 'string' } }, unresolved: { type: 'array', items: { type: 'string' } }, notes: { type: 'string' } }, required: ['compiles', 'suiteGreen', 'minorFixesApplied', 'unresolved', 'notes'] } }).catch(() => null)
    log(`Verify: compiles=${portRecords.fullVerify && portRecords.fullVerify.compiles} green=${portRecords.fullVerify && portRecords.fullVerify.suiteGreen}`)

  } else {                                                   // trino-main: small batch + full; big one-at-a-time + targeted+full
    const small = files.filter(f => f.loc < SMALL_LOC)
    const big = files.filter(f => f.loc >= SMALL_LOC).sort((a, b) => b.loc - a.loc)
    log(`trino-main two-tier: ${small.length} small (batched) + ${big.length} big (one at a time, full verify each).`)

    phase('Port')
    const smallPorted = (await pipeline(small, portStage, equivStage)).filter(Boolean)   // small value/state types together
    portRecords.push(...smallPorted)
    phase('Verify')
    const smallVerify = await agent(
      `Just ported ${small.length} SMALL main files (records / state / value objects) to Kotlin as a batch (originals as *.java.old). Compile + run the FULL ${slice.module} test suite once. ${GG}\n./gradlew ${slice.module}:compileKotlin ${slice.module}:compileJava ${slice.testTask} --console=plain. Fix MINOR port-caused issues; record. Regressions beyond minor -> unresolved[], continue.`,
      { label: 'verify:small-batch', phase: 'Verify', schema: { type: 'object', additionalProperties: false, properties: { compiles: { type: 'boolean' }, suiteGreen: { type: 'boolean' }, minorFixesApplied: { type: 'array', items: { type: 'string' } }, unresolved: { type: 'array', items: { type: 'string' } }, notes: { type: 'string' } }, required: ['compiles', 'suiteGreen', 'minorFixesApplied', 'unresolved', 'notes'] } }).catch(() => null)
    log(`Small-batch verify: compiles=${smallVerify && smallVerify.compiles} green=${smallVerify && smallVerify.suiteGreen}`)

    const bigVerifs = []
    for (let i = 0; i < big.length; i++) {                   // big files: one at a time, full verify after each
      const f = big[i]
      log(`[big ${i + 1}/${big.length}] ${base(f.path)} (${f.loc} loc)`)
      const rec = await equivStage(await portStage(f))
      portRecords.push(rec)
      const v = await agent(
        `Just ported the big main file ${base(f.path)} to Kotlin (original at ${f.path}.old). Verify per file (NOT per method). ${GG}\n` +
        `1. Compile: ./gradlew ${slice.module}:compileKotlin ${slice.module}:compileJava --console=plain; fix MINOR port-caused issues, record them.\n` +
        `2. FAST signal — run this file's own test class if one exists (convention Test${stem(f.path)}): ./gradlew ${slice.testTask} --tests "*.Test${stem(f.path)}" --console=plain (it's fine if no test matches — note it).\n` +
        `3. SAFETY — then run the FULL suite: ./gradlew ${slice.testTask} --console=plain. Report regressions; minor fix or add to unresolved[] and continue (don't revert).`,
        { label: `verify:${base(f.path)}`, phase: 'Verify', schema: { type: 'object', additionalProperties: false, properties: { compiles: { type: 'boolean' }, targetedTestFound: { type: 'boolean' }, suiteGreen: { type: 'boolean' }, regressions: { type: 'array', items: { type: 'string' } }, minorFixesApplied: { type: 'array', items: { type: 'string' } }, notes: { type: 'string' } }, required: ['compiles', 'targetedTestFound', 'suiteGreen', 'regressions', 'minorFixesApplied', 'notes'] } }).catch(() => null)
      bigVerifs.push({ file: f.path, verify: v })
    }
    portRecords.fullVerify = { smallBatch: smallVerify, bigFiles: bigVerifs }
    log(`Ported ${portRecords.length} files; equivalence-flagged=${portRecords.filter(r => r.flagged).length}`)
  }
}

const portedKt = portRecords.filter(r => r.kotlinPath).map(r => r.kotlinPath)
if (stopAfter === 'port') {
  return { slice: sliceName, kind: slice.kind, stoppedAfter: 'port', outputDir: OUT, ported: portRecords, flagged: portRecords.filter(r => r.flagged), fullVerify: portRecords.fullVerify, note: 'Faithful translation done + verified. Stopped before idiom kit.' }
}

// ---------------------------------------------------------------------------
// STEP 2 — BUILD KIT -> APPLY -> RE-VERIFY
// ---------------------------------------------------------------------------
phase('Kit')
const KIT_SCHEMA = { type: 'object', additionalProperties: false, properties: { status: { type: 'string', enum: ['created', 'extended', 'unchanged'] }, kitSource: { type: 'string' }, helpers: { type: 'array', items: { type: 'object', additionalProperties: false, properties: { name: { type: 'string' }, signature: { type: 'string' }, replaces: { type: 'string' }, targetFiles: { type: 'array', items: { type: 'string' } } }, required: ['name', 'signature', 'replaces', 'targetFiles'] } }, summary: { type: 'string' } }, required: ['status', 'kitSource', 'helpers', 'summary'] }
const kit = await agent(
  `Build or EXTEND the shared Kotlin idiom kit FROM THE JUST-PORTED Kotlin in this slice (${sliceName}) — lift the repetitive ceremony you can now SEE into a small set of shared helpers so the logic reads cleanly.\nPorted Kotlin files:\n${portedKt.map(p => '  - ' + p).join('\n')}\n\nIf ${IDIOM_KIT} exists, READ + EXTEND it (status="extended"; reuse, don't duplicate). Else create it. Grep the ported files for high-frequency patterns (jOOQ fetchOne(idx, Foo.class) / DSL.field(.., Foo.class), JDBC try/finally runners, builder ceremony, repeated null-handling). Favour reified inline helpers that erase .class tokens + extensions wrapping runners. Keep SMALL (only patterns recurring across MANY files); each helper Java-interop-safe; don't get silly. List targetFiles per helper. Write ${IDIOM_KIT}, rationale to ${IDIOM_DOC}, append candidates to ${KIT_NOTES} (json array; create if absent). Return.`,
  { label: 'kit:build', phase: 'Kit', schema: KIT_SCHEMA })
log(`Idiom kit ${kit.status}: ${kit.helpers.length} helpers`)
if (stopAfter === 'kit' || kit.status === 'unchanged' || kit.helpers.length === 0) {
  return { slice: sliceName, kind: slice.kind, stoppedAfter: 'kit', outputDir: OUT, ported: portRecords, fullVerify: portRecords.fullVerify, kit: { path: IDIOM_KIT, doc: IDIOM_DOC, status: kit.status, helpers: kit.helpers }, note: 'Kit built but NOT applied. Review it, then re-run without stopAfter.' }
}

phase('Apply')
const applyTargets = [...new Set(portedKt)]   // kit helpers can apply anywhere; apply agents skip spots that don't fit
const APPLY_SCHEMA = { type: 'object', additionalProperties: false, properties: { edited: { type: 'array', items: { type: 'string' } }, helpersApplied: { type: 'array', items: { type: 'string' } }, notes: { type: 'string' } }, required: ['edited', 'helpersApplied', 'notes'] }
await parallel(chunk(applyTargets, 5).map((b, i) => () => agent(
  `Apply the shared idiom kit (${IDIOM_KIT}) to these already-ported, already-passing Kotlin files — replace hand-rolled instances of a kit-covered pattern with the helper. BEHAVIOR-PRESERVING only; if a spot doesn't cleanly fit, leave it. Before editing each file, copy it to ${OUT}/prekit/ (recoverable pre-kit version). Don't change public API shape or touch /generated/.\n${b.map(p => '  - ' + p).join('\n')}\n\nKit helpers + where they apply:\n${JSON.stringify(kit.helpers).slice(0, 6000)}`,
  { label: `apply:batch-${i + 1}`, phase: 'Apply', schema: APPLY_SCHEMA }).catch(() => null)))

phase('ReVerify')
let reverify
if (slice.kind === 'test') {
  if (slice.fast) {
    reverify = await agent(`The idiom kit was applied to the Kotlin tests in ${slice.module}. Re-run the suite, confirm STILL matching the Java baseline. ${GG}\n./gradlew ${slice.testTask} --console=plain; compare to ${OUT}/baseline.json. Minor kit fixes only, else flag (pre-kit copies in ${OUT}/prekit/).`,
      { label: 'reverify:suite', phase: 'ReVerify', schema: { type: 'object', additionalProperties: false, properties: { matchesBaseline: { type: 'boolean' }, regressions: { type: 'array', items: { type: 'string' } }, minorFixesApplied: { type: 'array', items: { type: 'string' } }, notes: { type: 'string' } }, required: ['matchesBaseline', 'regressions', 'minorFixesApplied', 'notes'] } }).catch(() => null)
  } else {                                                   // trino-test: re-run only the classes the kit touched
    const touched = portRecords.filter(r => r.kind === 'test' && applyTargets.includes(r.kotlinPath)).map(r => r.fqcn).filter(Boolean)
    reverify = await agent(`The idiom kit was applied to ${touched.length} Kotlin test classes in ${slice.module}. Re-run ONLY those (the full trino suite is slow), confirm each still matches its Java baseline in ${OUT}/baseline.json. ${GG}\n./gradlew ${slice.testTask} ${touched.map(t => `--tests "${t}"`).join(' ')} --console=plain. Report regressions; minor kit fixes only, else flag (pre-kit copies in ${OUT}/prekit/).`,
      { label: 'reverify:touched', phase: 'ReVerify', schema: { type: 'object', additionalProperties: false, properties: { matchesBaseline: { type: 'boolean' }, regressions: { type: 'array', items: { type: 'string' } }, minorFixesApplied: { type: 'array', items: { type: 'string' } }, notes: { type: 'string' } }, required: ['matchesBaseline', 'regressions', 'minorFixesApplied', 'notes'] } }).catch(() => null)
  }
} else {
  reverify = await agent(`The idiom kit was applied to the Kotlin main source in ${slice.module}. Re-compile + re-run the suite, confirm green. ${GG}\n./gradlew ${slice.module}:compileKotlin ${slice.module}:compileJava ${slice.testTask} --console=plain. Report regressions; minor kit fixes only, else flag (pre-kit copies in ${OUT}/prekit/).`,
    { label: 'reverify:suite', phase: 'ReVerify', schema: { type: 'object', additionalProperties: false, properties: { compiles: { type: 'boolean' }, suiteGreen: { type: 'boolean' }, regressions: { type: 'array', items: { type: 'string' } }, minorFixesApplied: { type: 'array', items: { type: 'string' } }, notes: { type: 'string' } }, required: ['compiles', 'suiteGreen', 'regressions', 'minorFixesApplied', 'notes'] } }).catch(() => null)
}
log(`ReVerify done.`)

const nextIdx = ORDER.indexOf(sliceName)
const nextSlice = nextIdx >= 0 && nextIdx < ORDER.length - 1 ? ORDER[nextIdx + 1] : 'combine'
log(`Slice ${sliceName} complete. Next: ${nextSlice}`)
return {
  slice: sliceName, kind: slice.kind, outputDir: OUT,
  ported: portRecords, flagged: portRecords.filter(r => r.flagged), portVerify: portRecords.fullVerify,
  kit: { path: IDIOM_KIT, doc: IDIOM_DOC, status: kit.status, helpers: kit.helpers }, appliedTo: applyTargets.length, reverify, nextSlice,
  note: 'Two verified steps: faithful port then idiom-kit apply. .java.old kept alongside; pre-kit .kt copies in <slice>/prekit/. Run slice="combine" after all slices.',
}
