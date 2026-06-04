export const meta = {
  name: 'kotlin-idiom-review',
  description: 'Review & clean up the ported Kotlin for idiomatic Kotlin + collapse tiny files (NO functional change). Plan-only by default; apply is opt-in.',
  whenToUse: 'After the Java→Kotlin port is complete and green. Improves kotlin-ness only: idioms, nullability, interop-annotation removal, file collapse. Never changes behavior.',
  phases: [
    { title: 'Map' },
    { title: 'Review' },
    { title: 'Collapse' },
    { title: 'Plan' },
    { title: 'Apply' },
  ],
}

// ---------------------------------------------------------------------------
// CONTROLS (args)
//   args.scope     : 'main' (default) | 'test' | 'all'   — which source sets to review
//   args.module    : 'catalog' | 'trino' | 'both' (default)
//   args.apply     : false (default) — when true, applies ONLY 'safe' items serially with
//                    gradle verify between groups. Plan-only otherwise.
//   args.planPath  : where the plan is written / read. Default below.
// This workflow is QUALITY ONLY. It must not change behavior. Every apply is gated by
// the green baseline (catalog 33/108, trino ~76/853 — read from on-disk XML, never clean).
// ---------------------------------------------------------------------------

const scope = (args && args.scope) || 'main'
const moduleArg = (args && args.module) || 'both'
const doApply = !!(args && args.apply)
const PLAN_PATH = (args && args.planPath) || '.ai/kotlin-port/IDIOM-CLEANUP-PLAN.md'

const JVM = 'jvm'
const SRC_DIRS = []
if (moduleArg === 'catalog' || moduleArg === 'both') {
  if (scope === 'main' || scope === 'all') SRC_DIRS.push('ducklake-catalog/src')
  if (scope === 'test' || scope === 'all') SRC_DIRS.push('ducklake-catalog/test', 'ducklake-catalog/testFixtures')
}
if (moduleArg === 'trino' || moduleArg === 'both') {
  if (scope === 'main' || scope === 'all') SRC_DIRS.push('trino-ducklake/src')
  if (scope === 'test' || scope === 'all') SRC_DIRS.push('trino-ducklake/test')
}

const KOTLIN_GUIDANCE = `
You are reviewing KOTLIN that was mechanically ported from Java. Goal: make it idiomatic
Kotlin WITHOUT changing behavior. This is a QUALITY pass, not a bug hunt and not a feature
change. Output is a list of concrete, mechanical edit proposals — never vague advice.

Idiom dimensions to look for (each finding cites exact line + current snippet + proposed snippet):
1. Nullability: Optional<T> → T? on INTERNAL paths. RISK: a return type of an SPI override, or
   a field that is Jackson-serialized (@JsonProperty / @JsonCreator) or crosses the wire, MUST
   stay Optional — mark those severity:"skip" with reason. When unsure, "review".
2. Interop annotations: @JvmStatic / @JvmField / @JvmName / @JvmOverloads are only needed for
   JAVA callers. Everything is Kotlin now EXCEPT generated/ (jOOQ, still Java). Propose dropping
   an annotation only if you can confirm no Java caller (generated/ rarely calls our types, but
   say so). Mark "review" if a Java caller might exist.
3. Value/state types: should be 'data class' with 'val' primary-constructor properties; use
   destructuring where it reads well; expression-body functions; 'object' for stateless singletons.
4. Control flow: 'when' over if/else chains; single-expression bodies; early return; elvis '?:'.
5. Scope functions: let/run/apply/also/with where they genuinely improve readability — do NOT
   sprinkle them for their own sake ("idiomatic where it helps readability, don't get silly").
6. Collections: map/filter/associate/groupBy/first/single/buildList over manual loops where
   clearer; keep behavior identical (watch lazy vs eager, null handling, ordering).
7. Visibility: tighten public → internal/private now that callers are all Kotlin (mark "review"
   if the symbol is part of the Trino SPI surface or a public catalog API).
8. Strings: templates, trimIndent, buildString.
9. requireNonNull / Objects.requireNonNull → Kotlin null-checks or just non-null types.
10. java.util factories (List.of/Map.of/Set.of/Collections.empty*) → listOf/mapOf/setOf/emptyList…
    where the immutability/semantics match.

HARD RULES:
- Do NOT propose anything that changes behavior, public binary signatures consumed by the Trino
  SPI, or Jackson serialization shape. When a change is behavior-adjacent, severity:"review".
- Do NOT touch generated/ (jOOQ codegen) — it stays Java.
- Read the file fully before proposing. Cite real line numbers.
- Prefer fewer, high-confidence "safe" proposals over many speculative ones.
`

const MAP_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['files', 'interopFiles', 'optionalSites'],
  properties: {
    files: {
      type: 'array',
      items: {
        type: 'object', additionalProperties: false,
        required: ['path', 'loc', 'sizeClass', 'classification'],
        properties: {
          path: { type: 'string', description: 'repo-relative path from jvm/, e.g. ducklake-catalog/src/.../Foo.kt' },
          loc: { type: 'integer' },
          sizeClass: { enum: ['small', 'medium', 'large'] }, // small <60, medium 60-250, large >250
          classification: { enum: ['value-type', 'handle', 'enum', 'exception', 'service', 'util', 'factory', 'test', 'other'] },
        },
      },
    },
    interopFiles: {
      type: 'array',
      items: {
        type: 'object', additionalProperties: false,
        required: ['path', 'annotations', 'safeToDropAll', 'note'],
        properties: {
          path: { type: 'string' },
          annotations: { type: 'array', items: { type: 'string' } },
          safeToDropAll: { type: 'boolean', description: 'true only if NO Java caller (incl generated/) references the annotated members' },
          note: { type: 'string' },
        },
      },
    },
    optionalSites: {
      type: 'array',
      items: {
        type: 'object', additionalProperties: false,
        required: ['path', 'count', 'anySpiOrJackson'],
        properties: {
          path: { type: 'string' },
          count: { type: 'integer' },
          anySpiOrJackson: { type: 'boolean', description: 'true if any Optional here is an SPI override return or Jackson-serialized field (must stay)' },
        },
      },
    },
  },
}

const REVIEW_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['file', 'findings'],
  properties: {
    file: { type: 'string' },
    findings: {
      type: 'array',
      items: {
        type: 'object', additionalProperties: false,
        required: ['kind', 'line', 'severity', 'current', 'proposed', 'rationale'],
        properties: {
          kind: { enum: ['nullability', 'interop-annotation', 'data-class', 'control-flow', 'scope-function', 'collections', 'visibility', 'strings', 'null-check', 'java-factory', 'other'] },
          line: { type: 'integer' },
          severity: { enum: ['safe', 'review', 'skip'], description: 'safe=mechanical no-behavior-risk; review=needs human eyes; skip=looks tempting but must NOT change' },
          current: { type: 'string', description: 'exact current snippet' },
          proposed: { type: 'string', description: 'exact replacement snippet' },
          rationale: { type: 'string' },
          interopSensitive: { type: 'boolean' },
        },
      },
    },
  },
}

const COLLAPSE_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['groups', 'keepSeparate'],
  properties: {
    groups: {
      type: 'array',
      items: {
        type: 'object', additionalProperties: false,
        required: ['newFileName', 'packageDir', 'module', 'members', 'rationale', 'risk'],
        properties: {
          newFileName: { type: 'string', description: 'e.g. CatalogExceptions.kt' },
          packageDir: { type: 'string', description: 'dir the new file lives in (same package as members)' },
          module: { enum: ['catalog', 'trino'] },
          members: {
            type: 'array',
            items: {
              type: 'object', additionalProperties: false,
              required: ['fromFile', 'declaration'],
              properties: {
                fromFile: { type: 'string' },
                declaration: { type: 'string', description: 'the top-level declaration moved, e.g. "class DucklakeException", "enum class DucklakeNullOrder"' },
              },
            },
          },
          rationale: { type: 'string' },
          risk: { enum: ['safe', 'review'], description: 'safe: same-package class move (FQN unchanged, source-compatible). review: members are top-level funcs/props (file-facade name changes) or public-API relocation worth a human glance' },
        },
      },
    },
    keepSeparate: {
      type: 'array',
      items: {
        type: 'object', additionalProperties: false,
        required: ['file', 'reason'],
        properties: { file: { type: 'string' }, reason: { type: 'string' } },
      },
    },
  },
}

// =========================================================================
// PHASE 1 — MAP
// =========================================================================
phase('Map')
log(`Scope=${scope} module=${moduleArg} dirs=[${SRC_DIRS.join(', ')}] apply=${doApply}`)

const mapResult = await agent(`
Inventory the ported Kotlin source for an idiom-cleanup pass. Work from ${JVM}/ as cwd.
Cover exactly these source dirs (relative to ${JVM}/): ${SRC_DIRS.map(d => '`' + d + '`').join(', ')}.

Run REAL commands (do not guess; a fan-out "no matches" is a hypothesis, not a finding):
- List every *.kt under those dirs (exclude anything under generated/ and *.java.old). For each,
  get LOC ('wc -l') and bucket sizeClass: small <60, medium 60-250, large >250.
- Classify each file by its primary top-level declaration: value-type (data/record-like),
  handle (Trino *Handle/*Split/*TableHandle), enum, exception, service (has logic/IO), util,
  factory, test, or other.
- For interop: 'grep -rln' for @JvmStatic/@JvmField/@JvmName/@JvmOverloads in those dirs. For each
  hit, check whether any JAVA file references the annotated member (generated/ is jOOQ — note if it
  could). Set safeToDropAll only when you are confident no Java caller exists.
- For Optional: 'grep -rln "Optional<"' in those dirs; for each file, count and decide
  anySpiOrJackson (any Optional that is an SPI override return type OR a @JsonProperty/@JsonCreator
  field — those must stay Optional).

Return the structured inventory. Paths must be relative to ${JVM}/ (start with the module dir).
`, { phase: 'Map', schema: MAP_SCHEMA })

const files = (mapResult && mapResult.files || []).filter(f => f && f.path && !f.path.includes('/generated/'))
log(`Map: ${files.length} .kt files · ${files.filter(f => f.sizeClass === 'small').length} small · ${(mapResult.interopFiles || []).length} interop · ${(mapResult.optionalSites || []).length} Optional sites`)

// =========================================================================
// PHASE 2 — REVIEW (fan-out, read-only)
//   large → 1 agent each; medium → pairs; small → groups of 6.
// =========================================================================
phase('Review')

const large = files.filter(f => f.sizeClass === 'large').map(f => [f.path])
const medium = chunk(files.filter(f => f.sizeClass === 'medium').map(f => f.path), 2)
const small = chunk(files.filter(f => f.sizeClass === 'small').map(f => f.path), 6)
const reviewBatches = [...large, ...medium, ...small].filter(b => b.length)
log(`Review: ${reviewBatches.length} batches over ${files.length} files`)

const reviewResults = (await parallel(reviewBatches.map((batch, i) => () =>
  agent(`
${KOTLIN_GUIDANCE}

Review these file(s) for idiomatic Kotlin (cwd ${JVM}/). READ-ONLY — propose, do not edit:
${batch.map(p => '- ' + p).join('\n')}

If multiple files, return findings for each (set "file" per finding-block; if the schema only
holds one file, return the file with the most findings and mention the others in rationale).
Prefer high-confidence "safe" proposals. Cite exact line numbers and exact snippets.
`, { label: `review:${batch[0].split('/').pop()}${batch.length > 1 ? `+${batch.length - 1}` : ''}`, phase: 'Review', schema: REVIEW_SCHEMA })
))).filter(Boolean)

const totalFindings = reviewResults.reduce((n, r) => n + (r.findings ? r.findings.length : 0), 0)
const safeFindings = reviewResults.reduce((n, r) => n + (r.findings ? r.findings.filter(f => f.severity === 'safe').length : 0), 0)
log(`Review: ${totalFindings} findings (${safeFindings} safe) across ${reviewResults.length} batches`)

// =========================================================================
// PHASE 3 — COLLAPSE (one agent, global view of small + value/enum/exception types)
// =========================================================================
phase('Collapse')

const collapseCandidates = files.filter(f =>
  f.sizeClass === 'small' &&
  ['value-type', 'enum', 'exception'].includes(f.classification))

const collapseResult = await agent(`
${KOTLIN_GUIDANCE}

Propose collapsing TINY, related top-level declarations into shared themed files to reduce
file sprawl (cwd ${JVM}/). Kotlin allows many top-level declarations per file.

Candidates (small value-type/enum/exception files — read each before grouping):
${collapseCandidates.map(f => `- ${f.path} (${f.loc} LOC, ${f.classification})`).join('\n')}

Group ONLY genuinely cohesive sets in the SAME package, e.g.:
- catalog exceptions → CatalogExceptions.kt
- sort types (sort key / null order / direction) → SortTypes.kt
- partition value/field types → PartitionTypes.kt
Rules:
- Members of one group MUST share a package dir. Same-package class/enum/object moves keep their
  fully-qualified name (source- & binary-compatible) → risk "safe".
- If a member is a top-level FUNCTION or PROPERTY (file-facade class name would change) or a
  public API relocation worth a glance → risk "review".
- Do NOT group things that aren't conceptually siblings just because they're small. List anything
  you'd keep separate in keepSeparate with a one-line reason.
- Aim for a handful of high-value merges, not maximal consolidation.
`, { phase: 'Collapse', schema: COLLAPSE_SCHEMA })

log(`Collapse: ${(collapseResult.groups || []).length} merge groups proposed, ${(collapseResult.keepSeparate || []).length} kept separate`)

// =========================================================================
// PHASE 4 — PLAN (write to disk, STOP here by default)
// =========================================================================
phase('Plan')

const planAgent = await agent(`
You are writing the idiom-cleanup PLAN to disk at \`${PLAN_PATH}\` (cwd repo root; you have Write).

This is a QUALITY-ONLY plan (no behavior change). Synthesize the inputs below into an ordered,
conflict-aware, human-reviewable plan. Group ALL edits BY FILE (so one file is touched once),
and order so file-collapse moves and in-file idiom edits to the same file don't fight.

Structure the markdown:
1. Summary: counts (files reviewed, findings by severity, collapse groups).
2. "Apply order" — a numbered list of work units. Each unit = one file (or one collapse group),
   listing its edits with severity tags. Safe-only items first, then a "Needs review" section.
3. "Collapse groups" — each new file, its members (from→declaration), risk, and the deletes.
4. "Explicitly skipped" — Optional-must-stay (SPI/Jackson), interop kept for Java callers, etc.,
   each with the reason (so nobody re-proposes them).
5. "Verification" — the green baseline to preserve (read on-disk TEST-*.xml, never gradle clean):
   catalog 33 suites/108 tests/0-0-0; trino ~76 suites/853 tests/0-0-0. Apply is serial,
   per-unit, gradle-verified.

INPUTS (JSON):
REVIEW_FINDINGS = ${JSON.stringify(reviewResults).slice(0, 100000)}
COLLAPSE = ${JSON.stringify(collapseResult)}
INTEROP = ${JSON.stringify(mapResult.interopFiles || [])}
OPTIONAL = ${JSON.stringify(mapResult.optionalSites || [])}

After writing the file, return a 1-paragraph summary + the path.
`, { phase: 'Plan' })

if (!doApply) {
  log('Plan written. Stopping (plan-only). Re-run with args.apply=true to apply SAFE items serially.')
  return {
    status: 'plan-ready',
    planPath: PLAN_PATH,
    filesReviewed: files.length,
    findings: { total: totalFindings, safe: safeFindings },
    collapseGroups: (collapseResult.groups || []).length,
    note: planAgent,
  }
}

// =========================================================================
// PHASE 5 — APPLY (opt-in, SERIAL, safe-only, gradle-verified between units)
// =========================================================================
phase('Apply')
log('APPLY mode: applying SAFE items only, serially, with gradle verify between units. Never clean.')

// Build per-file safe-edit units from review findings + safe collapse groups.
const safeByFile = {}
for (const r of reviewResults) {
  const safe = (r.findings || []).filter(f => f.severity === 'safe')
  if (safe.length) safeByFile[r.file] = (safeByFile[r.file] || []).concat(safe)
}
const fileUnits = Object.entries(safeByFile).map(([file, findings]) => ({ kind: 'edits', file, findings }))
const collapseUnits = (collapseResult.groups || []).filter(g => g.risk === 'safe').map(g => ({ kind: 'collapse', group: g }))
const units = [...collapseUnits, ...fileUnits] // collapses first, then in-file idioms

log(`Apply: ${units.length} safe units (${collapseUnits.length} collapses + ${fileUnits.length} file-edit sets). Serial.`)

const applied = []
for (let i = 0; i < units.length; i++) {
  const u = units[i]
  const desc = u.kind === 'collapse' ? `collapse→${u.group.newFileName}` : `edits:${u.file.split('/').pop()}`
  const moduleOf = (p) => p.includes('ducklake-catalog') ? 'ducklake-catalog' : 'trino-ducklake'
  const mod = u.kind === 'collapse' ? (u.group.module === 'catalog' ? 'ducklake-catalog' : 'trino-ducklake') : moduleOf(u.file)

  const res = await agent(`
QUALITY-ONLY apply, unit ${i + 1}/${units.length}: ${desc} (cwd ${JVM}/). Behavior must NOT change.

${u.kind === 'collapse'
  ? `Collapse group → create \`${u.group.packageDir}/${u.group.newFileName}\` containing these members,
then DELETE the now-empty source files. Preserve each declaration verbatim (imports, KDoc, visibility,
annotations). Members:\n${u.group.members.map(m => `- ${m.declaration}  (from ${m.fromFile})`).join('\n')}`
  : `Apply ONLY these pre-approved SAFE edits to \`${u.file}\`. Use exact-match replacements; if a
snippet no longer matches (file drifted), SKIP that one and report it — never force it:
${u.findings.map(f => `- L${f.line} [${f.kind}]: \`${f.current}\` → \`${f.proposed}\``).join('\n')}`}

Then verify: from ${JVM}/ run \`./gradlew :${mod}:compileKotlin :${mod}:compileTestKotlin\` and, if it
compiles, \`./gradlew :${mod}:test\`. NEVER 'gradle clean'. Read PASS/FAIL from on-disk
${mod}/build/test-results/test/TEST-*.xml (failures+errors must be 0), not from stdout.
Report: applied list, skipped (with reason), and compile/test result (suite/test/fail/err counts).
If tests regress, REVERT this unit's changes (git checkout -- the touched files) and report the failure.
`, { label: desc, phase: 'Apply' })
  applied.push({ unit: desc, result: res })
  log(`Apply ${i + 1}/${units.length}: ${desc} done`)
}

return {
  status: 'applied',
  planPath: PLAN_PATH,
  unitsApplied: applied.length,
  details: applied,
  note: 'Safe-only applied serially. "Needs review" items remain in the plan for human follow-up. Commit per-unit or as one cleanup commit; then run the .java.old sweep if still pending.',
}

// ---------------------------------------------------------------------------
function chunk(arr, n) {
  const out = []
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n))
  return out
}
