export const meta = {
  name: 'adversarial-code-review',
  description: 'Adversarial code review of the DuckLake JVM modules — find correctness + efficiency issues, then refute each finding with an independent skeptic',
  whenToUse: 'Nitpick the ducklake-catalog + trino-ducklake Java code for correctness bugs, inefficiency, and sloppiness. Each candidate finding is independently challenged before it survives.',
  phases: [
    { title: 'Map', detail: 'discover the non-test Java source files and their sizes' },
    { title: 'Review', detail: 'one reviewer per substantial file + batched reviewers for small files' },
    { title: 'Verify', detail: 'an adversarial skeptic tries to refute each correctness/efficiency finding' },
  ],
}

const base = p => p.split('/').pop()

// ---- Phase 0: discover the files to review (self-contained, no args needed) ----
// args.roots can override the directories scanned; defaults to both core modules.
const roots = (args && Array.isArray(args.roots) && args.roots.length)
  ? args.roots
  : [
      '/Users/jminard/DEV/brikk/repos/ducklake-integrations/jvm/ducklake-catalog/src',
      '/Users/jminard/DEV/brikk/repos/ducklake-integrations/jvm/trino-ducklake/src',
    ]

phase('Map')

// Pre-extract this project's third-party dependency SOURCE jars into ONE merged tree so
// reviewers can read the library interfaces that matter (Trino SPI, DuckDB, Arrow, airlift)
// without ever scanning the filesystem. These are compileOnly/runtime deps — their sources
// live in the gradle cache, not the repo — so we idempotently extract to a gitignored dir
// (re-extracts after `gradle clean`). This is the cure for agents wasting minutes on
// unbounded `find` whenever they want a new jar.
const DEPS_REF = '/Users/jminard/DEV/brikk/repos/ducklake-integrations/jvm/trino-ducklake/build/deps-reference'
const DEPS_SCHEMA = {
  type: 'object', additionalProperties: false,
  properties: { available: { type: 'boolean' }, path: { type: 'string' }, javaFiles: { type: 'integer' }, note: { type: 'string' } },
  required: ['available', 'path', 'javaFiles', 'note'],
}
const GRADLE_DIR = '/Users/jminard/DEV/brikk/repos/ducklake-integrations/jvm'
const deps = await agent(
  `Ensure both modules' dependency SOURCE jars are extracted into one merged reference tree for the reviewers.\n` +
  `Target dir: ${DEPS_REF}\n\n` +
  `IDEMPOTENT: if ${DEPS_REF}/io/trino/spi/connector/ConnectorMetadata.java AND ` +
  `${DEPS_REF}/org/apache/arrow/vector/VectorSchemaRoot.java already exist, do nothing — just count and report.\n\n` +
  `Otherwise, drive extraction off the AUTHORITATIVE per-project compile dependency trees (this auto-corrects ` +
  `versions on bumps and covers both modules; do NOT hardcode versions). We only need source for the ` +
  `libraries the review actually cares about — Trino, DuckDB, Arrow, airlift — NOT jOOQ/Hikari/Guava. ` +
  `Run exactly this as BASH (not zsh):\n\n` +
  `  cd ${GRADLE_DIR}\n` +
  `  ./gradlew ducklake-catalog:printCompileDependencyTree trino-ducklake:printCompileDependencyTree \\\n` +
  `    --console=plain -q 2>/dev/null > /tmp/dl-deps.txt\n` +
  `  mkdir -p ${DEPS_REF}\n` +
  `  # Path-based: pull every gradle-cache jar path, keep the API groups reviewers actually read,\n` +
  `  # dedupe to version dirs, extract each artifact's -sources.jar into the one merged tree.\n` +
  `  GROUPS='io\\.trino|org\\.apache\\.arrow|io\\.airlift|org\\.duckdb'\n` +
  `  grep -oE '/Users/[^ ]*/modules-2/files-2.1/[^ ]+\\.jar' /tmp/dl-deps.txt \\\n` +
  `   | grep -E "/files-2.1/(\$GROUPS)/" \\\n` +
  `   | while read jar; do dirname "$(dirname "$jar")"; done | sort -u \\\n` +
  `   | while read vdir; do\n` +
  `       src=$(find "$vdir" -name '*-sources.jar' 2>/dev/null | head -1)\n` +
  `       if [ -n "$src" ]; then (cd ${DEPS_REF} && unzip -oq "$src" 'io/*' 'org/*' 'com/*' >/dev/null 2>&1); fi\n` +
  `     done\n\n` +
  `NOTE: unzip exits non-zero when a jar lacks one of the io/org/com prefixes — that is NOT a failure, so do ` +
  `not treat it as one. Some artifacts have no -sources.jar cached; skipping them is fine.\n` +
  `Then report javaFiles = $(find ${DEPS_REF} -name '*.java' | wc -l) and available=true if ConnectorMetadata.java exists.`,
  { label: 'map:deps-source', phase: 'Map', model: 'sonnet', schema: DEPS_SCHEMA },
)
const DEPS_HINT = deps && deps.available
  ? `\n\nDEPENDENCY SOURCE — READ, DON'T HUNT:\n` +
    `All third-party library sources you might need are ALREADY extracted into one local tree:\n` +
    `  ${DEPS_REF}\n` +
    `It contains the full source (interfaces + Javadoc) for the compile-classpath versions of the libraries ` +
    `that matter here: Trino SPI / parquet / plugin-toolkit / filesystem / memory-context / hive / orc / ` +
    `metastore / cache, DuckDB JDBC, Apache Arrow (vector, memory-core, c-data, format), and airlift ` +
    `(slice, units, ...). jOOQ/Hikari/Guava are deliberately not included — their usage is not under review.\n` +
    `To inspect a library class FooBar, do ONE of:\n` +
    `  find ${DEPS_REF} -name 'FooBar.java'        # locate\n` +
    `  grep -rl 'interface FooBar' ${DEPS_REF}     # or by declaration\n` +
    `This is instant. HARD RULE: NEVER run find/grep over $HOME, /, or ~/.gradle for a jar or class — those ` +
    `scans take minutes and are forbidden. If a class is not under ${DEPS_REF}, do NOT go searching the ` +
    `filesystem for it: reason from the calling code and the signatures you can already see. At most, you may ` +
    `run exactly ONE narrowly-scoped lookup: find ~/.gradle/caches/modules-2 -name '<artifact>-*-sources.jar' ` +
    `(scoped to that dir, returns fast). Do not broaden it.`
  : `\n\nDEPENDENCY SOURCE: not pre-extracted this run. Do NOT scan $HOME, /, or ~/.gradle for jars — those ` +
    `scans waste minutes. Reason from the calling code and visible signatures instead.`

const MANIFEST_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    files: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          path: { type: 'string', description: 'absolute path' },
          loc: { type: 'integer', description: 'line count' },
        },
        required: ['path', 'loc'],
      },
    },
  },
  required: ['files'],
}
const mapResult = await agent(
  `List every non-test Java source file under these roots and its line count:\n${roots.map(r => '  - ' + r).join('\n')}\n\n` +
  `Run a shell command such as:\n` +
  `  for r in ${roots.join(' ')}; do find "$r" -name '*.java' -not -path '*test*'; done | while read f; do echo "$(wc -l < "$f") $f"; done\n\n` +
  `Exclude anything under a directory containing "test". Return one entry per file with its absolute path and integer line count via the structured output tool. Do not review the files — only enumerate them.`,
  { label: 'map:discover', phase: 'Map', model: 'sonnet', schema: MANIFEST_SCHEMA },
)
const files = ((mapResult && mapResult.files) || []).filter(f => f && f.path && !/test/i.test(f.path))
if (files.length === 0) { log('No source files discovered — aborting.'); return { error: 'no files discovered', roots } }

// Substantial files (>=120 LOC) get a dedicated reviewer on the inherited (Opus) model
// where the real logic lives; tiny boilerplate files are batched and reviewed on Sonnet.
const big = files.filter(f => f.loc >= 120).sort((a, b) => b.loc - a.loc)
const small = files.filter(f => f.loc < 120).sort((a, b) => b.loc - a.loc)
const smallBatches = []
for (let i = 0; i < small.length; i += 5) smallBatches.push(small.slice(i, i + 5))

const work = [
  ...big.map(f => ({ kind: 'file', model: undefined, paths: [f.path], loc: f.loc, label: base(f.path) })),
  ...smallBatches.map((b, i) => ({ kind: 'batch', model: 'sonnet', paths: b.map(x => x.path), label: `batch-${i + 1}` })),
]
log(`Discovered ${files.length} files: ${big.length} substantial (individual) + ${small.length} small (${smallBatches.length} batches)`)

// ---- Brief: distill project intent so reviewers don't flag intentional choices as bugs,
// and so they actively check our code against the reference implementations. Runs once;
// the digest is injected into every reviewer + verifier prompt. ----
const REPO = '/Users/jminard/DEV/brikk/repos/ducklake-integrations'
const BRIEF_SCHEMA = {
  type: 'object', additionalProperties: false,
  properties: {
    intentionalDecisions: {
      type: 'array', description: 'documented deliberate choices a reviewer might wrongly flag as a bug',
      items: { type: 'object', additionalProperties: false,
        properties: { topic: { type: 'string' }, summary: { type: 'string' } }, required: ['topic', 'summary'] },
    },
    knownGaps: {
      type: 'array', description: 'documented TODOs / known-incomplete areas — not bugs to report',
      items: { type: 'object', additionalProperties: false,
        properties: { topic: { type: 'string' }, summary: { type: 'string' } }, required: ['topic', 'summary'] },
    },
    upstreamBugWatch: {
      type: 'array', description: 'bugs/edge-cases reported in the reference impls worth checking in our code',
      items: { type: 'object', additionalProperties: false,
        properties: { topic: { type: 'string' }, checkInOurCode: { type: 'string' } }, required: ['topic', 'checkInOurCode'] },
    },
    conventions: { type: 'array', items: { type: 'string' }, description: 'project-specific idioms a reviewer should respect' },
  },
  required: ['intentionalDecisions', 'knownGaps', 'upstreamBugWatch', 'conventions'],
}
const brief = await agent(
  `Build a concise reviewer briefing for an adversarial code review of the DuckLake JVM connector. ` +
  `Your output drives ~64 downstream reviewers, so be accurate and BOUNDED (cap each list at ~12 of the most decision-relevant items, one or two sentences each).\n\n` +
  `READ these (use Read/Grep; skim, don't exhaustively read megabytes):\n` +
  `  - ${REPO}/README.md and ${REPO}/jvm/trino-ducklake/README.md (overall design — fairly complete)\n` +
  `  - ${REPO}/jvm/trino-ducklake/dev-docs/*.md and ${REPO}/jvm/trino-ducklake/dev-docs/archive/*.md\n` +
  `    (DESIGN-/REPORT-/RESEARCH- docs record INTENTIONAL edge-case decisions e.g. unicode, temporal\n` +
  `     partition encoding, datetime/timezone, hash null handling; TODO-* docs record KNOWN GAPS)\n` +
  `  - Skim the reference implementations for divergences and reported bugs:\n` +
  `      ${REPO}/vendor/pg_ducklake (Postgres DuckLake reference — README/CLAUDE.md/docs)\n` +
  `      ${REPO}/vendor/ducklake/docs (the DuckDB ducklake extension, the spec reference impl)\n\n` +
  `Produce four lists via the structured tool:\n` +
  `  1. intentionalDecisions — deliberate choices documented in dev-docs that a reviewer might wrongly flag as a bug.\n` +
  `  2. knownGaps — documented TODO/known-incomplete areas; reviewers should NOT report these as findings.\n` +
  `  3. upstreamBugWatch — specific bugs/edge-cases the reference impls got wrong or had to fix, that are worth\n` +
  `     checking we handle correctly in our code.\n` +
  `  4. conventions — project-specific idioms/patterns reviewers should respect rather than 'correct'.`,
  { label: 'brief:distill', phase: 'Map', schema: BRIEF_SCHEMA },
)
function renderBrief(b) {
  if (!b) return ''
  const sec = (title, items, fmt) => items && items.length ? `\n${title}:\n${items.map(fmt).map(s => '  - ' + s).join('\n')}` : ''
  return `\n\nPROJECT BRIEFING (from README + dev-docs + reference impls — TREAT AS AUTHORITATIVE):` +
    sec('INTENTIONAL DECISIONS — do NOT report these as bugs', b.intentionalDecisions, x => `${x.topic}: ${x.summary}`) +
    sec('KNOWN GAPS — documented, do NOT report as findings', b.knownGaps, x => `${x.topic}: ${x.summary}`) +
    sec('UPSTREAM BUG WATCH — actively check our code handles these', b.upstreamBugWatch, x => `${x.topic} — ${x.checkInOurCode}`) +
    sec('CONVENTIONS — respect these', b.conventions, x => x)
}
const BRIEF_HINT = renderBrief(brief)

const PROJECT_CONTEXT = `
PROJECT CONTEXT — you are reviewing a JVM implementation of the DuckLake lakehouse catalog:
- ducklake-catalog: shared Java library. JDBC-based metadata access against the DuckLake catalog
  DB, connection pooling, write transactions, optimistic-concurrency conflict detection,
  type conversion, partition computation.
- trino-ducklake: a Trino connector plugin built on that library. Read + write, DDL/DML,
  row-level DELETE/UPDATE/MERGE, ALTER TABLE, partitioned writes, time travel, metadata tables,
  and predicate pushdown executed via an embedded/served DuckDB engine ("Quack").
This is production code that has been built over ~1 month. The author wants a strict nitpick:
where has it gotten sloppy, inefficient, or silly?
`.trim()

const REVIEW_FOCUS = `
PRIMARY focus (report these aggressively):
- CORRECTNESS: logic errors, wrong edge-case handling, off-by-one, null hazards, swallowed
  exceptions, incorrect type/encoding conversion, time-zone / temporal mistakes, sign/overflow
  on unsigned ranges, concurrency races, resource leaks (unclosed JDBC Connection/Statement/
  ResultSet, DuckDB handles, Arrow allocators, file streams), transaction/commit-ordering bugs,
  conflict-detection gaps, mis-built SQL (injection, wrong quoting, wrong predicate pushdown).
- EFFICIENCY: N+1 JDBC queries / per-row round trips that should be batched, repeated work that
  could be hoisted or cached, needless full-collection materialization where streaming works,
  quadratic loops, redundant re-parsing/re-serialization, missing prepared-statement reuse,
  per-split or per-page allocations that could be shared, fetching more columns/rows than needed.

WHERE THE RISK LIVES — concentrate at these library boundaries (this is what we worry about):
- TRINO SPI: do we honour the connector contract correctly (call ordering, nullability, threading,
  handle/transaction lifecycle, what each method is allowed to return)?
- DUCKDB: are we driving the engine correctly — SQL we generate, result/Arrow stream handling,
  closing statements/result sets/connections, session/attach state?
- DUCKLAKE semantics: does our catalog implementation match the DuckLake spec / reference behaviour
  (snapshots, transaction & commit ordering, conflict detection, metadata-schema reads/writes)?
- ARROW: ONLY memory safety — allocator/buffer/VectorSchemaRoot lifecycle, leaks (unclosed
  allocators or vectors), use-after-close, reading or referencing buffers outside their valid scope,
  ownership/transfer mistakes. Do NOT critique general Arrow API usage or style.
NOT A CONCERN — do not scrutinise how we use jOOQ, HikariCP/connection pooling mechanics, or Guava;
we are comfortable with those. Only flag them if they directly cause one of the PRIMARY bugs above
(e.g. a leaked pooled connection), never as "you could use library X better".

SECONDARY (note briefly, lower severity): dead code, copy-paste duplication, misleading names,
leftover TODO/FIXME, needlessly complex control flow, inconsistent idioms. Do NOT report pure
formatting or large-file/large-method size on its own — the author explicitly does not care about
those unless they hide a real bug.

OUT OF SCOPE — NEVER REPORT: dependency versions, version conflicts / skew between modules,
transitive-dependency bloat, classpath issues, and build / Gradle / version-catalog configuration.
The author manages dependencies deliberately and does not want any agent raising them. The extracted
library source under the deps reference dir exists ONLY so you can READ how a library behaves to judge
the connector's code — it is not an invitation to critique dependency choices. Judge the Java source
in this repo, nothing about the build or its dependencies.

Be a skeptic of the code, not of yourself: if something looks wrong, report it. But every finding
must point to a SPECIFIC line range and explain the concrete consequence. No vague "consider"
advice, no praise, no restating what the code does. If a file is genuinely clean, return an empty
findings array — that is a perfectly good and expected outcome.

You MAY and SHOULD read neighbouring files, callers, and the interfaces a class implements (use
Read/Grep) when that is needed to judge whether something is actually a bug.
`.trim()

const FINDINGS_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    findings: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          file: { type: 'string', description: 'absolute path of the file the issue is in' },
          lines: { type: 'string', description: 'line or range, e.g. "412-430"' },
          category: { type: 'string', enum: ['correctness', 'efficiency', 'sloppiness', 'design'] },
          severity: { type: 'string', enum: ['high', 'medium', 'low'] },
          title: { type: 'string', description: 'one-line summary of the issue' },
          detail: { type: 'string', description: 'what is wrong and the concrete consequence' },
          suggestion: { type: 'string', description: 'specific fix' },
        },
        required: ['file', 'lines', 'category', 'severity', 'title', 'detail', 'suggestion'],
      },
    },
  },
  required: ['findings'],
}

const VERDICT_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    verdict: { type: 'string', enum: ['confirmed', 'refuted', 'uncertain'] },
    confidence: { type: 'string', enum: ['high', 'medium', 'low'] },
    correctedSeverity: { type: 'string', enum: ['high', 'medium', 'low'] },
    reasoning: { type: 'string', description: 'evidence from the actual code for the verdict' },
  },
  required: ['verdict', 'confidence', 'correctedSeverity', 'reasoning'],
}

function reviewPrompt(item) {
  const fileList = item.paths.map(p => `  - ${p}`).join('\n')
  const scope = item.kind === 'file'
    ? `Review this single file in depth:\n${fileList}`
    : `Review this small batch of files (each is short — be terse, focus only on real issues):\n${fileList}`
  return `${PROJECT_CONTEXT}${DEPS_HINT}${BRIEF_HINT}\n\n${scope}\n\n${REVIEW_FOCUS}\n\nRead the file(s) now and return your findings via the structured output tool.`
}

function verifyPrompt(f) {
  return `${PROJECT_CONTEXT}${DEPS_HINT}${BRIEF_HINT}

You are an ADVERSARIAL VERIFIER. Another reviewer raised the finding below. Your job is to try to
REFUTE it by reading the actual code and its surrounding context — do not take the claim on faith.

FINDING
  file:       ${f.file}
  lines:      ${f.lines}
  category:   ${f.category}
  severity:   ${f.severity}
  title:      ${f.title}
  detail:     ${f.detail}
  suggestion: ${f.suggestion}

Open ${base(f.file)} at the cited lines. Read enough surrounding code, callers, and related
classes (Read/Grep) to judge it independently. Consider: is the claimed consequence real given how
this code is actually invoked? Is there a guard, contract, or framework guarantee elsewhere that
already prevents it? Is the "inefficiency" actually hot, or negligible / intentional? If the
PROJECT BRIEFING above lists this as an INTENTIONAL DECISION or a documented KNOWN GAP, return
"refuted" and say so.

Return:
- verdict "confirmed" only if you independently reproduced the reasoning and it is a real issue.
- verdict "refuted" if the code is actually fine (explain why).
- verdict "uncertain" if you cannot tell from the code alone.
Default toward "refuted" or "uncertain" when the evidence is not clear. Set correctedSeverity to
the severity you'd actually assign (downgrade overstated ones). Cite specific lines in reasoning.`
}

// Only the user's primary concerns get the expensive adversarial gate. Secondary
// sloppiness/design notes pass through unverified (still reported, clearly marked).
const needsVerify = f => f.category === 'correctness' || f.category === 'efficiency'

phase('Review')
log(`Reviewing ${work.length} units (${big.length} files + ${smallBatches.length} small-file batches)`)

const results = await pipeline(
  work,
  // Stage 1: review
  item => agent(reviewPrompt(item), {
    label: `review:${item.label}`,
    phase: 'Review',
    model: item.model,
    schema: FINDINGS_SCHEMA,
  }).then(r => ({ item, findings: (r && r.findings) || [] })),
  // Stage 2: adversarially verify each primary finding; pass secondary ones through
  ({ item, findings }) => {
    const toVerify = findings.filter(needsVerify)
    const passThrough = findings.filter(f => !needsVerify(f))
      .map(f => ({ ...f, verdict: 'unverified', confidence: 'n/a', correctedSeverity: f.severity }))
    if (toVerify.length === 0) return Promise.resolve({ label: item.label, findings: passThrough })
    return parallel(toVerify.map(f => () =>
      agent(verifyPrompt(f), { label: `verify:${base(f.file)}:${f.lines}`, phase: 'Verify', schema: VERDICT_SCHEMA })
        .then(v => ({ ...f, verdict: v.verdict, confidence: v.confidence, correctedSeverity: v.correctedSeverity, verifierReasoning: v.reasoning }))
        .catch(() => ({ ...f, verdict: 'uncertain', confidence: 'low', correctedSeverity: f.severity, verifierReasoning: 'verifier errored' }))
    )).then(verified => ({ label: item.label, findings: [...verified, ...passThrough] }))
  },
)

const all = results.filter(Boolean).flatMap(r => r.findings)
const confirmed = all.filter(f => f.verdict === 'confirmed')
const uncertain = all.filter(f => f.verdict === 'uncertain')
const refuted = all.filter(f => f.verdict === 'refuted')
const secondary = all.filter(f => f.verdict === 'unverified')

const sevRank = { high: 0, medium: 1, low: 2 }
const bySev = arr => [...arr].sort((a, b) => (sevRank[a.correctedSeverity] ?? 3) - (sevRank[b.correctedSeverity] ?? 3))

log(`Done. confirmed=${confirmed.length} uncertain=${uncertain.length} refuted=${refuted.length} secondary=${secondary.length}`)

return {
  summary: {
    unitsReviewed: work.length,
    totalRaw: all.length,
    confirmed: confirmed.length,
    uncertain: uncertain.length,
    refuted: refuted.length,
    secondary: secondary.length,
  },
  confirmed: bySev(confirmed),
  uncertain: bySev(uncertain),
  secondary: bySev(secondary),
  refuted, // kept for transparency / audit
}
