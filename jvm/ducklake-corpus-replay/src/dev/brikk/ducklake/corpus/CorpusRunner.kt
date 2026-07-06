package dev.brikk.ducklake.corpus

import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.extension
import kotlin.io.path.readText
import kotlin.io.path.relativeTo

/**
 * Discovers and replays corpus files, aggregating a [CorpusReport].
 *
 * The corpus root is the `test/sql` directory of the pinned `duckdb/ducklake`
 * submodule (system property `ducklake.corpus.root` in tests). [skipList]
 * carries per-file skips with reasons — the curation surface, seeded over time
 * from upstream's own per-backend test-config skip lists (test/configs).
 */
class CorpusRunner(
    private val corpusRoot: Path,
    private val skipList: Map<String, String> = emptyMap(),
    private val engine: ReplayReadEngine? = null,
    /** See [ReplayDriver]'s `metadataRewriter` — the backend axis. */
    private val metadataRewriter: ((String) -> String)? = null,
) {

    fun discover(subdir: String? = null): List<Path> {
        val base = if (subdir == null) corpusRoot else corpusRoot.resolve(subdir)
        if (!Files.isDirectory(base)) return emptyList()
        Files.walk(base).use { stream ->
            return stream
                .filter { Files.isRegularFile(it) && it.extension == "test" }
                .sorted()
                .toList()
        }
    }

    fun run(files: List<Path>): CorpusReport {
        // corpusRoot = <repo>/test/sql → repo root two levels up (for `data/` refs).
        val driver = ReplayDriver(engine, repoRoot = corpusRoot.parent?.parent, metadataRewriter = metadataRewriter)
        val results = mutableListOf<FileResult>()
        for (file in files) {
            val rel = file.relativeTo(corpusRoot).toString()
            val curated = skipList[rel]
            if (curated != null) {
                results += FileResult(rel, "skip-list: $curated", emptyList())
                continue
            }
            val parsed = SltParser.parse(rel, file.readText())
            results +=
                runCatching { driver.replay(parsed) }
                    .getOrElse { e ->
                        FileResult(rel, "CRASH: ${e.message?.lineSequence()?.firstOrNull() ?: e}", emptyList())
                    }
        }
        return CorpusReport(results)
    }
}

class CorpusReport(val files: List<FileResult>) {

    val ranFiles: List<FileResult> get() = files.filter { it.fileSkipReason == null }
    val skippedFiles: List<FileResult> get() = files.filter { it.fileSkipReason != null }
    val failures: List<Pair<FileResult, RecordOutcome.Fail>>
        get() = ranFiles.flatMap { f -> f.failed.map { f to it } }

    /** Files whose replay threw (harness bug or driver escape) — always a defect, never a skip. */
    val crashes: List<FileResult> get() = skippedFiles.filter { it.fileSkipReason!!.startsWith("CRASH:") }

    val totalPassed: Int get() = ranFiles.sumOf { it.passed }
    val totalRecordSkips: Int get() = ranFiles.sumOf { it.skipped.size }

    fun summary(maxFailures: Int = 20): String {
        val sb = StringBuilder()
        sb.appendLine("corpus replay: ${files.size} files")
        sb.appendLine(
            "  ran ${ranFiles.size} (records: $totalPassed passed, ${failures.size} failed, $totalRecordSkips skipped)",
        )
        sb.appendLine("  file-skips ${skippedFiles.size}:")
        skippedFiles
            .groupBy { it.fileSkipReason!!.substringBefore(" at line") }
            .entries
            .sortedByDescending { it.value.size }
            .forEach { (reason, fs) -> sb.appendLine("    ${fs.size}\t$reason") }
        if (failures.isNotEmpty()) {
            sb.appendLine("  failures (first $maxFailures):")
            failures.take(maxFailures).forEach { (f, fail) ->
                val line = fail.record?.line ?: 0
                val reasonLines = fail.reason.lines()
                sb.appendLine("    ${f.path}:$line  ${reasonLines.first()}")
                reasonLines.drop(1).forEach { sb.appendLine("      $it") }
            }
        }
        return sb.toString()
    }
}
