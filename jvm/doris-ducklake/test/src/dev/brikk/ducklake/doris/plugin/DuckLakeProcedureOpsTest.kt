package dev.brikk.ducklake.doris.plugin

import java.lang.reflect.Proxy
import java.time.Instant

import dev.brikk.ducklake.catalog.DucklakeCatalog
import dev.brikk.ducklake.catalog.ExpireSnapshotsResult
import dev.brikk.ducklake.catalog.TransactionConflictException

import org.apache.doris.connector.api.DorisConnectorException
import org.apache.doris.connector.api.procedure.ProcedureExecutionMode
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * Pure-logic coverage of [DuckLakeProcedureOps] — the `expire_snapshots` argument contract,
 * duration parsing, retention-floor rejection, dry-run vs. real expiry dispatch, and result
 * shaping. Uses a recording [FakeCatalog] (no Postgres) so the whole file is headless; the
 * required deliverable per the task brief. An integration test that seeds real snapshots is
 * intentionally NOT added — the module has no trivially-reusable Testcontainers helper wired for
 * this path, and the catalog's own suite already covers the SQL-level expiry semantics.
 */
internal class DuckLakeProcedureOpsTest {

    // ---- getSupportedProcedures / getExecutionMode ----

    @Test
    fun advertisesExpireSnapshotsAsSingleCall() {
        val ops = DuckLakeProcedureOps(FakeCatalog(), emptyMap())
        assertThat(ops.getSupportedProcedures()).contains("expire_snapshots")
        assertThat(ops.getExecutionMode("expire_snapshots")).isEqualTo(ProcedureExecutionMode.SINGLE_CALL)
    }

    @Test
    fun rejectsUnknownProcedure() {
        val ops = DuckLakeProcedureOps(FakeCatalog(), emptyMap())
        assertThatThrownBy { ops.execute(null, null, "rewrite_data_files", emptyMap(), null, emptyList()) }
            .isInstanceOf(DorisConnectorException::class.java)
            .hasMessageContaining("does not support procedure")
    }

    // ---- retention mode ----

    @Test
    fun retentionModeListsAndExpiresWithNullVersions() {
        val fake = FakeCatalog(expirable = listOf(1L, 2L, 3L), result = ExpireSnapshotsResult(3, 7))
        val ops = DuckLakeProcedureOps(fake, emptyMap())

        val res = ops.execute(null, null, "expire_snapshots", mapOf("retention_threshold" to "30d"), null, emptyList())

        // retention mode → listExpirableSnapshots(cutoff, null), then expireSnapshots(the ids)
        assertThat(fake.listOlderThan).isNotNull()
        assertThat(fake.listVersions).isNull()
        assertThat(fake.expiredWith).containsExactlyInAnyOrder(1L, 2L, 3L)

        val row = res.rows.single()
        assertThat(res.resultSchema.map { it.name })
            .containsExactly("dry_run", "scope", "expired_snapshot_count", "scheduled_file_count", "snapshot_ids")
        assertThat(row).containsExactly("false", "catalog-wide (ignores the named table)", "3", "7", "1,2,3")
    }

    @Test
    fun defaultRetentionIsSevenDaysWhenNoArgGiven() {
        val fake = FakeCatalog(expirable = listOf(5L), result = ExpireSnapshotsResult(1, 1))
        val ops = DuckLakeProcedureOps(fake, emptyMap())
        val before = Instant.now()

        ops.execute(null, null, "expire_snapshots", emptyMap(), null, emptyList())

        // default 7d → cutoff roughly 7 days ago (allow a generous window for test slowness).
        val cutoff = requireNotNull(fake.listOlderThan)
        val sevenDaysMillis = 7L * 24 * 60 * 60 * 1000
        assertThat(cutoff).isBetween(before.minusMillis(sevenDaysMillis + 60_000), before.minusMillis(sevenDaysMillis - 60_000))
    }

    @Test
    fun retentionBelowFloorIsRejected() {
        // default floor is 7d; a 1h retention must be refused before touching the catalog.
        val fake = FakeCatalog()
        val ops = DuckLakeProcedureOps(fake, emptyMap())
        assertThatThrownBy {
            ops.execute(null, null, "expire_snapshots", mapOf("retention_threshold" to "1h"), null, emptyList())
        }
            .isInstanceOf(DorisConnectorException::class.java)
            .hasMessageContaining("below the minimum")
        assertThat(fake.listOlderThan).isNull()
        assertThat(fake.expiredWith).isNull()
    }

    @Test
    fun configurableFloorFromCatalogProperty() {
        // Lower the floor to 1h via the catalog property; 2h retention now passes.
        val fake = FakeCatalog(expirable = listOf(9L), result = ExpireSnapshotsResult(1, 0))
        val ops = DuckLakeProcedureOps(fake, mapOf("maintenance.min-retention" to "1h"))
        ops.execute(null, null, "expire_snapshots", mapOf("retention_threshold" to "2h"), null, emptyList())
        assertThat(fake.expiredWith).containsExactly(9L)
    }

    // ---- explicit snapshot_ids mode ----

    @Test
    fun explicitSnapshotIdsBypassRetentionFloorAndPassVersions() {
        val fake = FakeCatalog(expirable = listOf(11L, 22L), result = ExpireSnapshotsResult(2, 4))
        val ops = DuckLakeProcedureOps(fake, emptyMap())

        ops.execute(null, null, "expire_snapshots", mapOf("snapshot_ids" to "11, 22"), null, emptyList())

        // explicit mode → listExpirableSnapshots(null, {11,22})
        assertThat(fake.listOlderThan).isNull()
        assertThat(fake.listVersions).containsExactlyInAnyOrder(11L, 22L)
        assertThat(fake.expiredWith).containsExactlyInAnyOrder(11L, 22L)
    }

    @Test
    fun retentionAndSnapshotIdsAreMutuallyExclusive() {
        val ops = DuckLakeProcedureOps(FakeCatalog(), emptyMap())
        assertThatThrownBy {
            ops.execute(
                null, null, "expire_snapshots",
                mapOf("retention_threshold" to "30d", "snapshot_ids" to "1"), null, emptyList(),
            )
        }
            .isInstanceOf(DorisConnectorException::class.java)
            .hasMessageContaining("mutually exclusive")
    }

    @Test
    fun invalidSnapshotIdElementThrows() {
        val ops = DuckLakeProcedureOps(FakeCatalog(), emptyMap())
        assertThatThrownBy {
            ops.execute(null, null, "expire_snapshots", mapOf("snapshot_ids" to "1,two,3"), null, emptyList())
        }
            .isInstanceOf(DorisConnectorException::class.java)
            .hasMessageContaining("expected a bigint")
    }

    // ---- dry run ----

    @Test
    fun dryRunListsButNeverExpires() {
        val fake = FakeCatalog(expirable = listOf(1L, 2L))
        val ops = DuckLakeProcedureOps(fake, emptyMap())

        val res = ops.execute(
            null, null, "expire_snapshots",
            mapOf("retention_threshold" to "30d", "dry_run" to "true"), null, emptyList(),
        )

        assertThat(fake.listOlderThan).isNotNull()
        assertThat(fake.expiredWith).isNull() // NEVER called on a dry run
        val row = res.rows.single()
        assertThat(row).containsExactly("true", "catalog-wide (ignores the named table)", "2", "n/a", "1,2")
    }

    @Test
    fun emptyEligibleSetReportsZeroAndDoesNotExpire() {
        val fake = FakeCatalog(expirable = emptyList())
        val ops = DuckLakeProcedureOps(fake, emptyMap())
        val res = ops.execute(null, null, "expire_snapshots", mapOf("retention_threshold" to "30d"), null, emptyList())
        assertThat(fake.expiredWith).isNull()
        assertThat(res.rows.single()).containsExactly("false", "catalog-wide (ignores the named table)", "0", "n/a", "")
    }

    // ---- case-insensitivity & WHERE/PARTITION rejection ----

    @Test
    fun argumentKeysAreCaseInsensitive() {
        val fake = FakeCatalog(expirable = listOf(1L), result = ExpireSnapshotsResult(1, 0))
        val ops = DuckLakeProcedureOps(fake, emptyMap())
        ops.execute(null, null, "EXPIRE_SNAPSHOTS", mapOf("RETENTION_THRESHOLD" to "30d", "DRY_RUN" to "FALSE"), null, emptyList())
        assertThat(fake.expiredWith).containsExactly(1L)
    }

    @Test
    fun partitionClauseIsRejected() {
        val ops = DuckLakeProcedureOps(FakeCatalog(), emptyMap())
        assertThatThrownBy {
            ops.execute(null, null, "expire_snapshots", emptyMap(), null, listOf("p1"))
        }
            .isInstanceOf(DorisConnectorException::class.java)
            .hasMessageContaining("PARTITION")
    }

    // ---- transaction conflict wrapping ----

    @Test
    fun transactionConflictIsWrapped() {
        val fake = FakeCatalog(expirable = listOf(1L), conflict = true)
        val ops = DuckLakeProcedureOps(fake, emptyMap())
        assertThatThrownBy {
            ops.execute(null, null, "expire_snapshots", mapOf("retention_threshold" to "30d"), null, emptyList())
        }
            .isInstanceOf(DorisConnectorException::class.java)
            .hasMessageContaining("conflicted")
            .cause().isInstanceOf(TransactionConflictException::class.java)
    }

    // ---- duration parser ----

    @Test
    fun parsesValidDurations() {
        assertThat(DuckLakeProcedureOps.parseDuration("60s", "x")).isEqualTo(60_000L)
        assertThat(DuckLakeProcedureOps.parseDuration("30m", "x")).isEqualTo(30L * 60 * 1000)
        assertThat(DuckLakeProcedureOps.parseDuration("24h", "x")).isEqualTo(24L * 60 * 60 * 1000)
        assertThat(DuckLakeProcedureOps.parseDuration("7d", "x")).isEqualTo(7L * 24 * 60 * 60 * 1000)
        assertThat(DuckLakeProcedureOps.parseDuration("  7d  ", "x")).isEqualTo(7L * 24 * 60 * 60 * 1000)
        assertThat(DuckLakeProcedureOps.parseDuration("7 d", "x")).isEqualTo(7L * 24 * 60 * 60 * 1000)
    }

    @Test
    fun rejectsInvalidDurations() {
        for (bad in listOf("", "d", "7", "7x", "7dd", "7 days", "-7d", "abc", "7.5d")) {
            assertThatThrownBy { DuckLakeProcedureOps.parseDuration(bad, "retention_threshold") }
                .describedAs("duration %s", bad)
                .isInstanceOf(DorisConnectorException::class.java)
        }
    }

    @Test
    fun parsesBooleanCaseInsensitively() {
        assertThat(DuckLakeProcedureOps.parseBoolean(null, "x")).isFalse()
        assertThat(DuckLakeProcedureOps.parseBoolean("", "x")).isFalse()
        assertThat(DuckLakeProcedureOps.parseBoolean("TrUe", "x")).isTrue()
        assertThat(DuckLakeProcedureOps.parseBoolean("FALSE", "x")).isFalse()
        assertThatThrownBy { DuckLakeProcedureOps.parseBoolean("yes", "x") }
            .isInstanceOf(DorisConnectorException::class.java)
    }

    /**
     * Recording stub of [DucklakeCatalog]. Only the two methods the procedure calls are
     * implemented; every other method delegates to a throwing [java.lang.reflect.Proxy] so an
     * accidental extra catalog call fails loudly rather than silently returning a default.
     */
    private class FakeCatalog(
        private val expirable: List<Long> = emptyList(),
        private val result: ExpireSnapshotsResult = ExpireSnapshotsResult(0, 0),
        private val conflict: Boolean = false,
    ) : DucklakeCatalog by throwingDelegate() {

        var listOlderThan: Instant? = null
        var listVersions: Set<Long>? = null
        var expiredWith: Set<Long>? = null

        override fun listExpirableSnapshots(olderThan: Instant?, versions: Set<Long>?): List<Long> {
            listOlderThan = olderThan
            listVersions = versions
            return expirable
        }

        override fun expireSnapshots(snapshotIds: Set<Long>): ExpireSnapshotsResult {
            expiredWith = snapshotIds
            if (conflict) {
                throw TransactionConflictException("simulated conflict", null)
            }
            return result
        }

        companion object {
            private fun throwingDelegate(): DucklakeCatalog =
                Proxy.newProxyInstance(
                    DucklakeCatalog::class.java.classLoader,
                    arrayOf(DucklakeCatalog::class.java),
                ) { _, method, _ ->
                    throw NotImplementedError("FakeCatalog does not implement ${method.name}")
                } as DucklakeCatalog
        }
    }
}
