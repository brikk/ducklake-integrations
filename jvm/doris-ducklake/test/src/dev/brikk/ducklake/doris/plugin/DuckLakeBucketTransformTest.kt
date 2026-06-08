package dev.brikk.ducklake.doris.plugin

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Proves our murmur3-based BUCKET transform matches DuckLake's writer exactly —
 * the load-bearing correctness check for bucket pruning. A mismatch would
 * silently drop matching rows, so this pins it against DuckLake's own reference.
 */
internal class DuckLakeBucketTransformTest {

    @Test
    fun matchesDuckLakeStringBucketReference() {
        // From vendor/ducklake/test/sql/partitioning/bucket_partitioning.test:
        //   "murmur3_32 bucket(4): alice->1, bob->2, charlie->3"
        assertThat(DuckLakeBucketTransform.bucket("alice", 4)).isEqualTo(1)
        assertThat(DuckLakeBucketTransform.bucket("bob", 4)).isEqualTo(2)
        assertThat(DuckLakeBucketTransform.bucket("charlie", 4)).isEqualTo(3)
    }

    @Test
    fun bucketIsAlwaysInRange() {
        for (s in listOf("a", "alice", "", "a-longer-value-here", "δ")) {
            val b = DuckLakeBucketTransform.bucket(s, 8)!!
            assertThat(b).isBetween(0, 7)
        }
    }

    @Test
    fun returnsNullForUnbucketableLiteralTypes() {
        // Pruning is best-effort: types we don't encode here yield null so the
        // caller keeps every file rather than guessing a bucket.
        assertThat(DuckLakeBucketTransform.bucket(1.5, 4)).isNull()
        assertThat(DuckLakeBucketTransform.bucket(null, 4)).isNull()
    }
}
