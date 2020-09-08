package util

import scala.collection.mutable.ArrayBuffer

//import org.apache.spark.sql.delta.Snapshot

//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{Dataset, SparkSession}
//import org.apache.spark.sql.execution.LogicalRDD
//import org.apache.spark.storage.StorageLevel

/**
 * Machinary that caches the reconstructed state of a Delta table
 * using the RDD cache. The cache is designed so that the first access
 * will materialize the results.  However once uncache is called,
 * all data will be flushed and will not be cached again.
 */
trait StateCache {
  protected def spark: SparkSession

  /** If state RDDs for this snapshot should still be cached. */
  private var isCached = true
  /** A list of RDDs that we need to uncache when we are done with this snapshot. */
  private val cached = ArrayBuffer[RDD[_]]()

  class CachedDS[A](ds: Dataset[A], name: String) {
    // While we cache RDD to avoid re-computation in different spark sessions, `Dataset` can only be
    // reused by the session that created it to avoid session pollution. So we use `DatasetRefCache`
    // to re-create a new `Dataset` when the active session is changed. This is an optimization for
    // single-session scenarios to avoid the overhead of `Dataset` creation which can take 100ms.
    private val cachedDs = cached.synchronized {
      if (isCached) {
        val rdd = ds.queryExecution.toRdd.map(_.copy())
        rdd.setName(name)
        rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
        cached += rdd
        val dsCache = new DatasetRefCache(() => {
          Dataset.ofRows(
            spark,
            LogicalRDD(
              ds.queryExecution.analyzed.output,
              rdd)(
              spark)).as[A](ds.exprEnc)
        })
        Some(dsCache)
      } else {
        None
      }
    }

    /**
     * Get the DS from the cache.
     *
     * If a RDD cache is available,
     * - return the cached DS if called from the same session in which the cached DS is created, or
     * - reconstruct the DS using the RDD cache if called from a different session.
     *
     * If no RDD cache is available,
     * - return a copy of the original DS with updated spark session.
     *
     * Since a cached DeltaLog can be accessed from multiple Spark sessions, this interface makes
     * sure that the original Spark session in the cached DS does not leak into the current active
     * sessions.
     */
    def getDS: Dataset[A] = {
      if (cached.synchronized(isCached) && cachedDs.isDefined) {
        cachedDs.get.get
      } else {
        Dataset.ofRows(
          spark,
          ds.queryExecution.logical
        ).as[A](ds.exprEnc)
      }
    }
  }

  /**
   * Create a CachedDS instance for the given Dataset and the name.
   */
  def cacheDS[A](ds: Dataset[A], name: String): CachedDS[A] = {
    new CachedDS[A](ds, name)
  }

  /** Drop any cached data for this [[Snapshot]]. */
  def uncache(): Unit = cached.synchronized {
    if (isCached) {
      isCached = false
      cached.foreach(_.unpersist(blocking = false))
    }
  }
}
