package main.scala

import main.scala.storage.LogStoreProvider
import main.scala.util.Clock
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * Used to query the current state of the log as well as modify it by adding
 * new atomic collections of actions.
 *
 * Internally, this class implements an optimistic concurrency control
 * algorithm to handle multiple readers or writers. Any single read
 * is guaranteed to see a consistent snapshot of the table.
 */
class DeltaLog private(
    val logPath: Path,
    val dataPath: Path,
    val clock: Clock)
  extends Checkpoints
  // with MetadataCleanup
  with LogStoreProvider
  // with ReadChecksum
  with SnapshotManagement {
  import DeltaLog._
  val store = createLogStore(hadoopConf)
}

object DeltaLog {
  private val hadoopConf = new Configuration()
}
