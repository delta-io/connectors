package main.scala

import main.scala.storage.LogStoreProvider
import main.scala.util.Clock
import org.apache.hadoop.fs.Path

class DeltaLog private(
    val logPath: Path,
    val dataPath: Path,
    val clock: Clock)
  extends Checkpoints
  // with MetadataCleanup
  with LogStoreProvider
  // with SnapshotManagement
  with ReadChecksum {
}
