package main.scala

trait SnapshotManagement { self: DeltaLog =>

  @volatile protected var lastUpdateTimestamp: Long = -1L
  @volatile protected var currentSnapshot: Snapshot = getSnapshotAtInit

  def snapshot: Snapshot = currentSnapshot

  def update(): Snapshot = {
    null
  }

  def getSnapshotAt(
    version: Long,
    commitTimestamp: Option[Long] = None,
    lastCheckpointHint: Option[CheckpointInstance] = None): Snapshot = {
    null
  }

  protected def getSnapshotAtInit: Snapshot = {
    null
  }

  protected def getLogSegmentFrom(
    startingCheckpoint: Option[CheckpointMetaData]): LogSegment = {
    getLogSegmentForVersion(startingCheckpoint.map(_.version))
  }
}
