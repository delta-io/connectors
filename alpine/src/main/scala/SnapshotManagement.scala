package main.scala

import java.io.FileNotFoundException

import exception.DeltaErrors
import main.scala.util.FileNames._
import org.apache.hadoop.fs.{FileStatus, Path}

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

  protected def getLogSegmentForVersion(startCheckpoint: Option[Long]): LogSegment = {
    val newFiles = store.listFrom(checkpointPrefix(logPath, startCheckpoint.getOrElse(0L)))
      .filter { file => isCheckpointFile(file.getPath) || isDeltaFile(file.getPath) }
      .filterNot { file => isCheckpointFile(file.getPath) && file.getLen == 0 }
      .toArray

    if (newFiles.isEmpty && startCheckpoint.isEmpty) {
      throw DeltaErrors.emptyDirectoryException(logPath.toString)
    } else if (newFiles.isEmpty) {
      // The directory may be deleted and recreated and we may have stale state in our DeltaLog
      // singleton, so try listing from the first version
      return getLogSegmentForVersion(None)
    }
    val (checkpoints, deltas) = newFiles.partition(f => isCheckpointFile(f.getPath))

    // Find the latest checkpoint in the listing that is not older than the versionToLoad
    val lastChkpoint = CheckpointInstance.MaxValue
    val checkpointFiles = checkpoints.map(f => CheckpointInstance(f.getPath))
    val newCheckpoint = getLatestCompleteCheckpointFromList(checkpointFiles, lastChkpoint)
    if (newCheckpoint.isDefined) {
      // If there is a new checkpoint, start new lineage there.
      val newCheckpointVersion = newCheckpoint.get.version
      val newCheckpointPaths = newCheckpoint.get.getCorrespondingFiles(logPath).toSet

      val deltasAfterCheckpoint = deltas.filter { file =>
        deltaVersion(file.getPath) > newCheckpointVersion
      }
      val deltaVersions = deltasAfterCheckpoint.map(f => deltaVersion(f.getPath))

      // We may just be getting a checkpoint file after the filtering
      if (deltaVersions.nonEmpty) {
        verifyDeltaVersions(deltaVersions)
        require(deltaVersions.head == newCheckpointVersion + 1, "Did not get the first delta " +
          s"file version: ${newCheckpointVersion + 1} to compute Snapshot")
      }
      val newVersion = deltaVersions.lastOption.getOrElse(newCheckpoint.get.version)
      val newCheckpointFiles = checkpoints.filter(f => newCheckpointPaths.contains(f.getPath))
      assert(newCheckpointFiles.length == newCheckpointPaths.size,
        "Failed in getting the file information for:\n" +
          newCheckpointPaths.mkString(" -", "\n -", "") + "\n" +
          "among\n" + checkpoints.map(_.getPath).mkString(" -", "\n -", ""))

      // In the case where `deltasAfterCheckpoint` is empty, `deltas` should still not be empty,
      // they may just be before the checkpoint version unless we have a bug in log cleanup
      val lastCommitTimestamp = deltas.last.getModificationTime

      LogSegment(
        logPath,
        newVersion,
        deltasAfterCheckpoint,
        newCheckpointFiles,
        newCheckpoint.map(_.version),
        lastCommitTimestamp)
    } else {
      // No starting checkpoint found. This means that we should definitely have version 0, or the
      // last checkpoint we thought should exist (the `_last_checkpoint` file) no longer exists
      if (startCheckpoint.isDefined) {
        throw DeltaErrors.missingPartFilesException(
          startCheckpoint.get, new FileNotFoundException(
            s"Checkpoint file to load version: ${startCheckpoint.get} is missing."))
      }

      val deltaVersions = deltas.map(f => deltaVersion(f.getPath))
      verifyDeltaVersions(deltaVersions)
      if (deltaVersions.head != 0) {
        throw DeltaErrors.logFileNotFoundException(
          deltaFile(logPath, 0L), deltaVersions.last)
      }

      val latestCommit = deltas.last

      LogSegment(
        logPath,
        deltaVersion(latestCommit.getPath), // deltas is not empty, so can call .last
        deltas,
        Nil,
        None,
        latestCommit.getModificationTime)
    }
  }

  protected def getSnapshotAtInit: Snapshot = {
    val logSegment = getLogSegmentForVersion(lastCheckpoint.map(_.version))

    lastUpdateTimestamp = clock.getTimeMillis()

    new Snapshot(
      logPath,
      logSegment.version,
      logSegment,
      minFileRetentionTimestamp,
      this,
      logSegment.lastCommitTimestamp)
  }

  protected def verifyDeltaVersions(versions: Array[Long]): Unit = {
    // Turn this to a vector so that we can compare it with a range.
    val deltaVersions = versions.toVector
    if (deltaVersions.nonEmpty && (deltaVersions.head to deltaVersions.last) != deltaVersions) {
      throw DeltaErrors.deltaVersionsNotContiguousException(deltaVersions)
    }
  }
}

case class LogSegment(
    logPath: Path,
    version: Long,
    deltas: Seq[FileStatus],
    checkpoints: Seq[FileStatus],
    checkpointVersion: Option[Long],
    lastCommitTimestamp: Long)