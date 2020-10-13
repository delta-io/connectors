/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.alpine.internal

import collection.JavaConverters._
import java.io.FileNotFoundException
import java.sql.Timestamp

import io.delta.alpine.internal.exception.DeltaErrors
import io.delta.alpine.internal.util.FileNames._
import org.apache.hadoop.fs.{FileStatus, Path}

private[internal] trait SnapshotManagement { self: DeltaLogImpl =>

  @volatile protected var currentSnapshot: SnapshotImpl = getSnapshotAtInit

  def snapshot: SnapshotImpl = currentSnapshot

  def update(): SnapshotImpl = {
    lockInterruptibly {
      updateInternal()
    }
  }

  def getSnapshotForVersionAsOf(version: Long): SnapshotImpl = {
    val historyManager = DeltaHistoryManager(self)
    historyManager.checkVersionExists(version)
    getSnapshotAt(version)
  }

  def getSnapshotForTimestampAsOf(timestamp: Long): SnapshotImpl = {
    val historyManager = DeltaHistoryManager(self)
    val latestCommit = historyManager.getActiveCommitAtTime(new Timestamp(timestamp))
    getSnapshotAt(latestCommit.version)
  }

  private def updateInternal(): SnapshotImpl = {
    try {
      val segment = getLogSegmentForVersion(currentSnapshot.logSegment.checkpointVersion)
      if (segment != currentSnapshot.logSegment) {
        val newSnapshot = createSnapshot(segment, segment.lastCommitTimestamp)
        // TODO: snapshot version error checking
        currentSnapshot = newSnapshot
      }
    } catch {
      case e: FileNotFoundException =>
        if (Option(e.getMessage).exists(_.contains("reconstruct state at version"))) {
          throw e
        }
        currentSnapshot = new InitialSnapshotImpl(hadoopConf, logPath, this)
    }
    currentSnapshot
  }

  /**
   * Get a list of files that can be used to compute a Snapshot at version `versionToLoad`, If
   * `versionToLoad` is not provided, will generate the list of files that are needed to load the
   * latest version of the Delta table. This method also performs checks to ensure that the delta
   * files are contiguous.
   *
   * @param startCheckpoint A potential start version to perform the listing of the DeltaLog,
   *                        typically that of a known checkpoint. If this version's not provided,
   *                        we will start listing from version 0.
   * @param versionToLoad A specific version to load. Typically used with time travel and the
   *                      Delta streaming source. If not provided, we will try to load the latest
   *                      version of the table.
   * @return Some LogSegment to build a Snapshot if files do exist after the given
   *         startCheckpoint. None, if there are no new files after `startCheckpoint`.
   */
  protected def getLogSegmentForVersion(
    startCheckpoint: Option[Long],
    versionToLoad: Option[Long] = None): LogSegment = {

    // List from the starting checkpoint. If a checkpoint doesn't exist, this will still return
    // deltaVersion=0.
    val newFiles = store.listFrom(checkpointPrefix(logPath, startCheckpoint.getOrElse(0L)))
      .asScala
      // Pick up all checkpoint and delta files
      .filter { file => isCheckpointFile(file.getPath) || isDeltaFile(file.getPath) }
      // filter out files that aren't atomically visible. Checkpoint files of 0 size are invalid
      .filterNot { file => isCheckpointFile(file.getPath) && file.getLen == 0 }
      // take files until the version we want to load
      .takeWhile(f => versionToLoad.forall(v => getFileVersion(f.getPath) <= v))
      .toArray

    if (newFiles.isEmpty && startCheckpoint.isEmpty) {
      throw DeltaErrors.emptyDirectoryException(logPath.toString)
    } else if (newFiles.isEmpty) {
      // The directory may be deleted and recreated and we may have stale state in our DeltaLog
      // singleton, so try listing from the first version
      return getLogSegmentForVersion(None, versionToLoad)
    }
    val (checkpoints, deltas) = newFiles.partition(f => isCheckpointFile(f.getPath))

    // Find the latest checkpoint in the listing that is not older than the versionToLoad
    val lastCheckpoint = versionToLoad.map(CheckpointInstance(_, None))
      .getOrElse(CheckpointInstance.MaxValue)
    val checkpointFiles = checkpoints.map(f => CheckpointInstance(f.getPath))
    val newCheckpoint = getLatestCompleteCheckpointFromList(checkpointFiles, lastCheckpoint)
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
        versionToLoad.foreach { version =>
          require(deltaVersions.last == version,
            s"Did not get the last delta file version: $version to compute Snapshot")
        }
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
      versionToLoad.foreach { version =>
        require(deltaVersions.last == version,
          s"Did not get the last delta file version: $version to compute Snapshot")
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

  private def getSnapshotAtInit: SnapshotImpl = {
    try {
      val logSegment = getLogSegmentForVersion(lastCheckpoint.map(_.version))
      val snapshot = createSnapshot(logSegment, logSegment.lastCommitTimestamp)
      snapshot
    } catch {
      case _: FileNotFoundException =>
        new InitialSnapshotImpl(hadoopConf, logPath, this)
    }
  }

  private def getSnapshotAt(version: Long): SnapshotImpl = {
    if (snapshot.version == version) return snapshot

    val startingCheckpoint = findLastCompleteCheckpoint(CheckpointInstance(version, None))
    val segment = getLogSegmentForVersion(startingCheckpoint.map(_.version), Some(version))

    createSnapshot(
      segment,
      segment.lastCommitTimestamp
    )
  }

  private def createSnapshot(segment: LogSegment, lastCommitTimestamp: Long): SnapshotImpl = {
    new SnapshotImpl(
      hadoopConf,
      logPath,
      segment.version,
      segment,
      this,
      lastCommitTimestamp)
  }

  private def verifyDeltaVersions(versions: Array[Long]): Unit = {
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

object LogSegment {
  /** The LogSegment for an empty transaction log directory. */
  def empty(path: Path): LogSegment = LogSegment(path, -1L, Nil, Nil, None, -1L)
}
