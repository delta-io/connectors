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

import java.sql.Timestamp

import collection.JavaConverters._

import io.delta.alpine.internal.actions.CommitMarker
import io.delta.alpine.internal.exception.DeltaErrors
import io.delta.alpine.internal.util.FileNames._
import io.delta.alpine.ReadOnlyLogStore
import org.apache.hadoop.fs.Path

case class DeltaHistoryManager(deltaLog: DeltaLogImpl) {

  /** Check whether the given version can be recreated by replaying the DeltaLog. */
  def checkVersionExists(version: Long): Unit = {
    val earliest = getEarliestDeltaFileVersion // TODO OSS uses getEarliestReproducibleCommit
    val latest = deltaLog.update().version
    if (version < earliest || version > latest) {
      throw DeltaErrors.versionNotExistException(version, earliest, latest)
    }
  }

  def getActiveCommitAtTime(
    timestamp: Timestamp
    //      canReturnLastCommit: Boolean,
    //      mustBeRecreatable: Boolean = true,
    //      canReturnEarliestCommit: Boolean = false
  ): Commit = {
    val time = timestamp.getTime
    val earliest = getEarliestDeltaFileVersion
    val latestVersion = deltaLog.update().version

    // Search for the commit
    val commits = getCommits(deltaLog.store, deltaLog.logPath, earliest, Some(latestVersion + 1))
    // If it returns empty, we will fail below with `timestampEarlierThanCommitRetention`.
    val commit = lastCommitBeforeTimestamp(commits, time).getOrElse(commits.head)

    // TODO
    // Error handling
    //    val commitTs = new Timestamp(commit.timestamp)
    //    val timestampFormatter = TimestampFormatter(
    //      DateTimeUtils.getTimeZone(SQLConf.get.sessionLocalTimeZone))
    //    val tsString = DateTimeUtils.timestampToString(
    //      timestampFormatter, DateTimeUtils.fromJavaTimestamp(commitTs))
    //    if (commit.timestamp > time && !canReturnEarliestCommit) {
    //      throw DeltaErrors.timestampEarlierThanCommitRetention(timestamp, commitTs, tsString)
    //    } else if (commit.version == latestVersion && !canReturnLastCommit) {
    //      if (commit.timestamp < time) {
    //        throw
    //        DeltaErrors.temporallyUnstableInput(timestamp, commitTs, tsString, commit.version)
    //      }
    //    }
    commit
  }

  private def getEarliestDeltaFileVersion: Long = {
    val earliestVersionOpt = deltaLog.store.listFrom(deltaFile(deltaLog.logPath, 0))
      .asScala
      .filter(f => isDeltaFile(f.getPath))
      .take(1).toArray.headOption
    if (earliestVersionOpt.isEmpty) {
      throw DeltaErrors.noHistoryFound(deltaLog.logPath)
    }
    deltaVersion(earliestVersionOpt.get.getPath)
  }

  // TODO private[tahoe]
  private def getCommits(
    logStore: ReadOnlyLogStore,
    logPath: Path,
    start: Long,
    end: Option[Long] = None): Array[Commit] = {
    val until = end.getOrElse(Long.MaxValue)
    val commits = logStore.listFrom(deltaFile(logPath, start))
      .asScala
      .filter(f => isDeltaFile(f.getPath))
      .map { fileStatus =>
        Commit(deltaVersion(fileStatus.getPath), fileStatus.getModificationTime)
      }
      .takeWhile(_.version < until)

    monotonizeCommitTimestamps(commits.toArray)
  }

  private def monotonizeCommitTimestamps[T <: CommitMarker](commits: Array[T]): Array[T] = {
    var i = 0
    val length = commits.length
    while (i < length - 1) {
      val prevTimestamp = commits(i).getTimestamp
      assert(commits(i).getVersion < commits(i + 1).getVersion, "Unordered commits provided.")
      if (prevTimestamp >= commits(i + 1).getTimestamp) {
        commits(i + 1) = commits(i + 1).withTimestamp(prevTimestamp + 1).asInstanceOf[T]
      }
      i += 1
    }
    commits
  }

  /** Returns the latest commit that happened at or before `time`. */
  private def lastCommitBeforeTimestamp(commits: Seq[Commit], time: Long): Option[Commit] = {
    val i = commits.lastIndexWhere(_.timestamp <= time)
    if (i < 0) None else Some(commits(i))
  }

  /** A helper class to represent the timestamp and version of a commit. */
  case class Commit(version: Long, timestamp: Long) extends CommitMarker {
    override def withTimestamp(timestamp: Long): Commit = this.copy(timestamp = timestamp)

    override def getTimestamp: Long = timestamp

    override def getVersion: Long = version
  }
}
