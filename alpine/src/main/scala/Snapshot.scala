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

package main.scala

import java.net.URI

import scala.main.util.JsonUtils

import com.github.mjakubowski84.parquet4s.ParquetReader
import actions.{AddFile, InMemoryLogReplay, Metadata, Protocol, SetTransaction, SingleAction}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class Snapshot(
    val path: Path,
    val version: Long,
    val logSegment: LogSegment,
    val minFileRetentionTimestamp: Long,
    val deltaLog: DeltaLog,
    val timestamp: Long) {
  import Snapshot._

  private def load(paths: Seq[Path]): Seq[SingleAction] = {
    paths.map(_.toString).sortWith(_ < _).par.flatMap { path =>
      if (path.endsWith("json")) {
        deltaLog.store.read(path).map { line =>
          JsonUtils.mapper.readValue[SingleAction](line)
        }
      } else if (path.endsWith("parquet")) {
        ParquetReader.read[SingleAction](path).toSeq
      } else Seq.empty[SingleAction]
    }.toList
  }

  protected lazy val state: State = {
    val logPathURI = path.toUri
    val replay = new InMemoryLogReplay(minFileRetentionTimestamp, DeltaLog.hadoopConf)
    val files = (logSegment.deltas ++ logSegment.checkpoints).map(_.getPath)

    // assertLogBelongsToTable
    files.foreach {f =>
      if (f.toString.isEmpty || f.getParent != new Path(logPathURI)) {
        // scalastyle:off throwerror
        throw new AssertionError(s"File (${f.toString}) doesn't belong in the " +
          s"transaction log at $logPathURI. Please contact Databricks Support.")
        // scalastyle:on throwerror
      }
    }

    val actions = load(files).map(_.unwrap)

    replay.append(0, actions.iterator)

    State(
      replay.currentProtocolVersion,
      replay.currentMetaData,
      replay.transactions.values.toSeq,
      replay.activeFiles.toMap,
      replay.sizeInBytes,
      replay.activeFiles.size,
      replay.numMetadata,
      replay.numProtocol,
      replay.numRemoves,
      replay.transactions.size
    )
  }

  def protocol: Protocol = state.protocol
  def metadata: Metadata = state.metadata
  def setTransactions: Seq[SetTransaction] = state.setTransactions
  def sizeInBytes: Long = state.sizeInBytes
  def numOfFiles: Long = state.numOfFiles
  def numOfMetadata: Long = state.numOfMetadata
  def numOfProtocol: Long = state.numOfProtocol
  def numOfRemoves: Long = state.numOfRemoves
  def numOfSetTransactions: Long = state.numOfSetTransactions
  def allFiles: Set[AddFile] = state.activeFiles.values.toSet
}

object Snapshot {
  /** Canonicalize the paths for Actions */
  def canonicalizePath(path: String, hadoopConf: Configuration): String = {
    val hadoopPath = new Path(new URI(path))
    if (hadoopPath.isAbsoluteAndSchemeAuthorityNull) {
      val fs = FileSystem.get(hadoopConf)
      fs.makeQualified(hadoopPath).toUri.toString
    } else {
      // return untouched if it is a relative path or is already fully qualified
      hadoopPath.toUri.toString
    }
  }

  case class State(
      protocol: Protocol,
      metadata: Metadata,
      setTransactions: Seq[SetTransaction],
      activeFiles: scala.collection.immutable.Map[URI, AddFile],
      sizeInBytes: Long,
      numOfFiles: Long,
      numOfMetadata: Long,
      numOfProtocol: Long,
      numOfRemoves: Long,
      numOfSetTransactions: Long)
}

class InitialSnapshot(
    val logPath: Path,
    override val deltaLog: DeltaLog,
    override val metadata: Metadata)
  extends Snapshot(logPath, -1, LogSegment.empty(logPath), -1, deltaLog, -1) {

  def this(logPath: Path, deltaLog: DeltaLog) = this(
    logPath,
    deltaLog,
    Metadata() // TODO: SparkSession.active.sessionState.conf ?
  )

  override protected lazy val state: Snapshot.State = {
    val protocol = Protocol()
    Snapshot.State(protocol, metadata, Nil, Map.empty[URI, AddFile], 0L, 0L, 1L, 1L, 0L, 0L)
  }
}
