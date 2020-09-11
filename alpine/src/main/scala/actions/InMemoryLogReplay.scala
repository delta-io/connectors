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

package main.scala.actions

import java.net.URI

import main.scala.Snapshot.canonicalizePath
import org.apache.hadoop.conf.Configuration

class InMemoryLogReplay(
    minFileRetentionTimestamp: Long,
    hadoopConf: Configuration) {
  var currentProtocolVersion: Protocol = null
  var currentVersion: Long = -1
  var currentMetaData: Metadata = null
  val transactions = new scala.collection.mutable.HashMap[String, SetTransaction]()
  val activeFiles = new scala.collection.mutable.HashMap[URI, AddFile]()
  var sizeInBytes: Long = 0
  var numMetadata: Long = 0
  var numProtocol: Long = 0
  private val tombstones = new scala.collection.mutable.HashMap[URI, RemoveFile]()

  def append(version: Long, actions: Iterator[Action]): Unit = {
    assert(currentVersion == -1 || version == currentVersion + 1,
      s"Attempted to replay version $version, but state is at $currentVersion")
    currentVersion = version
    actions.foreach {
      case a: SetTransaction =>
        transactions(a.appId) = a
      case a: Metadata =>
        currentMetaData = a
        numMetadata += 1
      case a: Protocol =>
        currentProtocolVersion = a
        numProtocol += 1
      case add: AddFile =>
        val canonicalizeAdd = add.copy(
          dataChange = false,
          path = canonicalizePath(add.path, hadoopConf))
        activeFiles(canonicalizeAdd.pathAsUri) = canonicalizeAdd
        // Remove the tombstone to make sure we only output one `FileAction`.
        tombstones.remove(canonicalizeAdd.pathAsUri)
        sizeInBytes += canonicalizeAdd.size
      case remove: RemoveFile =>
        val canonicaleRemove = remove.copy(
          dataChange = false,
          path = canonicalizePath(remove.path, hadoopConf))
        val removedFile = activeFiles.remove(canonicaleRemove.pathAsUri)
        tombstones(canonicaleRemove.pathAsUri) = canonicaleRemove

        if (removedFile.isDefined) {
          sizeInBytes -= removedFile.get.size
        }
      case _: CommitInfo => // do nothing
      case null => // Some crazy future feature. Ignore
    }
  }

  private def getTombstones: Iterable[FileAction] = {
    tombstones.values.filter(_.delTimestamp > minFileRetentionTimestamp)
  }

  def numRemoves: Long = getTombstones.size

  /** Returns the current state of the Table as an iterator of actions. */
  def checkpoint: Iterator[Action] = {
    Option(currentProtocolVersion).toIterator ++
    Option(currentMetaData).toIterator ++
    transactions.values.toIterator ++
    (activeFiles.values ++ getTombstones).toSeq.sortBy(_.path).iterator
  }
}
