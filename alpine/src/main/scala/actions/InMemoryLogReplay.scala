package actions

import java.net.URI


import main.scala.actions.{Action, AddFile, CommitInfo, FileAction, Metadata, Protocol, RemoveFile, SetTransaction}
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

  /** Returns the current state of the Table as an iterator of actions. */
  def checkpoint: Iterator[Action] = {
    Option(currentProtocolVersion).toIterator ++
    Option(currentMetaData).toIterator ++
    transactions.values.toIterator ++
    (activeFiles.values ++ getTombstones).toSeq.sortBy(_.path).iterator
  }
}
