package actions

import java.net.URI

import main.scala.actions.{Action, AddFile, FileAction, Metadata, Protocol, RemoveFile, SetTransaction}
import org.apache.hadoop.conf.Configuration

class InMemoryLogReplay(
    minFileRetentionTimestamp: Long,
    hadoopConf: Configuration) {
  var currentProtocolVersion: Protocol = null
  var currentVersion: Long = -1
  var currentMetaData: Metadata = null
  val transactions = new scala.collection.mutable.HashMap[String, SetTransaction]()
  val activeFiles = new scala.collection.mutable.HashMap[URI, AddFile]()
  private val tombstones = new scala.collection.mutable.HashMap[URI, RemoveFile]()


  def append(version: Long, actions: Iterator[Action]): Unit = {

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
