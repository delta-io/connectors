package main.scala

import java.net.URI

import main.scala.actions.{AddFile, Metadata, Protocol, SetTransaction}
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

  private def load() = {

  }

  lazy val computedState: State = {
    null
  }

  def protocol: Protocol = computedState.protocol
  def metadata: Metadata = computedState.metadata
  def setTransactions: Seq[SetTransaction] = computedState.setTransactions
  def sizeInBytes: Long = computedState.sizeInBytes
  def numOfFiles: Long = computedState.numOfFiles
  def numOfMetadata: Long = computedState.numOfMetadata
  def numOfProtocol: Long = computedState.numOfProtocol
  def numOfRemoves: Long = computedState.numOfRemoves
  def numOfSetTransactions: Long = computedState.numOfSetTransactions
  def allFiles: Set[AddFile] = computedState.activeFiles.values.toSet
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