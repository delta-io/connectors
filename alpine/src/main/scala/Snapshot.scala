package main.scala

import java.net.URI

import main.scala.actions.{Metadata, Protocol, SetTransaction}
import main.scala.Snapshot.State
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class Snapshot(
    val path: Path,
    val version: Long,
    previousSnapshot: Option[State],
    files: Seq[Path],
    val deltaLog: DeltaLog,
    val timestamp: Long) {

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
      sizeInBytes: Long,
      numOfFiles: Long,
      numOfMetadata: Long,
      numOfProtocol: Long,
      numOfRemoves: Long,
      numOfSetTransactions: Long)

}