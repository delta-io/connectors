package main.scala

import org.apache.hadoop.fs.{FileSystem, Path}

class Snapshot(
  val path: Path,
  val version: Long,
  previousSnapshot: Option[State],
  files: Seq[Path],
  val deltaLog: DeltaLog,
  val timestamp: Long) {
    ...
}
