package main.scala.util

import org.apache.hadoop.fs.Path

object FileNames {
  def checkpointFileSingular(path: Path, version: Long): Path =
    new Path(path, f"$version%020d.checkpoint.parquet")

  def checkpointFileWithParts(path: Path, version: Long, numParts: Int): Seq[Path] = {
    Range(1, numParts + 1)
      .map(i => new Path(path, f"$version%020d.checkpoint.$i%010d.$numParts%010d.parquet"))
  }

  def numCheckpointParts(path: Path): Option[Int] = {
    val segments = path.getName.split("\\.")

    if (segments.size != 5) None else Some(segments(3).toInt)
  }

  def checkpointVersion(path: Path): Long = path.getName.split("\\.")(0).toLong
}
