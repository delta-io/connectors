package main.scala

import main.scala.util.FileNames._
import org.apache.hadoop.fs.Path

case class CheckpointMetaData(
    version: Long,
    size: Long,
    parts: Option[Int])

case class CheckpointInstance(
    version: Long,
    numParts: Option[Int]) extends Ordered[CheckpointInstance] {

  def isEarlierThan(other: CheckpointInstance): Boolean = {
    if (other == CheckpointInstance.MaxValue) return true
    version < other.version ||
      (version == other.version && numParts.forall(_ < other.numParts.getOrElse(1)))
  }

  def isNotLaterThan(other: CheckpointInstance): Boolean = {
    if (other == CheckpointInstance.MaxValue) return true
    version <= other.version
  }

  def getCorrespondingFiles(path: Path): Seq[Path] = {
    assert(this != CheckpointInstance.MaxValue, "Can't get files for CheckpointVersion.MaxValue.")
    numParts match {
      case None => checkpointFileSingular(path, version) :: Nil
      case Some(parts) => checkpointFileWithParts(path, version, parts)
    }
  }

  override def compare(that: CheckpointInstance): Int = {
    if (version == that.version) {
      numParts.getOrElse(1) - that.numParts.getOrElse(1)
    } else {
      // we need to guard against overflow. We just can't return (this - that).toInt
      if (version - that.version < 0) -1 else 1
    }
  }
}

object CheckpointInstance {
  def apply(path: Path): CheckpointInstance = {
    CheckpointInstance(checkpointVersion(path), numCheckpointParts(path))
  }

  def apply(metadata: CheckpointMetaData): CheckpointInstance = {
    CheckpointInstance(metadata.version, metadata.parts)
  }

  val MaxValue: CheckpointInstance = CheckpointInstance(-1, None)
}

trait Checkpoints {
  self: DeltaLog =>
}