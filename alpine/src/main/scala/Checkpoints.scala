package main.scala

import java.io.FileNotFoundException

import scala.main.util.JsonUtils
import scala.util.control.NonFatal

import main.scala.storage.LogStore
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

  def logPath: Path
  def dataPath: Path
  def snapshot: Snapshot
  protected def store: LogStore

  /** The path to the file that holds metadata about the most recent checkpoint. */
  val LAST_CHECKPOINT = new Path(logPath, "_last_checkpoint")

  private[delta] def lastCheckpoint: Option[CheckpointMetaData] = {
    loadMetadataFromFile(0)
  }

  /** Loads the checkpoint metadata from the _last_checkpoint file. */
  private def loadMetadataFromFile(tries: Int): Option[CheckpointMetaData] = {
    try {
      val checkpointMetadataJson = store.read(LAST_CHECKPOINT)
      val checkpointMetadata =
        JsonUtils.mapper.readValue[CheckpointMetaData](checkpointMetadataJson.head)
      Some(checkpointMetadata)
    } catch {
      case _: FileNotFoundException =>
        None
      case NonFatal(e) if tries < 3 =>
        // scalastyle:off println
        println(s"Failed to parse $LAST_CHECKPOINT. This may happen if there was an error " +
          "during read operation, or a file appears to be partial. Sleeping and trying again.", e)
        // scalastyle:on println

        Thread.sleep(1000)
        loadMetadataFromFile(tries + 1)
      case NonFatal(e) =>
        // scalastyle:off println
        println(s"$LAST_CHECKPOINT is corrupted. Will search the checkpoint files directly", e)
        // scalastyle:on println
        // Hit a partial file. This could happen on Azure as overwriting _last_checkpoint file is
        // not atomic. We will try to list all files to find the latest checkpoint and restore
        // CheckpointMetaData from it.
        val verifiedCheckpoint = findLastCompleteCheckpoint(CheckpointInstance(-1L, None))
        verifiedCheckpoint.map(manuallyLoadCheckpoint)
    }
  }

  protected def manuallyLoadCheckpoint(cv: CheckpointInstance): CheckpointMetaData = {
    CheckpointMetaData(cv.version, -1L, cv.numParts)
  }

  protected def findLastCompleteCheckpoint(cv: CheckpointInstance): Option[CheckpointInstance] = {
    var cur = math.max(cv.version, 0L)
    while (cur >= 0) {
      val checkpoints = store.listFrom(checkpointPrefix(logPath, math.max(0, cur - 1000)))
        .map(_.getPath)
        .filter(isCheckpointFile)
        .map(CheckpointInstance(_))
        .takeWhile(tv => (cur == 0 || tv.version <= cur) && tv.isEarlierThan(cv))
        .toArray
      val lastCheckpoint = getLatestCompleteCheckpointFromList(checkpoints, cv)
      if (lastCheckpoint.isDefined) {
        return lastCheckpoint
      } else {
        cur -= 1000
      }
    }
    None
  }

  protected def getLatestCompleteCheckpointFromList(
    instances: Array[CheckpointInstance],
    notLaterThan: CheckpointInstance): Option[CheckpointInstance] = {
    val complete = instances.filter(_.isNotLaterThan(notLaterThan)).groupBy(identity).filter {
      case (CheckpointInstance(_, None), inst) => inst.length == 1
      case (CheckpointInstance(_, Some(parts)), inst) => inst.length == parts
    }
    complete.keys.toArray.sorted.lastOption
  }
}