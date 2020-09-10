package exception

import java.io.FileNotFoundException

import org.apache.hadoop.fs.Path

object DeltaErrors {

  def deltaVersionsNotContiguousException(deltaVersions: Seq[Long]): Throwable = {
    new IllegalStateException(s"Versions ($deltaVersions) are not contiguous.")
  }

  def emptyDirectoryException(directory: String): Throwable = {
    new FileNotFoundException(s"No file found in the directory: $directory.")
  }

  def logFileNotFoundException(
    path: Path,
    version: Long): Throwable = {
// TODO
//    val logRetention = DeltaConfigs.LOG_RETENTION.fromMetaData(metadata)
//    val checkpointRetention = DeltaConfigs.CHECKPOINT_RETENTION_DURATION.fromMetaData(metadata)
    new FileNotFoundException(s"$path: Unable to reconstruct state at version $version as the " +
      s"transaction log has been truncated due to manual deletion or the log retention policy ")

//      s"(${DeltaConfigs.LOG_RETENTION.key}=$logRetention) and checkpoint retention policy " +
//      s"(${DeltaConfigs.CHECKPOINT_RETENTION_DURATION.key}=$checkpointRetention)")
  }

  def missingPartFilesException(version: Long, ae: Exception): Throwable = {
    new IllegalStateException(
      s"Couldn't find all part files of the checkpoint version: $version", ae)
  }

}
