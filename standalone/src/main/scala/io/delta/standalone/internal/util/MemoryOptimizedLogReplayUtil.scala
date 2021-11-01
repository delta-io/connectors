package io.delta.standalone.internal.util

import java.util.TimeZone

import scala.collection.JavaConverters._

import com.github.mjakubowski84.parquet4s.ParquetReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import io.delta.standalone.storage.LogStore

import io.delta.standalone.internal.actions.{Action, Parquet4sSingleActionWrapper, SingleAction}

private[internal] object MemoryOptimizedLogReplayUtil {

  /**
   * Replay the transaction logs from the newest log file to the oldest log file.
   *
   * @param actionListener a listener to receive all actions when we are reading logs. The second
   *                       `Boolean` parameter means whether an action is loaded from a checkpoint.
   */
  def replayActionsReversely(
      files: Seq[Path],
      logStore: LogStore,
      hadoopConf: Configuration,
      timeZone: TimeZone,
      actionListener: (Action, Boolean) => Unit): Unit = {
    files.sortWith(_.getName > _.getName).foreach { path =>
      if (path.getName.endsWith(".json")) {
        val iter = logStore.read(path, hadoopConf)
        try {
          iter
            .asScala
            .map { line =>
              JsonUtils.mapper.readValue[SingleAction](line).unwrap
            }
            .foreach { action =>
              actionListener(action, false)
            }
        } finally {
          iter.close()
        }
      } else if (path.getName.endsWith(".parquet")) {
        val iter = ParquetReader.read[Parquet4sSingleActionWrapper](
          path.toString,
          ParquetReader.Options(timeZone, hadoopConf = hadoopConf)
        )
        try {
          iter.iterator.map(_.unwrap.unwrap).foreach { action =>
            actionListener(action, true)
          }
        } finally {
          iter.close()
        }
      } else {
        throw new IllegalStateException(s"unexpected log file path: $path")
      }
    }
  }

}
