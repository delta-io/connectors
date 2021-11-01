/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone.internal.actions

import java.util.TimeZone

import scala.collection.JavaConverters._

import com.github.mjakubowski84.parquet4s.ParquetReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import io.delta.standalone.storage.LogStore

import io.delta.standalone.internal.util.JsonUtils

private[internal] class MemoryOptimizedLogReplay(
    files: Seq[Path],
    logStore: LogStore,
    hadoopConf: Configuration,
    timeZone: TimeZone) {

  /**
   * @param actionListener a listener to receive all actions when we are reading logs. The second
   *                       `Boolean` parameter means whether an action is loaded from a checkpoint.
   */
  def replayActionsReversely(actionListener: (Action, Boolean) => Unit) : Unit = {
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
