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

import com.github.mjakubowski84.parquet4s.{ParquetIterable, ParquetReader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import io.delta.standalone.data.CloseableIterator
import io.delta.standalone.storage.LogStore

import io.delta.standalone.internal.util.JsonUtils

private[internal] class MemoryOptimizedLogReplay(
    files: Seq[Path],
    logStore: LogStore,
    hadoopConf: Configuration,
    timeZone: TimeZone) {

  /**
   * Replay the transaction logs from the newest log file to the oldest log file.
   *
   * @return a [[CloseableIterator]] of tuple (Action, isLoadedFromCheckpoint)
   */
  def getReverseIterator: CloseableIterator[(Action, Boolean)] =
    new CloseableIterator[(Action, Boolean)] {
      private val reverseFilesIter: Iterator[Path] = files.sortWith(_.getName > _.getName).iterator
      private var jsonIter: Option[CloseableIterator[String]] = None
      private var parquetIter: Option[(
        ParquetIterable[Parquet4sSingleActionWrapper],
        Iterator[Parquet4sSingleActionWrapper])] = None
      private var nextIsLoaded = false

      private def prepareNext(): Unit = {
        if (jsonIter.exists(_.hasNext)) return
        if (parquetIter.exists(_._2.hasNext)) return

        jsonIter.foreach(_.close())
        parquetIter.foreach(_._1.close())

        jsonIter = None
        parquetIter = None

        if (reverseFilesIter.hasNext) {
          val nextFile = reverseFilesIter.next()

          if (nextFile.getName.endsWith(".json")) {
            jsonIter = Some(logStore.read(nextFile, hadoopConf))
          } else if (nextFile.getName.endsWith(".parquet")) {
            val parquetIterable = ParquetReader.read[Parquet4sSingleActionWrapper](
              nextFile.toString,
              ParquetReader.Options(timeZone, hadoopConf = hadoopConf)
            )

            parquetIter = Some(parquetIterable, parquetIterable.iterator)
          } else {
            throw new IllegalStateException(s"unexpected log file path: $nextFile")
          }
        }
      }

      override def hasNext: Boolean = {
        if (!nextIsLoaded) {
          prepareNext()
          nextIsLoaded = true
        }

        if (jsonIter.isDefined) return jsonIter.get.hasNext
        if (parquetIter.isDefined) return parquetIter.get._2.hasNext

        false
      }

      override def next(): (Action, Boolean) = {
        if (!hasNext) throw new NoSuchElementException

        val result = if (jsonIter.isDefined) {
          val nextLine = jsonIter.get.next()
          (JsonUtils.mapper.readValue[SingleAction](nextLine).unwrap, false)
        } else if (parquetIter.isDefined) {
          val nextWrappedSingleAction = parquetIter.get._2.next()
          (nextWrappedSingleAction.unwrap.unwrap, true)
        } else {
          throw new IllegalStateException("Impossible")
        }

        nextIsLoaded = false

        result
      }

      override def close(): Unit = {
        jsonIter.foreach(_.close())
        parquetIter.foreach(_._1.close())
      }
  }

}
