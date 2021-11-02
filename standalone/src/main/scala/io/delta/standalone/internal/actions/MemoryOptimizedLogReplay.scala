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

import com.github.mjakubowski84.parquet4s.ParquetReader
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

  def getReverseIterator: CloseableIterator[(Action, Boolean)] =
    new CloseableIterator[(Action, Boolean)] {
      private val reverseFilesIter: Iterator[Path] = files.sortWith(_.getName > _.getName).iterator
      private var jsonIter: Option[CloseableIterator[String]] = None
      private var parquetIter: Option[Iterator[Parquet4sSingleActionWrapper]] = None

      // Initialize next element so that the first hasNext() and next() calls succeed
      prepareNext()

      private def prepareNext(): Unit = {
        if (jsonIter.isDefined && jsonIter.get.hasNext) return
        if (parquetIter.isDefined && parquetIter.get.hasNext) return

        if (jsonIter.isDefined) jsonIter.get.close()

        if (!reverseFilesIter.hasNext) {
          jsonIter = None
          parquetIter = None
        } else {
          // TODO: what about empty JSON files?
          val nextFile = reverseFilesIter.next()

          if (nextFile.getName.endsWith(".json")) {
            jsonIter = Some(logStore.read(nextFile, hadoopConf))
            parquetIter = None
          } else if (nextFile.getName.endsWith(".parquet")) {
            val parquetIterable = ParquetReader.read[Parquet4sSingleActionWrapper](
              nextFile.toString,
              ParquetReader.Options(timeZone, hadoopConf = hadoopConf)
            )

            parquetIter = Some(parquetIterable.iterator)
            jsonIter = None
          } else {
            throw new IllegalStateException(s"unexpected log file path: $nextFile")
          }
        }
      }

      override def hasNext: Boolean = {
        if (jsonIter.isDefined) return jsonIter.get.hasNext
        if (parquetIter.isDefined) return parquetIter.get.hasNext

        false
      }

      override def next(): (Action, Boolean) = {
        if (!hasNext) throw new NoSuchElementException

        val result = if (jsonIter.isDefined) {
          val nextLine = jsonIter.get.next()
          (JsonUtils.mapper.readValue[SingleAction](nextLine).unwrap, false)
        } else if (parquetIter.isDefined) {
          val nextWrappedSingleAction = parquetIter.get.next()
          (nextWrappedSingleAction.unwrap.unwrap, true)
        } else {
          throw new IllegalStateException("Impossible")
        }

        prepareNext()

        result
      }

      override def close(): Unit = {
        if (jsonIter.isDefined) jsonIter.get.close()
      }
  }

}
