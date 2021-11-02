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
    val hadoopConf: Configuration,
    timeZone: TimeZone) {

  /**
   * Replay the transaction logs from the newest log file to the oldest log file.
   *
   * @return a [[CloseableIterator]] of tuple (Action, isLoadedFromCheckpoint)
   */
  def getReverseIterator: CloseableIterator[(Action, Boolean)] =
    new CloseableIterator[(Action, Boolean)] {
      private val reverseFilesIter: Iterator[Path] = files.sortWith(_.getName > _.getName).iterator
      private var actionIter: Option[CloseableIterator[(Action, Boolean)]] = None

      /**
       * If the current `actionIter` has no more elements, this function repeatedly reads the next
       * file, if it exists, and creates the next `actionIter` until we find a non-empty file.
       */
      private def ensureNextIterIsReady(): Unit = {
        // this iterator already has a next element, we can return early
        if (actionIter.exists(_.hasNext)) return

        actionIter.foreach(_.close())
        actionIter = None

        // there might be empty files. repeat until we find a non-empty file or run out of files
        while (reverseFilesIter.hasNext) {
          val nextFile = reverseFilesIter.next()

          if (nextFile.getName.endsWith(".json")) {
            actionIter = Some(new CustomJsonIterator(logStore.read(nextFile, hadoopConf)))
          } else if (nextFile.getName.endsWith(".parquet")) {
            val parquetIterable = ParquetReader.read[Parquet4sSingleActionWrapper](
              nextFile.toString,
              ParquetReader.Options(timeZone, hadoopConf = hadoopConf)
            )
            actionIter = Some(new CustomParquetIterator(parquetIterable))
          } else {
            throw new IllegalStateException(s"unexpected log file path: $nextFile")
          }

          if (actionIter.exists(_.hasNext)) return

          // It was an empty file
          actionIter.foreach(_.close())
          actionIter = None
        }
      }

      override def hasNext: Boolean = {
        ensureNextIterIsReady()

        // from the semantics of `ensureNextIterIsReady()`, if `actionIter` is defined, then it has
        // a next element
        actionIter.isDefined
      }

      override def next(): (Action, Boolean) = {
        if (!hasNext()) throw new NoSuchElementException

        if (actionIter.isEmpty) throw new IllegalStateException("Impossible")

        actionIter.get.next()
      }

      override def close(): Unit = {
        actionIter.foreach(_.close())
      }
  }
}

private class CustomJsonIterator(iter: CloseableIterator[String])
  extends CloseableIterator[(Action, Boolean)] {

  override def hasNext: Boolean = iter.hasNext

  override def next(): (Action, Boolean) = {
    (JsonUtils.mapper.readValue[SingleAction](iter.next).unwrap, false)
  }

  override def close(): Unit = iter.close()
}

private class CustomParquetIterator(iterable: ParquetIterable[Parquet4sSingleActionWrapper])
  extends CloseableIterator[(Action, Boolean)] {

  private val iter = iterable.iterator

  override def hasNext: Boolean = iter.hasNext

  override def next(): (Action, Boolean) = {
    (iter.next().unwrap.unwrap, true)
  }

  override def close(): Unit = iterable.close()
}
