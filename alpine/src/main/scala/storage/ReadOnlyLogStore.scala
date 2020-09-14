/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package main.scala.storage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

trait ReadOnlyLogStore {

  /** Read the given `path` */
  final def read(path: String): Seq[String] = read(new Path(path))

  /** Read the given `path` */
  def read(path: Path): Seq[String]

  /**
   * List the paths in the same directory that are lexicographically greater or equal to
   * (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
   */
  final def listFrom(path: String): Iterator[FileStatus] = listFrom(new Path(path))

  /**
   * List the paths in the same directory that are lexicographically greater or equal to
   * (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
   */
  def listFrom(path: Path): Iterator[FileStatus]

  /** Resolve the fully qualified path for the given `path`. */
  def resolvePathOnPhysicalStorage(path: Path): Path = {
    throw new UnsupportedOperationException()
  }
}

object ReadOnlyLogStore extends ReadOnlyLogStoreProvider {
  def apply(hadoopConf: Configuration): ReadOnlyLogStore = {
    createLogStore(hadoopConf)
  }
}

trait ReadOnlyLogStoreProvider {
  def createLogStore(hadoopConf: Configuration): ReadOnlyLogStore =
    new HDFSReadOnlyLogStore(hadoopConf)
}
