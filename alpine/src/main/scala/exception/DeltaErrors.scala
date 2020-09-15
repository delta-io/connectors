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
    // TODO: use DeltaConfigs.LOG_RETENTION, CHECKPOINT_RETENTION_DURATION for extra info
    new FileNotFoundException(s"$path: Unable to reconstruct state at version $version as the " +
      s"transaction log has been truncated due to manual deletion or the log retention policy ")
  }

  def missingPartFilesException(version: Long, ae: Exception): Throwable = {
    new IllegalStateException(
      s"Couldn't find all part files of the checkpoint version: $version", ae)
  }

}
