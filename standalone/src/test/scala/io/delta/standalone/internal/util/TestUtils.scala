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

package io.delta.standalone.internal.util

import java.io.File
import java.nio.file.Files
import java.util.UUID

import scala.collection.JavaConverters._

import io.delta.standalone.actions.{Action => ActionJ, AddFile => AddFileJ}
import io.delta.standalone.internal.actions.{Action, AddFile}

import org.apache.commons.io.FileUtils

object TestUtils {

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
   def withTempDir(f: File => Unit): Unit = {
    val dir = Files.createTempDirectory(UUID.randomUUID().toString).toFile
    try f(dir) finally {
      FileUtils.deleteDirectory(dir)
    }
  }

  implicit def actionSeqToList[T <: Action](seq: Seq[T]): java.util.List[ActionJ] =
    seq.map(ConversionUtils.convertAction).asJava

  implicit def addFileSeqToList(seq: Seq[AddFile]): java.util.List[AddFileJ] =
    seq.map(ConversionUtils.convertAddFile).asJava

}
