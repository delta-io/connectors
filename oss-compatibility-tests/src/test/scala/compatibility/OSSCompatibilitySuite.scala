/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package compatibility

import java.io.File
import java.nio.file.Files
import java.util.UUID

import io.delta.standalone.{DeltaLog => StandaloneDeltaLog}

import org.apache.spark.sql.delta.{DeltaLog => OSSDeltaLog}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class OSSCompatibilitySuite extends QueryTest
  with SharedSparkSession
  with StandaloneUtil
  with OSSUtil {

  private def withTempDirAndLogs(f: (File, StandaloneDeltaLog, OSSDeltaLog) => Unit): Unit = {
    val dir = Files.createTempDirectory(UUID.randomUUID().toString).toFile

    val standaloneLog = StandaloneDeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
    val ossLog = OSSDeltaLog.forTable(spark, dir.getCanonicalPath)

    try f(dir, standaloneLog, ossLog) finally {
      FileUtils.deleteDirectory(dir)
    }
  }

  test("actions") {
    withTempDirAndLogs { (dir, standaloneLog, ossLog) =>

    }
  }

  test("concurrency conflicts") {
    withTempDirAndLogs { (dir, standaloneLog, ossLog) =>

    }
  }
}
