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

package io.delta.alpine.internal

import java.io.File

import collection.JavaConverters._

import io.delta.alpine.internal.storage.ReadOnlyLogStoreProvider
import io.delta.alpine.internal.util.ConversionUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

abstract class ReadOnlyLogStoreSuite extends QueryTest
  with ReadOnlyLogStoreProvider
  with SharedSparkSession
  with ConversionUtils {

//  def logStoreClassName: String

//  protected override def sparkConf = {
//    super.sparkConf.set(DeltaLogStore.logStoreClassConfKey, logStoreClassName)
//  }
//
//  test("instantiation through SparkConf") {
//    assert(spark.sparkContext.getConf.get(DeltaLogStore.logStoreClassConfKey) ==
  //    logStoreClassName)
//    assert(DeltaLogStore(spark.sparkContext).getClass.getName == logStoreClassName)
//  }

  test("read") {
    withTempDir { tempDir =>
      val writeStore = LogStore(spark.sparkContext)
      val hadoopConf = spark.sessionState.newHadoopConf()
      val readStore = createLogStore(hadoopConf)

      val deltas = Seq(0, 1).map(i => new File(tempDir, i.toString)).map(_.getCanonicalPath)
      writeStore.write(deltas.head, Iterator("zero", "none"))
      writeStore.write(deltas(1), Iterator("one"))

      assert(readStore.read(deltas.head).asScala == Seq("zero", "none"))
      assert(readStore.read(deltas(1)).asScala == Seq("one"))
    }
  }

  test("listFrom") {
    withTempDir { tempDir =>
      val writeStore = LogStore(spark.sparkContext)
      val hadoopConf = spark.sessionState.newHadoopConf()
      val readStore = createLogStore(hadoopConf)

      val deltas = Seq(0, 1, 2, 3, 4)
        .map(i => new File(tempDir, i.toString))
        .map(_.toURI)
        .map(new Path(_))

      writeStore.write(deltas(1), Iterator("zero"))
      writeStore.write(deltas(2), Iterator("one"))
      writeStore.write(deltas(3), Iterator("two"))
      assert(readStore.listFrom(deltas.head).asScala.map(_.getPath.getName).toArray ===
        Seq(1, 2, 3).map(_.toString))
      assert(readStore.listFrom(deltas(1)).asScala.map(_.getPath.getName).toArray ===
        Seq(1, 2, 3).map(_.toString))
      assert(readStore.listFrom(deltas(2)).asScala.map(_.getPath.getName).toArray ===
        Seq(2, 3).map(_.toString))
      assert(readStore.listFrom(deltas(3)).asScala.map(_.getPath.getName).toArray ===
        Seq(3).map(_.toString))
      assert(readStore.listFrom(deltas(4)).asScala.map(_.getPath.getName).toArray === Nil)
    }
  }
}

// TODO: better tests
class HDFSReadOnlyLogStoreSuite extends ReadOnlyLogStoreSuite {
//  override val logStoreClassName: String = classOf[HDFSReadOnlyLogStore].getName
}
