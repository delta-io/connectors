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

package io.delta.standalone.internal

import java.util.TimeZone

import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import io.delta.standalone.DeltaLog
import io.delta.standalone.expressions.Literal
import io.delta.standalone.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

import io.delta.standalone.internal.actions.{AddFile, MemoryOptimizedLogReplay}
import io.delta.standalone.internal.scan.FilteredDeltaScanImpl
import io.delta.standalone.internal.storage.LogStoreProvider
import io.delta.standalone.internal.util.TestUtils._

class DataSkippingSuite extends FunSuite {
  /* The total number of records in the file. */
  final val NUM_RECORDS = "numRecords"
  /* The smallest (possibly truncated) value for a column. */
  final val MIN = "minValues"
  /* The largest (possibly truncated) value for a column. */
  final val MAX = "maxValues"
  /* The number of null values present for a column. */
  final val NULL_COUNT = "nullCount"

  private val partitionSchema = new StructType(Array(
    new StructField("col1", new IntegerType(), true)
  ))

  private val flatTableSchema = new StructType(Array(
    new StructField("col1", new IntegerType(), true),
    new StructField("col2", new StringType(), true)
  ))

  private val nestedTableSchema = new StructType(Array(
    new StructField("col1", new IntegerType(), true),
    new StructField("col2", new StructType(Array(
      new StructField("col2_1", new DateType(), true),
      new StructField("col2_2", new DoubleType(), true)
    )), true)
  ))

  // `col1` is missing compared to `nestedTableSchema`
  private val incompleteTableSchema = new StructType(Array(
    new StructField("col2", new StructType(Array(
      new StructField("col2_1", new DateType(), true),
      new StructField("col2_2", new DoubleType(), true)
    )), true)
  ))

  private val flatColumnStats = """
      | {
      |   "minValues": {
      |     "col1":11,
      |     "col2":"test"
      |   },
      |   "maxValues": {
      |     "col1":22,
      |     "col2":"vest"
      |   },
      |   "numRecords":2
      | }
      |""".stripMargin

  private val nestedColumnStats = """
      | {
      |   "numRecords":1,
      |   "minValues": {
      |     "col1":11,
      |     "col2": {
      |       "col2_1":"JUL11",
      |       "col2_2":11.1
      |     }
      |   }
      | }
      |""".stripMargin

  // Column `col2_2` is missing compared to `nestedColumnStats`
  private val incompleteColumnStats = """
      | {
      |   "numRecords":1,
      |   "minValues": {
      |     "col1":11,
      |     "col2": {
      |       "col2_2":11.1
      |     }
      |   }
      | }
      |""".stripMargin

  /**
   * Creates a temporary [[FilteredDeltaScanImpl]] instance, which is then passed to `scan`
   */
  def withDeltaScan (scan: FilteredDeltaScanImpl => Unit): Unit = {
    withTempDir { dir =>
      val hadoopConf = new Configuration()
      DeltaLog.forTable(hadoopConf, dir.getCanonicalPath)

      val memoryOptimizedLogReplay = new MemoryOptimizedLogReplay(
        Nil,
        LogStoreProvider.createLogStore(hadoopConf),
        hadoopConf,
        TimeZone.getDefault)

      val filteredScan = new FilteredDeltaScanImpl(
        memoryOptimizedLogReplay,
        expr = Literal.True,
        partitionSchema)

      scan(filteredScan)
    }
  }

  def testStatsParser(
      tableSchema: StructType,
      columnStats: String,
      conditions: Seq[FilteredDeltaScanImpl => Boolean]): Unit = {
    withDeltaScan { scan =>
      val file = AddFile("flat file", Map(), 1L, 1L, dataChange = true, stats = columnStats)
      scan.parseColumnStats(tableSchema, file)
      conditions.foreach { cond =>
        assert(cond(scan))
      }
    }
  }

  /**
   * Phase 1: Parse and store the column stats from AddFile
   */
  test("unit test 1.1: Parse and store stats with data columns") {
    testStatsParser(flatTableSchema, flatColumnStats, Seq(
      _.findColumnNode(Seq()).orNull.statsValue(NUM_RECORDS) == "2",
      _.findColumnNode(Seq("col1")).orNull.statsValue(MIN) == "11",
      _.findColumnNode(Seq("col1")).orNull.statsValue(MAX) == "22",
      _.findColumnNode(Seq("col1")).orNull.columnPath == Seq("col1"),
      _.findColumnNode(Seq("col1")).orNull.dataType.getTypeName == "integer",
      _.findColumnNode(Seq("col2")).orNull.statsValue(MIN) == "test",
      _.findColumnNode(Seq("col2")).orNull.statsValue(MAX) == "vest",
      _.findColumnNode(Seq("col2")).orNull.columnPath == Seq("col2"),
      _.findColumnNode(Seq("col2")).orNull.dataType.getTypeName == "string"))
  }

  test("unit test 1.2: Parse and store stats with nested columns") {
    testStatsParser(nestedTableSchema, nestedColumnStats, Seq(
      _.findColumnNode(Seq()).orNull.statsValue(NUM_RECORDS) == "1",
      _.findColumnNode(Seq("col1")).orNull.statsValue(MIN) == "11",
      _.findColumnNode(Seq("col1")).orNull.columnPath == Seq("col1"),
      _.findColumnNode(Seq("col2", "col2_1")).orNull.statsValue(MIN) == "JUL11",
      _.findColumnNode(Seq("col2", "col2_1")).orNull.columnPath == Seq("col2", "col2_1"),
      _.findColumnNode(Seq("col2", "col2_1")).orNull.dataType.getTypeName == "date",
      _.findColumnNode(Seq("col2", "col2_2")).orNull.statsValue(MIN) == "11.1",
      _.findColumnNode(Seq("col2", "col2_2")).orNull.columnPath == Seq("col2", "col2_2"),
      _.findColumnNode(Seq("col2", "col2_2")).orNull.dataType.getTypeName == "double"))
  }

  test("unit test 1.3: The missing stats should remain as `null` in statsStore") {
    testStatsParser(nestedTableSchema, incompleteColumnStats, Seq(
      _.findColumnNode(Seq()).orNull.statsValue(NUM_RECORDS) == "1",
      _.findColumnNode(Seq("col2", "col2_1")).orNull == null,
      _.findColumnNode(Seq("col2", "col2_2")).orNull.statsValue(MIN) == "11.1",
      _.findColumnNode(Seq("col2", "col2_2")).orNull.columnPath == Seq("col2", "col2_2"),
      _.findColumnNode(Seq("col2", "col2_2")).orNull.dataType.getTypeName == "double"))
  }

  test("unit test 1.4: The stats' column cannot found in table schema should be discarded.") {
    testStatsParser(incompleteTableSchema, nestedColumnStats, Seq(
      _.findColumnNode(Seq()).orNull.statsValue(NUM_RECORDS) == "1",
      _.findColumnNode(Seq("col1")).orNull == null,
      _.findColumnNode(Seq("col2", "col2_1")).orNull.statsValue(MIN) == "JUL11",
      _.findColumnNode(Seq("col2", "col2_1")).orNull.columnPath == Seq("col2", "col2_1"),
      _.findColumnNode(Seq("col2", "col2_1")).orNull.dataType.getTypeName == "date",
      _.findColumnNode(Seq("col2", "col2_2")).orNull.statsValue(MIN) == "11.1",
      _.findColumnNode(Seq("col2", "col2_2")).orNull.columnPath == Seq("col2", "col2_2"),
      _.findColumnNode(Seq("col2", "col2_2")).orNull.dataType.getTypeName == "double"))
  }
}
