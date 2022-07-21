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

import com.fasterxml.jackson.core.io.JsonEOFException
import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import io.delta.standalone.{DeltaLog, Operation}
import io.delta.standalone.expressions.{And, Column, EqualTo, Expression, GreaterThanOrEqual, IsNotNull, LessThanOrEqual, Literal}
import io.delta.standalone.types.{LongType, StringType, StructField, StructType}

import io.delta.standalone.internal.actions.{Action, AddFile, Metadata}
import io.delta.standalone.internal.util.DataSkippingUtils
import io.delta.standalone.internal.util.DataSkippingUtils.{MAX, MIN, NULL_COUNT, NUM_RECORDS}
import io.delta.standalone.internal.util.TestUtils._

class DataSkippingSuite extends FunSuite {
  private val op = new Operation(Operation.Name.WRITE)

  private val partitionSchema = new StructType(Array(
    new StructField("partitionCol", new LongType(), true)
  ))

  private val schema = new StructType(Array(
    new StructField("partitionCol", new LongType(), true),
    new StructField("col1", new LongType(), true),
    new StructField("col2", new LongType(), true),
    new StructField("stringCol", new StringType(), true)
  ))

  private val nestedSchema = new StructType(Array(
    new StructField("normalCol", new LongType(), true),
    new StructField("parentCol", new StructType(Array(
      new StructField("subCol1", new LongType(), true),
      new StructField("subCol2", new LongType(), true)
    )), true)))

  val metadata: Metadata = Metadata(partitionColumns = partitionSchema.getFieldNames,
    schemaString = schema.toJson)


  private val partitionColValue = (i: Int) => i.toString
  private val col1Value = (i: Int) => (i % 3).toString
  private val col2Value = (i: Int) => (i % 4).toString

  def buildFiles(
      customStats: Option[Int => String] = None,
      isStrColHasValue: Boolean = false): Seq[AddFile] = (1 to 20).map { i =>
    val stringColValue = if (isStrColHasValue) "\"a\"" else "null"
    val partitionValues = Map("partitionCol" -> partitionColValue(i))
    val fullColumnStats = s"""
      | {
      |   "$MIN": {
      |     "partitionCol":${partitionColValue(i)},
      |     "col1":${col1Value(i)},
      |     "col2":${col2Value(i)},
      |     "stringCol":$stringColValue
      |   },
      |   "$MAX": {
      |     "partitionCol":${partitionColValue(i)},
      |     "col1":${col1Value(i)},
      |     "col2":${col2Value(i)},
      |     "stringCol":$stringColValue
      |   },
      |   "$NULL_COUNT": {
      |     "partitionCol": 0,
      |     "col1": 0,
      |     "col2": 0,
      |     "stringCol": 1
      |   },
      |   "$NUM_RECORDS":1
      | }
      |"""

    val columnStats = (if (customStats.isDefined) customStats.get(i) else fullColumnStats)
      .stripMargin.split('\n').map(_.trim.filter(_ >= ' ')).mkString
    // We need to wrap the stats string since it will be parsed twice. Once when AddFile is parsed
    // in LogReplay, and once when stats string it self parsed in DataSkippingUtils.parseColumnStats
    val wrappedColumnStats = "\"" + columnStats.replace("\"", "\\\"") + "\""
    AddFile(i.toString, partitionValues, 1L, 1L, dataChange = true, stats = wrappedColumnStats)
  }

  private val nestedFiles = {
    val normalCol = 1
    val subCol1 = 2
    val subCol2 = 3
    val nestedColStats = s"""
      | {
      |   "${MIN}": {
      |     "normalCol":$normalCol,
      |     "parentCol": {
      |       "subCol1":$subCol1,
      |       "subCol2":$subCol2,
      |     }
      |   },
      |   "${MAX}": {
      |     "normalCol":$normalCol,
      |     "parentCol": {
      |       "subCol1":$subCol1,
      |       "subCol2":$subCol2,
      |     }
      |   },
      |   "${NUM_RECORDS}":1
      | }
      |""".stripMargin.split('\n').map(_.trim.filter(_ >= ' ')).mkString
    Seq(AddFile(
      path = "nested",
      Map[String, String](),
      1L,
      1L,
      dataChange = true,
      stats = "\"" + nestedColStats.replace("\"", "\\\"") + "\""))
  }

  private val nestedMetadata: Metadata = Metadata(partitionColumns = Seq[String](),
    schemaString = nestedSchema.toJson)

  private val unwrappedStats = buildFiles().get(0).getStats.replace("\\\"", "\"")
    .dropRight(1).drop(1)

  private val brokenStats = unwrappedStats.substring(0, 10)

  // partition column now supports expression other than equal
  private val metadataConjunct = new LessThanOrEqual(schema.column("partitionCol"), Literal.of(5L))

  private val dataConjunct = new EqualTo(schema.column("col1"), Literal.of(1L))

  def withDeltaLog(actions: Seq[Action], isNested: Boolean) (l: DeltaLog => Unit): Unit = {
    withTempDir { dir =>
      val m = if (isNested) nestedMetadata else metadata
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(m :: Nil, op, "engineInfo")
      log.startTransaction().commit(actions, op, "engineInfo")
      l(log)
    }
  }


  /**
   * The method for integration tests with different query predicate.
   * @param expr              the input query predicate
   * @param target            the file list that is not skipped by evaluating column stats
   * @param customStats       the customized stats string. If none, use default stats
   * @param isStrColHasValue  whether testing with a non-null string value
   * @param isNestedSchema    whether using nested schema
   */
  def filePruningTest(
      expr: Expression,
      target: Seq[String],
      customStats: Option[Int => String] = None,
      isStrColHasValue: Boolean = false,
      isNestedSchema: Boolean = false): Unit = {
    val logFiles = if (isNestedSchema) nestedFiles else buildFiles(customStats, isStrColHasValue)
    withDeltaLog(logFiles, isNestedSchema) { log =>
      val scan = log.update().scan(expr)
      val iter = scan.getFiles
      var resFiles: Seq[String] = Seq()
      while (iter.hasNext) {
        // get the index of accepted files
        resFiles = resFiles :+ iter.next().getPath
      }
      assert(resFiles == target)
    }
  }

  /**
   * Integration test
   *
   * Description of the first integration test:
   *
   * - table schema: (partitionCol: long, col1: long, col2: long, stringCol: string)
   *
   * - `files`: rows of data in table, for the i-th file in `files`,
   *      file.path = i, file.partitionCol = i, file.col1 = i % 3, file.col2 = i % 4
   *
   * - range of `i` is from 1 to 20.
   *
   * - the query predicate is `partitionCol <= 5 AND col1 = 1`
   * - [[metadataConjunct]]: the partition predicate expr, which is `partitionCol <= 5`
   * - [[dataConjunct]]: the non-partition predicate expr, which is `col1 = 1`
   *
   * - the accepted files' number should meet the condition: (i <= 5 AND i % 3 == 1)
   *
   * - the output should be: 1, 4.
   */
  test("integration test: column stats filter on 1 partition and 1 non-partition column") {
    filePruningTest(expr = new And(metadataConjunct, dataConjunct),
      target = Seq("1", "4"))
  }

  /**
   * Filter: (i % 3 == 1 AND i % 4 == 1) (1 <= i <= 20)
   * Output: i = 1 or 13
   */
  test("integration test: column stats filter on 2 non-partition column") {
    filePruningTest(expr = new And(new EqualTo(schema.column("col1"), Literal.of(1L)),
        new EqualTo(schema.column("col2"), Literal.of(1L))),
      target = Seq("1", "13"))
  }

  /**
   * Filter: (i % 4 == 1 AND i % 4 == 1) (1 <= i <= 20)
   * Output: i = 1 or 5 or 9 or 13 or 17
   */
  test("integration test: multiple filter on 1 non-partition column - duplicate") {
    filePruningTest(expr = new And(new EqualTo(schema.column("col2"), Literal.of(1L)),
        new EqualTo(schema.column("col2"), Literal.of(1L))),
      target = Seq("1", "5", "9", "13", "17"))
  }

  /**
   * Filter: (i % 3 == 1 AND i % 3 == 2) (1 <= i <= 20)
   * Output: No file meets the condition
   */
  test("integration test: multiple filter on 1 non-partition column - conflict") {
    filePruningTest(expr = new And(new EqualTo(schema.column("col1"), Literal.of(1L)),
        new EqualTo(schema.column("col1"), Literal.of(2L))),
      target = Seq())
  }

  /**
   * Filter: (i <= 5 AND i % 3 == 2)
   * Output: i = 1 or 2 or 3 or 4 or 5 (i % 3 == 2 not work)
   * Reason: Because col2.MIN and col2.MAX is used in column stats predicate while not appears in
   * the stats string, we can't evaluate column stats predicate and will skip column stats filter.
   * But the partition column filter still works here.
   */
  test("integration test: missing stats type") {
    val incompleteColumnStats =
      s"""
         | {
         |   "${NULL_COUNT}": {
         |     "partitionCol": 0,
         |     "col1": 0,
         |     "col2": 0,
         |     "stringCol": 1
         |   },
         |   "${NUM_RECORDS}":1
         | }
         |"""
    filePruningTest(expr = new And(metadataConjunct,
        new EqualTo(schema.column("col2"), Literal.of(2L))),
      target = Seq("1", "2", "3", "4", "5"), Some(_ => incompleteColumnStats))
  }

  /**
   * Filter: (i % 3 == 1 AND i % 4 == 1)
   * Output: i = 1 to 20 (column stats based pruning not work)
   * Reason: Because col2.MIN and col2.MAX is used in column stats predicate while not appears in
   * the stats string, we can't evaluate column stats predicate and will skip column stats filter.
   */
  test("integration test: missing column in stats") {
    val incompleteColumnStats = (i: Int) =>
      s"""
         | {
         |   "${MAX}": {
         |     "partitionCol": ${partitionColValue(i)},
         |     "col1": ${col1Value(i)},
         |     "stringCol": null
         |   },
         |   ${MIN}": {
         |     "partitionCol": ${partitionColValue(i)},
         |     "col1": ${col1Value(i)},
         |     "stringCol": null
         |   },
         |   "${NULL_COUNT}": {
         |     "partitionCol": 0,
         |     "col1": 0,
         |     "stringCol": 1
         |   },
         |   "${NUM_RECORDS}":1
         | }
         |"""
    filePruningTest(expr = new And(new EqualTo(schema.column("col1"), Literal.of(1L)),
        new EqualTo(schema.column("col2"), Literal.of(1L))),
      target = (1 to 20).map(_.toString).toSeq, Some(incompleteColumnStats))
  }

  /**
   * Filter: (i <= 5 AND i % 4 == 1)
   * Output: i = 1 or 2 or 3 or 4 or 5
   * Reason: Because stats string is empty, we can't evaluate column stats predicate and will skip
   * column stats filter. But the partition column still works here.
   */
  test("integration test: empty stats str") {
    filePruningTest(expr = new And(metadataConjunct,
        new EqualTo(schema.column("col1"), Literal.of(1L))),
      target = Seq("1", "2", "3", "4", "5"), customStats = Some(_ => "\"\""))
  }

  /**
   * Filter: (i <= 5 AND i % 4 == 1)
   * Output: i = 1 or 2 or 3 or 4 or 5
   * Reason: Because stats string is broken, we can't evaluate column stats predicate and will skip
   * column stats filter. But the partition column still works here. The JSON parser error is caught
   * in [[io.delta.standalone.internal.scan.FilteredDeltaScanImpl]].
   */
  test("integration test: broken stats str") {
    filePruningTest(expr = new And(metadataConjunct,
        new EqualTo(schema.column("col1"), Literal.of(1L))),
      target = Seq("1", "2", "3", "4", "5"), customStats = Some(_ => brokenStats))
  }

  /**
   * Filter: (i <= 5 AND i == "a")
   * Output: i = 1 or 2 or 3 or 4 or 5
   * Reason: Because string type is currently unsupported, we can't evaluate column stats
   * predicate and will skip column stats filter.
   */
  test("integration test: unsupported stats data type") {
    filePruningTest(expr = new And(metadataConjunct,
        new EqualTo(schema.column("stringCol"), Literal.of("1"))),
      target = Seq("1", "2", "3", "4", "5"), isStrColHasValue = true)
  }

  /**
   * Filter: (i <= 5 AND i % 3 <= 1)
   * Output: i = 1 or 2 or 3 or 4 or 5
   * Reason: Because LessThanOrEqual is currently unsupported, we can't evaluate column stats
   * predicate and will skip column stats filter.
   */
  test("integration test: unsupported expression type") {
    filePruningTest(expr = new And(metadataConjunct,
        new LessThanOrEqual(schema.column("col1"), Literal.of(1L))),
      target = Seq("1", "2", "3", "4", "5"))
  }

  /**
   * Filter: (parentCol.subCol1 == 1)
   * Output: path = nested
   * Reason: The nested file still returned though it is not qualified in the query predicate.
   * Because nested tables are not supported.
   */
  test("integration test: unsupported nested column") {
    filePruningTest(expr = new EqualTo(nestedSchema.column("normalCol"), Literal.of(1L)),
      target = Seq("nested"), isNestedSchema = true)
  }
}
