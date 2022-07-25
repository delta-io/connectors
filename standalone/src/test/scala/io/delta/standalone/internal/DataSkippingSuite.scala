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

import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import io.delta.standalone.{DeltaLog, Operation}
import io.delta.standalone.expressions.{And, EqualTo, Expression, LessThanOrEqual, Literal}
import io.delta.standalone.types.{LongType, StringType, StructField, StructType}

import io.delta.standalone.internal.actions.{Action, AddFile, Metadata}
import io.delta.standalone.internal.util.DataSkippingUtils.{MAX, MIN, NULL_COUNT, NUM_RECORDS}
import io.delta.standalone.internal.util.TestUtils._

/**
 * The integration test of column stats based file pruning in
 * [[io.delta.standalone.internal.scan.FilteredDeltaScanImpl]].
 */
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


  val metadata: Metadata = Metadata(partitionColumns = partitionSchema.getFieldNames,
    schemaString = schema.toJson)

  def buildFiles(
      customStats: Option[Int => String] = None,
      strColHasValue: Boolean = false): Seq[AddFile] = (1 to 20).map { i =>
    val stringColValue = if (strColHasValue) "\"a\"" else "null"
    val partitionValues = Map("partitionCol" -> i.toString)
    val fullColumnStats = s"""
      | {
      |   "$NUM_RECORDS":1,
      |   "$MIN": {
      |     "col1":${(i % 3).toString},
      |     "col2":${(i % 4).toString},
      |     "stringCol":$stringColValue
      |   },
      |   "$MAX": {
      |     "col1":${(i % 3 + 2).toString},
      |     "col2":${(i % 4 + 1).toString},
      |     "stringCol":$stringColValue
      |   },
      |   "$NULL_COUNT": {
      |     "col1": 0,
      |     "col2": 0,
      |     "stringCol": 1
      |   }
      | }
      |"""

    val columnStats = (if (customStats.isDefined) customStats.get(i) else fullColumnStats)
      .stripMargin.split('\n').map(_.trim.filter(_ >= ' ')).mkString

    // We need to wrap the stats string since it will be parsed twice: Once when AddFile is parsed
    // in LogReplay, and once when stats string it self parsed in DataSkippingUtils.parseColumnStats
    val wrappedColumnStats = "\"" + columnStats.replace("\"", "\\\"") + "\""
    AddFile(i.toString, partitionValues, 1L, 1L, dataChange = true, stats = wrappedColumnStats)
  }

  private val col1EqualTo1 = new EqualTo(schema.column("col1"), Literal.of(1L))

  def withDeltaLog(actions: Seq[Action], m: Option[Metadata] = None) (f: DeltaLog => Unit): Unit = {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(m.getOrElse(metadata) :: Nil, op, "engineInfo")
      log.startTransaction().commit(actions, op, "engineInfo")
      f(log)
    }
  }

  /**
   * Integration tests with given query predicate, target output and configurations. For each method
   * call, this method will test twice. Once with only the column stats filter, and once with column
   * stats filter and partition filter.
   *
   * @param expr              The input query predicate.
   * @param target            The file list that is not skipped by evaluating column stats.
   * @param customStats       The customized stats string. If none, use default stats.
   * @param isStrColHasValue  Whether testing with a non-null string value.
   */
  def filePruningTest(
      expr: Expression,
      target: Seq[String],
      customStats: Option[Int => String] = None,
      isStrColHasValue: Boolean = false): Unit = {
    val logFiles = buildFiles(customStats, isStrColHasValue)

    // Case 1: Test with only column stats predicates.
    withDeltaLog(logFiles) { log =>
      val scan = log.update().scan(expr)
      val iter = scan.getFiles
      var resFiles: Seq[String] = Seq()
      while (iter.hasNext) {
        // Get the index of accepted files.
        resFiles = resFiles :+ iter.next().getPath
      }
      assert(resFiles == target)
    }

    // Case 2: Test with column stats predicates and partition filter `partitionCol <= 10`.
    withDeltaLog(logFiles) { log =>
      val scan = log.update().scan(new And(
        new LessThanOrEqual(schema.column("partitionCol"), Literal.of(10L)),
        expr))
      val iter = scan.getFiles
      var resFiles: Seq[String] = Seq()
      while (iter.hasNext) {
        // Get the index of accepted files.
        resFiles = resFiles :+ iter.next().getPath
      }
      assert(resFiles == target.filter(_.toLong <= 10L))
    }
  }

  /**
   * Integration test
   *
   * Description of the first test:
   *
   * - table schema: (partitionCol: long, col1: long, col2: long, stringCol: string)
   *
   * - `files`: rows of data in table, for the i-th file in `files`,
   *      file.path = i, file.partitionCol = i, file.col1 = i % 3, file.col2 = i % 4
   *
   * - range of `i` is from 1 to 20.
   *
   * - the query predicate is `partitionCol <= 5 AND col1 = 1`
   * - partition column predicate: the partition predicate expr, which is `partitionCol <= 5`
   * - data column predicate: the non-partition predicate expr, which is `col1 == 1`
   *
   * - the accepted files' number should meet the condition:
   *    (i <= 5 && i % 3 <= 1 && i % 3 + 2 >= 1) (1 <= i <= 20)
   */
  test("integration test: column stats filter on 1 partition and 1 non-partition column") {
    val expectedResult = (1 to 20)
      .filter { i => i <= 5 && i % 3 <= 1 && i % 3 + 2 >= 1 }
      .map(_.toString)
    filePruningTest(expr = new And(
      new LessThanOrEqual(schema.column("partitionCol"), Literal.of(5L)), col1EqualTo1),
      expectedResult)
  }

  /**
   * Query: (col1 == 1 && col2 == 1) (1 <= i <= 20)
   * Column stats filter: (i % 3 <= 1 && i % 3 + 2 >= 1 && i % 4 <= 1 && i % 4 + 1 >= 1)
   */
  test("integration test: column stats filter on 2 non-partition column") {
    val expectedResult = (1 to 20)
      .filter { i => i % 3 <= 1 && i % 3 + 2 >= 1 && i % 4 <= 1 && i % 4 + 1 >= 1 }
      .map(_.toString)
    filePruningTest(
      expr = new And(
        new EqualTo(schema.column("col1"), Literal.of(1L)),
        new EqualTo(schema.column("col2"), Literal.of(1L))),
      expectedResult)
  }

  /**
   * Filter: (col2 == 1 AND col2 == 1) (1 <= i <= 20)
   * Column stats filter: (i % 4 <= 1 && i % 4 + 1 >= 1 && i % 4 <= 1 && i % 4 + 1 >= 1)
   */
  test("integration test: multiple filter on 1 non-partition column - duplicate") {
    val expectedResult = (1 to 20)
      .filter { i => i % 4 <= 1 && i % 4 + 1 >= 1 && i % 4 <= 1 && i % 4 + 1 >= 1 }
      .map(_.toString)
    filePruningTest(
      expr = new And(
        new EqualTo(schema.column("col2"), Literal.of(1L)),
        new EqualTo(schema.column("col2"), Literal.of(1L))),
      expectedResult)
  }

  /**
   * Filter: (col2 == 1 AND col2 == 2) (1 <= i <= 20)
   * Column stats filter: (i % 4 <= 1 && i % 4 + 1 >= 1 && i % 4 <= 2 && i % 4 + 1 >= 2)
   */
  test("integration test: multiple filter on 1 non-partition column - conflict") {
    val expectedResult = (1 to 20)
      .filter { i => i % 4 <= 1 && i % 4 + 1 >= 1 && i % 4 <= 2 && i % 4 + 1 >= 2 }
      .map(_.toString)
    filePruningTest(
      expr = new And(
        new EqualTo(schema.column("col2"), Literal.of(1L)),
        new EqualTo(schema.column("col2"), Literal.of(2L))),
      expectedResult)
  }

  /**
   * Filter: (col1 == 2)
   * Column stats filter: (i % 3 <= 2 && i % 3 + 2 >= 2)
   * Output: Return all files. (Column stats filter not work)
   * Reason: Because col2.MIN and col2.MAX is used in column stats predicate while not appears in
   * the stats string, we can't evaluate column stats predicate and will skip column stats filter.
   * But the partition column filter still works here.
   */
  test("integration test: some stats type missing") {
    val statsWithMissingType =
      s"""{"$NULL_COUNT":{"partitionCol": 0,"col1": 0,"col2": 0,"stringCol": 1},"$NUM_RECORDS":1}"""
    filePruningTest(
      expr = new EqualTo(schema.column("col1"), Literal.of(2L)),
      target = (1 to 20).map(_.toString), Some(_ => statsWithMissingType))
  }

  /**
   * Filter: (col1 == 1 AND col2 == 1)
   * Column stats filter: (i % 3 <= 1 && i % 3 + 2 >= 1 && i % 4 <= 1 && i % 4 + 1 >= 1)
   * Output: All files. (Column stats filter not work)
   * Reason: Because col2.MIN and col2.MAX is used in column stats predicate while not appears in
   * the stats string, we can't evaluate column stats predicate and will skip column stats filter.
   */
  test("integration test: missing stats for some column") {
    val incompleteColumnStats = (i: Int) =>
      s"""
         | {
         |   "$MAX": {
         |     "col1": ${(i % 3 + 2).toString},
         |     "stringCol": null
         |   },
         |   $MIN": {
         |     "col1": ${(i % 3).toString},
         |     "stringCol": null
         |   },
         |   "$NULL_COUNT": {
         |     "col1": 0,
         |     "stringCol": 1
         |   },
         |   "$NUM_RECORDS":1
         | }
         |"""
    filePruningTest(
      expr = new And(
        new EqualTo(schema.column("col1"), Literal.of(1L)),
        new EqualTo(schema.column("col2"), Literal.of(1L))),
      target = (1 to 20).map(_.toString), Some(incompleteColumnStats))
  }

  /**
   * Filter: (col2 == 1)
   * Column stats filter: (i % 4 <= 1 && i % 4 + 1 >= 1)
   * Output: All files. (Column stats filter not work)
   * Reason: Because stats string is empty, we can't evaluate column stats predicate and will skip
   * column stats filter. But the partition column still works here.
   */
  test("integration test: empty stats str") {
    filePruningTest(
      expr = new EqualTo(schema.column("col1"), Literal.of(1L)),
      target = (1 to 20).map(_.toString), customStats = Some(_ => "\"\""))
  }

  /**
   * Filter: (col2 == 1)
   * Column stats filter: (i % 4 <= 1 && i % 4 + 1 >= 1)
   * Output: All files. (Column stats filter not work)
   * Reason: Because stats string is broken, we can't evaluate column stats predicate and will skip
   * column stats filter. But the partition column still works here. The JSON parser error is caught
   * in [[io.delta.standalone.internal.scan.FilteredDeltaScanImpl]].
   */
  test("integration test: broken stats str") {
    // This stats string is wrapped in the `AddFile` we unwrap it first when we want to use it
    val unwrappedStats = buildFiles().get(0).getStats.replace("\\\"", "\"")
      .dropRight(1)
      .drop(1)

    val brokenStats = unwrappedStats.substring(0, 10)

    filePruningTest(
      expr = new EqualTo(schema.column("col1"), Literal.of(1L)),
      target = (1 to 20).map(_.toString), customStats = Some(_ => brokenStats))
  }

  /**
   * Filter: (stringCol == "a")
   * Column stats filter: None
   * Output: All files.
   * Reason: Because string type is currently unsupported, we can't evaluate column stats
   * predicate and will skip column stats filter.
   */
  test("integration test: unsupported stats data type") {
    filePruningTest(
      expr = new EqualTo(schema.column("stringCol"), Literal.of("a")),
      target = (1 to 20).map(_.toString), isStrColHasValue = true)
  }

  /**
   * Filter: (i % 3 <= 1)
   * Column stats filter: None
   * Output: All files.
   * Reason: Because LessThanOrEqual is currently unsupported in building column stats predicate,
   * the column stats filter will be empty and return all the files.
   */
  test("integration test: unsupported expression type") {
    filePruningTest(
      expr = new LessThanOrEqual(schema.column("col1"), Literal.of(1L)),
      target = (1 to 20).map(_.toString))
  }

  /**
   * Filter: (normalCol == 5)
   * Column stats filter: empty
   * Output: All files.
   * Reason: The nested table will not filtered by column stats predicate.
   * Because they are not supported.
   */
  test("integration test: unsupported nested column") {
    val nestedSchema = new StructType(Array(
      new StructField("normalCol", new LongType(), true),
      new StructField("parentCol", new StructType(Array(
        new StructField("subCol1", new LongType(), true),
        new StructField("subCol2", new LongType(), true)
      )), true)))

    val nestedMetadata: Metadata = Metadata(partitionColumns = Seq[String](),
      schemaString = nestedSchema.toJson)

    val nestedFiles = {
      val nestedColStats = s"""
        | {
        |   "$NUM_RECORDS":1,
        |   "$MIN": {
        |     "normalCol":1,
        |     "parentCol": {
        |       "subCol1":2,
        |       "subCol2":3,
        |     }
        |   },
        |   "$MAX": {
        |     "normalCol":4,
        |     "parentCol": {
        |       "subCol1":5,
        |       "subCol2":6,
        |     }
        |
        | }
        |""".stripMargin.split('\n').map(_.trim.filter(_ >= ' ')).mkString
      Seq(AddFile(path = "nested", Map[String, String](), 1L, 1L, dataChange = true,
        stats = "\"" + nestedColStats.replace("\"", "\\\"") + "\""))
    }

    val expr = new EqualTo(nestedSchema.column("normalCol"), Literal.of(1L))
    val target = Seq("nested")
    val logFiles = nestedFiles
    withDeltaLog(logFiles, Some(nestedMetadata)) { log =>
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
}
