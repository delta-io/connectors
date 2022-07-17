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
import io.delta.standalone.expressions.{And, Column, EqualTo, Expression, GreaterThanOrEqual, IsNotNull, LessThanOrEqual, Literal}
import io.delta.standalone.types.{LongType, StringType, StructField, StructType}

import io.delta.standalone.internal.actions.{Action, AddFile, Metadata}
import io.delta.standalone.internal.util.DataSkippingUtils
import io.delta.standalone.internal.util.TestUtils._

class DataSkippingSuite extends FunSuite {
  private val op = new Operation(Operation.Name.WRITE)

  private val partitionSchema = new StructType(Array(
    new StructField("col1", new LongType(), true)
  ))

  private val schema = new StructType(Array(
    new StructField("col1", new LongType(), true),
    new StructField("col2", new LongType(), true),
    new StructField("col3", new LongType(), true),
    new StructField("col4", new StringType(), true)
  ))

  val metadata: Metadata = Metadata(partitionColumns = partitionSchema.getFieldNames,
    schemaString = schema.toJson)

  private val files = (1 to 20).map { i =>
    val col1Value = (i % 2).toString
    val col2Value = (i % 3).toString
    val col3Value = (i % 4).toString
    val col4Value = "null"
    val partitionValues = Map("col1" -> col1Value)
    val flatColumnStats = ("\"" + s"""
      | {
      |   "minValues": {
      |     "col1":$col1Value,
      |     "col2":$col2Value,
      |     "col3":$col3Value,
      |     "col4":$col4Value
      |   },
      |   "maxValues": {
      |     "col1":$col1Value,
      |     "col2":$col2Value,
      |     "col3":$col3Value,
      |     "col4":$col4Value
      |   },
      |   "nullCount": {
      |     "col1": 0,
      |     "col2": 0,
      |     "col3": 0,
      |     "col4": 1
      |   },
      |   "numRecords":1
      | }
      |""".replace("\"", "\\\"")
      + "\"").stripMargin.split('\n').map(_.trim.filter(_ >= ' ')).mkString

    AddFile(i.toString, partitionValues, 1L, 1L, dataChange = true, stats = flatColumnStats)
  }

  private val metadataConjunct = new EqualTo(schema.column("col1"), Literal.of(1L))

  private val dataConjunct = new EqualTo(schema.column("col2"), Literal.of(1L))

  def withDeltaLog(actions: Seq[Action]) (l: DeltaLog => Unit): Unit = {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(metadata :: Nil, op, "engineInfo")
      log.startTransaction().commit(actions, op, "engineInfo")
      l(log)
    }
  }

  def testExpression(expr: Expression, target: Seq[String]): Unit = {
    withDeltaLog(files) { log =>
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
   * The unit test function for constructDataFilter.
   * @param in     input query predicate
   * @param target output column stats predicate from [[DataSkippingUtils.constructDataFilters]]
   *               in string, will be None if the method returned empty expression.
   */
  def constructDataFilterTest(
      in: Expression,
      target: Option[String],
      isSchemaMissing: Boolean = false): Unit = {
    val tableSchema = if (isSchemaMissing) new StructType(Array()) else schema
    val output = DataSkippingUtils.constructDataFilters(
      tableSchema = tableSchema,
      expression = in)

    assert(output.isDefined == target.isDefined)
    if (target.isDefined) {
      assert(target.get == output.get.expr.toString)
    }
  }

  /**
   * Unit test - constructDataFilters
   */
  test("filter construction: (col2 = 1)") {
    constructDataFilterTest(
      in = new EqualTo(new Column("col2", new LongType), Literal.of(1L)),
      target = Some("((Column(col2.minValues) <= 1) && (Column(col2.maxValues) >= 1))")
    )}

  test("filter construction: (col2 = 1 && col3 = 1)") {
    constructDataFilterTest(
      in = new And(new EqualTo(new Column("col2", new LongType), Literal.of(1L)),
        new EqualTo(new Column("col3", new LongType), Literal.of(1L))),
      target = Some("(((Column(col2.minValues) <= 1) && (Column(col2.maxValues) >= 1)) &&" +
        " ((Column(col3.minValues) <= 1) && (Column(col3.maxValues) >= 1)))")
    )}

  test("filter construction: (col2 >= 1) the expression '>=' is not supported") {
    constructDataFilterTest(
      in = new GreaterThanOrEqual(new Column("col2", new LongType), Literal.of(1L)),
      target = None
    )}

  test("filter construction: (col2 IS NOT NULL) the expression 'IsNotNull' is not supported") {
    constructDataFilterTest(
      in = new IsNotNull(new Column("col2", new LongType)),
      target = None
    )}

  test("filter construction: (col4 = 1) null value in stats will be ignored") {
    constructDataFilterTest(
      in = new EqualTo(new Column("col4", new LongType), Literal.of(1L)),
      target = None
    )}

  test("filter construction: (col2 = 1) empty expression will return if schema is missing") {
    constructDataFilterTest(
      in = new EqualTo(new Column("col2", new LongType), Literal.of(1L)),
      target = None,
      isSchemaMissing = true
    )}

  /** Integration test */
  // empty, MIN, MAX, NUM_RECORDS, NULL_VALUE, nested col - discard, unknown col


  // A sketchy demo:
  //
  // table schema: (col1: int, col2: int, col3: int) s partition column
  //
  // `files`: rows of data in table, for the i-th file in `files`,
  //    file.path = i, file.col1 = i % 2, file.col2 = i % 3, file.col3 = i % 4
  //
  // range of `i` is from 1 to 20.
  //
  // the query predicate is `col1 = 1 AND col2 = 1`
  // `metadataConjunct`: the partition predicate expr, which is `col1 = 1`
  // `dataConjunct`: the non-partition predicate expr, which is `col2 = 1`
  //
  // the accepted files' number should meet the condition: (i % 3 == 1 AND i % 2 == 1)
  // The output should be: 1, 7, 13, 19.
  test("test column stats filter on 1 partition and 1 non-partition column") {
    testExpression(new And(metadataConjunct, dataConjunct), Seq("1", "7", "13", "19"))
  }

  test("test column stats filter on 2 non-partition column") {
    // Filter: (i % 3 == 1 AND i % 4 == 1) (1 <= i <= 20)
    // Output: i = 1 or 13
    testExpression(new And(new EqualTo(schema.column("col2"), Literal.of(1L)),
      new EqualTo(schema.column("col3"), Literal.of(1L))), Seq("1", "13"))
  }

  test("test multiple filter on 1 partition column - duplicate") {
    // Filter: (i % 4 == 1 AND i % 4 == 1) (1 <= i <= 20)
    // Output: i = 1 or 5 or 9 or 13 or 17
    testExpression(new And(new EqualTo(schema.column("col3"), Literal.of(1L)),
      new EqualTo(schema.column("col3"), Literal.of(1L))), Seq("1", "5", "9", "13", "17"))
  }

  test("test multiple filter on 1 partition column - impossible") {
    // Filter: (i % 3 == 1 AND i % 3 == 2) (1 <= i <= 20)
    // Output: No file meets the condition
    testExpression(new And(new EqualTo(schema.column("col2"), Literal.of(1L)),
      new EqualTo(schema.column("col2"), Literal.of(2L))), Seq())
  }
}
