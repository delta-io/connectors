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
import io.delta.standalone.expressions.{And, EqualTo, Expression, Literal}
import io.delta.standalone.types.{LongType, StructField, StructType}

import io.delta.standalone.internal.actions.{Action, AddFile, Metadata}
import io.delta.standalone.internal.util.TestUtils._

class DataSkippingSuite extends FunSuite {
  private val op = new Operation(Operation.Name.WRITE)

  private val partitionSchema = new StructType(Array(
    new StructField("col1", new LongType(), true)
  ))

  private val schema = new StructType(Array(
    new StructField("col1", new LongType(), true),
    new StructField("col2", new LongType(), true),
    new StructField("col3", new LongType(), true)
  ))

  val metadata: Metadata = Metadata(partitionColumns = partitionSchema.getFieldNames,
    schemaString = schema.toJson)

  private val files = (1 to 20).map { i =>
    val col1Value = (i % 2).toString
    val col2Value = (i % 3).toString
    val col3Value = (i % 4).toString
    val partitionValues = Map("col1" -> col1Value)
    val flatColumnStats = ("\"" + s"""
      | {
      |   "minValues": {
      |     "col1":$col1Value,
      |     "col2":$col2Value,
      |     "col3":$col3Value
      |   },
      |   "maxValues": {
      |     "col1":$col1Value,
      |     "col2":$col2Value,
      |     "col3":$col3Value
      |   },
      |   "nullCount": {
      |     "col1": 0,
      |     "col2": 0,
      |     "col3": 0
      |   },
      |   "numRecords":1
      | }
      |""".replace("\"", "\\\"")
      + "\"").stripMargin.split('\n').map(_.trim.filter(_ >= ' ')).mkString

    AddFile(i.toString, partitionValues, 1L, 1L, dataChange = true, stats = flatColumnStats)
  }

  private val metadataConjunct = new EqualTo(schema.column("col1"), Literal.of(1L))

  private val dataConjunct = new EqualTo(schema.column("col2"), Literal.of(1L))

  def withLog(actions: Seq[Action])(test: DeltaLog => Unit): Unit = {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(metadata :: Nil, op, "engineInfo")
      log.startTransaction().commit(actions, op, "engineInfo")

      test(log)
    }
  }

  def withDeltaLog (l: DeltaLog => Unit): Unit = {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(metadata :: Nil, op, "engineInfo")
      log.startTransaction().commit(files, op, "engineInfo")
      l(log)
    }
  }

  def testExpression(expr: Expression, target: Seq[String]): Unit = {
    withDeltaLog { log =>
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

  /** Integration test */
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
