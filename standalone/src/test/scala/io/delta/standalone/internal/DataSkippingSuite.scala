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
import io.delta.standalone.expressions.{And, EqualTo, GreaterThanOrEqual, Literal}
import io.delta.standalone.types.{LongType, StringType, StructField, StructType}

import io.delta.standalone.internal.actions.{Action, AddFile, Metadata}
import io.delta.standalone.internal.util.DataSkippingUtils
import io.delta.standalone.internal.util.TestUtils._

class DataSkippingSuite extends FunSuite {
  private val op = new Operation(Operation.Name.WRITE)

  private val partitionSchema = new StructType(Array(
    new StructField("col2", new LongType(), true)
  ))

  private val schema = new StructType(Array(
    new StructField("col1", new LongType(), true),
    new StructField("col2", new LongType(), true)
  ))

  val metadata: Metadata = Metadata(
    partitionColumns = partitionSchema.getFieldNames, schemaString = schema.toJson)



  private val files = (1 to 20).map { i =>
    val col1Value = (i % 3).toString
    val col2Value = (i % 2).toString
    val partitionValues = Map("col2" -> col2Value)
    val flatColumnStats = ("\"" + s"""
      | {
      |   "minValues": {
      |     "col1":$col1Value,
      |     "col2":$col2Value
      |   },
      |   "maxValues": {
      |     "col1":$col1Value,
      |     "col2":$col2Value
      |   },
      |   "nullCount": {
      |     "col1": 0,
      |     "col2": 0
      |   },
      |   "numRecords":1
      | }
      |""".replace("\"", "\\\"")
      + "\"").stripMargin.split('\n').map(_.trim.filter(_ >= ' ')).mkString

    AddFile(i.toString, partitionValues, 1L, 1L, dataChange = true, stats = flatColumnStats)
  }

  private val metadataConjunct = new EqualTo(schema.column("col2"), Literal.of(1L))

  private val dataConjunct = new EqualTo(schema.column("col1"), Literal.of(1L))

  def withLog(actions: Seq[Action])(test: DeltaLog => Unit): Unit = {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(metadata :: Nil, op, "engineInfo")
      log.startTransaction().commit(actions, op, "engineInfo")

      test(log)
    }
  }

  // A sketchy demo:
  //
  // table schema: (col1: int, col2: int), col2 is partition column
  //
  // `files`: rows of data in table, for the i-th file in `files`,
  //    file.path = i, file.col1 = i % 3, file.col2 = i % 2
  //
  // range of `i` is from 1 to 20.
  //
  // the query predicate is `col1 = 1 AND col2 = 1`
  // `metadataConjunct`: the partition predicate expr, which is `col2 = 1`
  // `dataConjunct`: the non-partition predicate expr, which is `col1 = 1`
  //
  // the accepted files should meet the condition: (i % 3 = 1 AND i % 2 = 1)
  // The output should be: 1, 7, 13, 19.
  withLog(files) { log =>
    val scan = log.update().scan(new And(dataConjunct, metadataConjunct))
    val iter = scan.getFiles
    var resFiles: Seq[String] = Seq()
    while (iter.hasNext) {
      // get the index of accepted files
      resFiles = resFiles :+ iter.next().getPath
    }
    assert(resFiles.length == 4)
    assert(resFiles == Seq("1", "7", "13", "19"))
  }
}
