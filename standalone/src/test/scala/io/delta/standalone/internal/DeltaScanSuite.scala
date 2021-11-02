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

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import io.delta.standalone.{DeltaLog, Operation}
import io.delta.standalone.actions.{AddFile => AddFileJ}
import io.delta.standalone.expressions.{And, EqualTo, LessThan, Literal}
import io.delta.standalone.types.{IntegerType, StructField, StructType}

import io.delta.standalone.internal.actions.{Action, AddFile, Metadata}
import io.delta.standalone.internal.util.ConversionUtils
import io.delta.standalone.internal.util.TestUtils._

class DeltaScanSuite extends FunSuite {

  private val op = new Operation(Operation.Name.WRITE)

  private val schema = new StructType(Array(
    new StructField("col1", new IntegerType(), true),
    new StructField("col2", new IntegerType(), true),
    new StructField("col3", new IntegerType(), true),
    new StructField("col4", new IntegerType(), true)
  ))

  private val partitionSchema = new StructType(Array(
    new StructField("col1", new IntegerType(), true),
    new StructField("col2", new IntegerType(), true)
  ))

  private val files = (1 to 10).map { i =>
    val partitionValues = Map("col1" -> (i % 3).toString, "col2" -> (i % 2).toString)
    AddFile(i.toString, partitionValues, 1L, 1L, dataChange = true)
  }

  private val metadataConjunct = new EqualTo(schema.column("col1"), Literal.of(0))
  private val dataConjunct = new EqualTo(schema.column("col3"), Literal.of(5))

  def withLog(actions: Seq[Action])(test: DeltaLog => Unit): Unit = {
    val metadata = Metadata(
      partitionColumns = partitionSchema.getFieldNames, schemaString = schema.toJson)

    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(metadata :: Nil, op, "engineInfo")
      log.startTransaction().commit(actions, op, "engineInfo")

      test(log)
    }
  }

  test("properly splits metadata (pushed) and data (residual) predicates") {
    withLog(files) { log =>
      val mixedConjunct = new LessThan(schema.column("col2"), schema.column("col4"))
      val filter = new And(new And(metadataConjunct, dataConjunct), mixedConjunct)
      val scan = log.update().scan(filter)
      assert(scan.getPushedPredicate.get == metadataConjunct)
      assert(scan.getResidualPredicate.get == new And(dataConjunct, mixedConjunct))
    }
  }

  test("filtered scan with a metadata (pushed) conjunct should return matched files") {
    withLog(files) { log =>
      val filter = new And(metadataConjunct, dataConjunct)
      val scan = log.update().scan(filter)

      assert(scan.getFiles.asScala.toSeq.map(ConversionUtils.convertAddFileJ) ==
        files.filter(_.partitionValues("col1").toInt == 0))

      assert(scan.getPushedPredicate.get == metadataConjunct)
      assert(scan.getResidualPredicate.get == dataConjunct)
    }
  }

  test("filtered scan with only data (residual) predicate should return all files") {
    withLog(files) { log =>
      val filter = dataConjunct
      val scan = log.update().scan(filter)

      assert(scan.getFiles.asScala.toSeq.map(ConversionUtils.convertAddFileJ) == files)
      assert(!scan.getPushedPredicate.isPresent)
      assert(scan.getResidualPredicate.get == filter)
    }
  }

  test("correct reverse replay") {
    val addA_0 = AddFile("a", Map.empty, 100L, 100L, dataChange = true)
    val addA_1 = AddFile("a", Map.empty, 100L, 200L, dataChange = true)
    val addB_2_0 = AddFile("b", Map.empty, 100L, 300L, dataChange = true)
    val addB_2_1 = AddFile("b", Map.empty, 100L, 400L, dataChange = true)
    val addC_3 = AddFile("c", Map.empty, 100L, 500L, dataChange = true)
    val addD_4 = AddFile("d", Map.empty, 100L, 600L, dataChange = true)
    val removeC_5 = addC_3.removeWithTimestamp(700L)

    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(Metadata() :: Nil, op, "engineInfo")

      log.startTransaction().commit(addA_0 :: Nil, op, "engineInfo")
      log.startTransaction().commit(addA_1 :: Nil, op, "engineInfo")
      log.startTransaction().commit(addB_2_0 :: addB_2_1 :: Nil, op, "engineInfo")
      log.startTransaction().commit(addC_3 :: Nil, op, "engineInfo")
      log.startTransaction().commit(addD_4 :: Nil, op, "engineInfo")
      log.startTransaction().commit(removeC_5 :: Nil, op, "engineInfo")

      // addA_0 not returned since addA_1 was committed later and will be returned before it
      // addB_2_1 will not be returned since addB_2_0 will be written ahead in the .json delta file
      // addC_3 will not be returned since it was later deleted
      // addD_4 will be returned
      val expectedSet = Set(addA_1, addB_2_0, addD_4).map(ConversionUtils.convertAddFile)

      val set = new scala.collection.mutable.HashSet[AddFileJ]()
      val scan = log.update().scan()
      val iter = scan.getFiles
      while (iter.hasNext) {
        set += iter.next()
      }

      assert(set == expectedSet)

      iter.close()
    }

  }
}
