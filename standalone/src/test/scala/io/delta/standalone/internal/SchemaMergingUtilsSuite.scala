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

import org.scalatest.FunSuite

import io.delta.standalone.types._

import io.delta.standalone.internal.util.SchemaMergingUtils._

class SchemaMergingUtilsSuite extends FunSuite {
  ////////////////////////////
  // transformColumns
  ////////////////////////////

  test("transform columns - simple") {
    val base = new StructType()
      .add("a", new IntegerType)
      .add("b", new StringType)
    val update = new StructType()
      .add("c", new IntegerType)
      .add("b", new StringType)

    // Identity.
    var visitedFields = 0
    val res1 = transformColumns(base) {
      case (Seq(), field, _) =>
        visitedFields += 1
        field
    }
    assert(visitedFields === 2)
    assert(base === res1)

    // Rename a -> c
    visitedFields = 0
    val res2 = transformColumns(base) {
      case (Seq(), field, _) =>
        visitedFields += 1
        val name = field.getName
        field.withNewName(if (name == "a") "c" else name)
    }
    assert(visitedFields === 2)
    assert(update === res2)
  }

  test("transform element field type") {
    val base = new StructType()
      .add("a", new StructType()
        .add("element", new StringType()))

    val update = new StructType()
      .add("a", new StructType()
        .add("element", new IntegerType()))

    // Update type
    var visitedFields = 0
    val res = transformColumns(base) { (path, field, _) =>
      visitedFields += 1
      val dataType = path :+ field.getName match {
        case Seq("a", "element") => new IntegerType
        case _ => field.getDataType
      }
      field.withNewDataType(dataType)
    }
    assert(visitedFields === 2)
    assert(update === res)
  }

  test("transform array nested field type") {
    val nested = new StructType()
      .add("s1", new IntegerType)
      .add("s2", new LongType)
    val base = new StructType()
      .add("arr", new ArrayType(nested, true))

    val updatedNested = new StructType()
      .add("s1", new StringType)
      .add("s2", new LongType)
    val update = new StructType()
      .add("arr", new ArrayType(updatedNested, true))

    // Update type
    var visitedFields = 0
    val res = transformColumns(base) { (path, field, _) =>
      visitedFields += 1
      val dataType = path :+ field.getName match {
        case Seq("arr", "element", "s1") => new StringType
        case _ => field.getDataType
      }
      field.withNewDataType(dataType)
    }
    assert(visitedFields === 3)
    assert(update === res)
  }

  test("transform map nested field type") {
    val nested = new StructType()
      .add("s1", new IntegerType)
      .add("s2", new LongType)
    val base = new StructType()
      .add("m", new MapType(new StringType, nested, true))

    val updatedNested = new StructType()
      .add("s1", new StringType)
      .add("s2", new LongType)
    val update = new StructType()
      .add("m", new MapType(new StringType, updatedNested, true))

    // Update type
    var visitedFields = 0
    val res = transformColumns(base) { (path, field, _) =>
      visitedFields += 1
      val dataType = path :+ field.getName match {
        case Seq("m", "value", "s1") => new StringType
        case _ => field.getDataType
      }
      field.withNewDataType(dataType)
    }
    assert(visitedFields === 3)
    assert(update === res)
  }

  test("transform map type") {
    val base = new StructType()
      .add("m", new MapType(new StringType, new IntegerType, true))
    val update = new StructType()
      .add("m", new MapType(new StringType, new StringType, true))

    // Update type
    var visitedFields = 0
    val res = transformColumns(base) { (path, field, _) =>
      visitedFields += 1
      val dataType = path :+ field.getName match {
        case Seq("m") => new MapType(
          field.getDataType.asInstanceOf[MapType].getKeyType, new StringType(), true)
        case _ => field.getDataType
      }
      field.withNewDataType(dataType)
    }
    assert(visitedFields === 1)
    assert(update === res)
  }

  test("transform columns - nested") {
    val nested = new StructType()
      .add("s1", new IntegerType)
      .add("s2", new LongType)
    val base = new StructType()
      .add("nested", nested)
      .add("arr", new ArrayType(nested, true))
      .add("kvs", new MapType(nested, nested, true))
    val update = new StructType()
      .add("nested",
        new StructType()
          .add("t1", new IntegerType)
          .add("s2", new LongType))
      .add("arr", new ArrayType(
        new StructType()
          .add("s1", new IntegerType)
          .add("a2", new LongType),
        true))
      .add("kvs", new MapType(
        new StructType()
          .add("k1", new IntegerType)
          .add("s2", new LongType),
        new StructType()
          .add("s1", new IntegerType)
          .add("v2", new LongType),
        true))

    // Identity.
    var visitedFields = 0
    val res1 = transformColumns(base) {
      case (_, field, _) =>
        visitedFields += 1
        field
    }
    assert(visitedFields === 11)
    assert(base === res1)

    // Rename
    visitedFields = 0
    val res2 = transformColumns(base) { (path, field, _) =>
      visitedFields += 1
      val name = path :+ field.getName match {
        case Seq("nested", "s1") => "t1"
        case Seq("arr", "element", "s2") => "a2"
        case Seq("kvs", "key", "s1") => "k1"
        case Seq("kvs", "value", "s2") => "v2"
        case _ => field.getName
      }
      field.withNewName(name)
    }
    assert(visitedFields === 11)
    assert(update === res2)
  }
}
