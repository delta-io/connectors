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

package io.delta.standalone.internal

import org.apache.parquet.schema._
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition._

import io.delta.standalone.types._


// scalastyle:off funsuite
import org.scalatest.FunSuite

class ParquetSchemaConverterSuite extends FunSuite {

  private def createTestSchema(dataType: DataType, nullable: Boolean) = {
    new StructType(Array(new StructField(dataType.getTypeName, dataType, nullable)))
  }

  test("LegacyParquetFormat = False") {
    
    // atomic types
    val atomicTypes = Array(
      (new BooleanType(), BOOLEAN, None),
      (new ByteType(), INT32, Some(INT_8)),
      (new ShortType(), INT32, Some(INT_16)),
      (new IntegerType(), INT32, None),
      (new LongType(), INT64, None),
      (new FloatType(), FLOAT, None),
      (new DoubleType(), DOUBLE, None),
      (new StringType(), BINARY, Some(UTF8)),
      (new DateType(), INT32, Some(DATE)),
      (new BinaryType(), BINARY, None),
      (new TimestampType(), INT96, None)
    )

    atomicTypes.foreach{
      case (dataType, primitiveType, as) =>
        assert(
          new util.SparkToParquetSchemaConverter().convert(createTestSchema(dataType, true))
          == new MessageType(
            "spark_schema",
            as match {
              case Some(originalType) =>
                Types.optional(primitiveType).as(originalType).named(dataType.getTypeName)
              case None =>
                Types.optional(primitiveType).named(dataType.getTypeName)
            }))
        assert(
          new util.SparkToParquetSchemaConverter().convert(createTestSchema(dataType, false))
            == new MessageType(
            "spark_schema",
            as match {
              case Some(originalType) =>
                Types.required(primitiveType).as(originalType).named(dataType.getTypeName)
              case None =>
                Types.required(primitiveType).named(dataType.getTypeName)
            }))
    }

    // timestamp for outputTimestampType != INT96

    // decimal type

    // array type

    // map type

    // struct type
  }

  test("LegacyParquetFormat = True") {
    // decimal type
    // array
    // map type
  }

  test("schema with multiple fields") {

  }

}
