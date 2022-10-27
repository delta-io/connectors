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

package io.delta.standalone.internal.util

import io.delta.standalone.exceptions.DeltaStandaloneException
import io.delta.standalone.types.{ArrayType, DataType, MapType, StructField, StructType}

/**
 * Utils to merge table schema with data schema.
 */
private[internal] object SchemaMergingUtils {

  type Resolver = (String, String) => Boolean

  val DELTA_COL_RESOLVER: (String, String) => Boolean =
    (a: String, b: String) => a.equalsIgnoreCase(b)

  /**
   * Returns all column names in this schema as a flat list. For example, a schema like:
   *   | - a
   *   | | - 1
   *   | | - 2
   *   | - b
   *   | - c
   *   | | - nest
   *   |   | - 3
   *   will get flattened to: "a", "a.1", "a.2", "b", "c", "c.nest", "c.nest.3"
   */
  def explodeNestedFieldNames(schema: StructType): Seq[String] = {
    def explode(schema: StructType): Seq[Seq[String]] = {
      def recurseIntoComplexTypes(complexType: DataType): Seq[Seq[String]] = {
        complexType match {
          case s: StructType => explode(s)
          case a: ArrayType => recurseIntoComplexTypes(a.getElementType).map(Seq("element") ++ _)
          case m: MapType =>
            recurseIntoComplexTypes(m.getKeyType).map(Seq("key") ++ _) ++
              recurseIntoComplexTypes(m.getValueType).map(Seq("value") ++ _)
          case _ => Nil
        }
      }

      schema.getFields.flatMap { f =>
        val name = f.getName
        f.getDataType match {
          case s: StructType =>
            Seq(Seq(name)) ++ explode(s).map(nested => Seq(name) ++ nested)
          case a: ArrayType =>
            Seq(Seq(name)) ++ recurseIntoComplexTypes(a).map(nested => Seq(name) ++ nested)
          case m: MapType =>
            Seq(Seq(name)) ++ recurseIntoComplexTypes(m).map(nested => Seq(name) ++ nested)
          case _ => Seq(name) :: Nil
        }
      }
    }

    explode(schema).map { nameParts =>
      nameParts.map(n => if (n.contains(".")) s"`$n`" else n).mkString(".")
    }
  }

  /**
   * Checks if input column names have duplicate identifiers. This throws an exception if
   * the duplication exists.
   *
   * @param schema the schema to check for duplicates
   * @param colType column type name, used in an exception message
   */
  def checkColumnNameDuplication(schema: StructType, colType: String): Unit = {
    val columnNames = explodeNestedFieldNames(schema)
    // scalastyle:off caselocale
    val names = columnNames.map(_.toLowerCase)
    // scalastyle:on caselocale
    if (names.distinct.length != names.length) {
      val duplicateColumns = names.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => s"$x"
      }

      throw new DeltaStandaloneException(
        s"Found duplicate column(s) $colType: ${duplicateColumns.mkString(", ")}")
    }
  }

  /**
   * (Copy of similar named method in Delta on Spark project (https://github.com/delta-io/),
   * file: core/src/main/scala/org/apache/spark/sql/delta/schema/SchemaMergingUtils.scala.
   * Only thing changed is type classes).
   *
   * Transform (nested) columns in a schema.
   *
   * @param schema to transform.
   * @param tf function to apply.
   * @return the transformed schema.
   */
  def transformColumns(
      schema: StructType)(
      tf: (Seq[String], StructField, Resolver) => StructField): StructType = {
    def transform[E <: DataType](path: Seq[String], dt: E): E = {
      val newDt = dt match {
        case s: StructType =>
          new StructType(
            s.getFields().map(field => {
              val newField = tf(path, field, DELTA_COL_RESOLVER)
              val newDataType = transform(path :+ field.getName, newField.getDataType())

              // maintain the old name as we recurse into the subfields
              newField.withNewDataType(newDataType)
            }).toArray)
        case a: ArrayType =>
          new ArrayType(
            transform(path :+ "element", a.getElementType()),
            a.containsNull())
        case m: MapType =>
          new MapType(
            transform(path :+ "key", m.getKeyType()),
            transform(path :+ "value", m.getValueType()),
            m.valueContainsNull()
          )
        case other => other
      }
      newDt.asInstanceOf[E]
    }
    transform(Seq.empty, schema)
  }
}
