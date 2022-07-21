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

package io.delta.standalone.internal.data

import java.sql.{Date, Timestamp}
import java.util

import io.delta.standalone.data.{RowRecord => RowRecordJ}
import io.delta.standalone.types.{LongType, StructType}

import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.util.{DataSkippingUtils, SchemaUtils}

/**
 * Exposes the column stats in an [[AddFile]] as [[RowRecord]]. This is used to evaluate the
 * predicate in column stats based file pruning.
 *
 * Example: Assume we have a table schema like this: table1(col1: long, col2:long)),
 * with the supported stats types NUM_RECORDS, MAX, MIN, NULL_COUNT.
 *
 * [[fileStats]] will be: {"NUM_RECORDS": 1}
 * [[columnStats]] will be:
 * {
 *  "MIN.col1": 2,
 *  "MAX.col1": 2,
 *  "NULL_COUNT.col1": 0,
 *  "MIN.col2": 3,
 *  "MIN.col2": 3
 *  "NULL_COUNT.col2": 0,
 * }
 *
 * [[getLong]] is expected to return a non-null value. It is the responsibility of the called to
 * call [[isNullAt]] first before calling [[getLong]]. Similarly for the APIs fetching different
 * data types such as int, boolean etc.
 *
 * It is also guaranteed that the [[IsNull]] or [[IsNotNull]] won't exists in column stats
 * predicate, so [[isNullAt]] will only work for pre-checking when evaluating the column.
 *
 * @param statsSchema the schema of the stats column, the first level is stats type, and the
 *                    secondary level is data column name.
 * @param fileStats   file-specific stats, like NUM_RECORDS.
 * @param columnStats column-specific stats, like MIN, MAX, or NULL_COUNT.
 */
private[internal] class ColumnStatsRowRecord(
    statsSchema: StructType,
    fileStats: collection.Map[String, Long],
    columnStats: collection.Map[String, Long]) extends RowRecordJ {
  // TODO: support BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType,
  //  ShortType

  override def getSchema: StructType = statsSchema

  override def getLength: Int = statsSchema.length()

  /** Check whether there is a column in the stats schema */
  private def checkColumnNameAndType(columnName: String, statsType: String): Boolean = {
    if (!statsSchema.hasFieldName(statsType)) {
      // ensure that such column name exists in table schema
      return false
    }
    val statType = statsSchema.get(statsType).getDataType
    if (!statType.isInstanceOf[StructType]) {
      return false
    }
    // Ensure that the data type in table schema is supported.
    val colType = statType.asInstanceOf[StructType]
    if (!colType.hasFieldName(columnName)) {
      return false
    }
    // For now we only accept LongType.
    colType.get(columnName).getDataType.equals(new LongType)
  }

  /** Return a None if the stats is not found, return Some(Long) if it's found. */
  private def getLongOrNone(fieldName: String): Option[Long] = {
    // Parse column name with stats type: a.MAX => Seq(a, MAX)
    val pathToColumn = SchemaUtils.parseAndValidateColumn(fieldName)

    // In stats column the last element is stats type
    val statsType = pathToColumn.head

    statsType match {
      // For the file-level column, like NUM_RECORDS, we get value from fileStats map by
      // stats type.
      case DataSkippingUtils.NUM_RECORDS if pathToColumn.length == 1 =>
        // File-level column name should only have the stats type as name
        fileStats.get(fieldName)

      // For the column-level stats type, like MIN or MAX or NULL_COUNT, we get value from
      // columnStats map by the COMPLETE column name with stats type, like `a.MAX`.
      case DataSkippingUtils.NULL_COUNT if pathToColumn.length == 2 =>
        // Currently we only support non-nested columns, so the `pathToColumn` should only contain
        // 2 parts: the column name and the stats type.
        columnStats.get(fieldName)

      case DataSkippingUtils.MIN | DataSkippingUtils.MAX if pathToColumn.length == 2 =>
        val columnName = pathToColumn.last
        if (checkColumnNameAndType(columnName, statsType)) {
          columnStats.get(fieldName)
        } else None

      case _ => None
    }
  }

  /**
   * If a column not exists in both stats map, then it is missing, will return true.
   *
   * Since [[ColumnStatsRowRecord.isNullAt]] is used in the evaluation of IsNull and IsNotNull
   * expressions, it will return TRUE for IsNull(missingStats), which could be an incorrect
   * result. Here we avoid this problem by not using IsNull expression as a part of any column
   * stats filter.
   */
  override def isNullAt(fieldName: String): Boolean = {
    getLongOrNone(fieldName).isEmpty
  }

  override def getInt(fieldName: String): Int = {
    throw new UnsupportedOperationException("integer is not a supported column stats type.")
  }

  /** getLongOrNone must return the field name here as we have pre-checked by [[isNullAt]] */
  override def getLong(fieldName: String): Long = getLongOrNone(fieldName).getOrElse {
    throw DeltaErrors.nullValueFoundForPrimitiveTypes(fieldName)
  }

  override def getByte(fieldName: String): Byte =
    throw new UnsupportedOperationException("byte is not a supported column stats type.")

  override def getShort(fieldName: String): Short =
    throw new UnsupportedOperationException("short is not a supported column stats type.")

  override def getBoolean(fieldName: String): Boolean =
    throw new UnsupportedOperationException("boolean is not a supported column stats type.")

  override def getFloat(fieldName: String): Float =
    throw new UnsupportedOperationException("float is not a supported column stats type.")

  override def getDouble(fieldName: String): Double =
    throw new UnsupportedOperationException("double is not a supported column stats type.")

  override def getString(fieldName: String): String =
    throw new UnsupportedOperationException("string is not a supported column stats type.")

  override def getBinary(fieldName: String): Array[Byte] =
    throw new UnsupportedOperationException("binary is not a supported column stats type.")

  override def getBigDecimal(fieldName: String): java.math.BigDecimal =
    throw new UnsupportedOperationException("decimal is not a supported column stats type.")

  override def getTimestamp(fieldName: String): Timestamp =
    throw new UnsupportedOperationException("timestamp is not a supported column stats type.")

  override def getDate(fieldName: String): Date =
    throw new UnsupportedOperationException("date is not a supported column stats type.")

  override def getRecord(fieldName: String): RowRecordJ =
    throw new UnsupportedOperationException("Struct is not a supported column stats type.")

  override def getList[T](fieldName: String): util.List[T] =
    throw new UnsupportedOperationException("List is not a supported column stats type.")

  override def getMap[K, V](fieldName: String): util.Map[K, V] =
    throw new UnsupportedOperationException("Map is not a supported column stats type.")
}
