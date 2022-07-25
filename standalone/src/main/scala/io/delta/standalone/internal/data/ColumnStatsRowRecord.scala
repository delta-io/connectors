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
import io.delta.standalone.internal.util.DataSkippingUtils.{columnStatsPathLength, fileStatsPathLength, MAX, MIN, NULL_COUNT, NUM_RECORDS}
import io.delta.standalone.internal.util.SchemaUtils

/**
 * Exposes the column stats in a Delta table file as [[RowRecord]]. This is used to evaluate the
 * predicate in column stats based file pruning.
 *
 * Example: Assume we have a table schema like this: table1(col1: long, col2:long)),
 * with the supported stats types NUM_RECORDS, MAX, MIN, NULL_COUNT.
 *
 * [[statsSchema]] will be:
 * {
 *    "MIN": {
 *      "col1": LongType,
 *      "col2": LongType,
 *    },
 *    "MAX": {
 *      "col1": LongType,
 *      "col2": LongType,
 *    },
 *    "NULL_COUNT": {
 *      "col1": LongType,
 *      "col2": LongType,
 *    },
 *    "NUM_RECORD": 1
 * }
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
 * @param statsSchema The schema of the stats column, the first level is stats type, and the
 *                    secondary level is data column name.
 * @param fileStats   File-specific stats, like NUM_RECORDS.
 * @param columnStats Column-specific stats, like MIN, MAX, or NULL_COUNT.
 */
private[internal] class ColumnStatsRowRecord(
    statsSchema: StructType,
    fileStats: Map[String, Long],
    columnStats: Map[String, Long]) extends RowRecordJ {
  // `getLong` is expected to return a non-null value. It is the responsibility of the caller to
  // call `isNullAt` first before calling `getLong`. Similarly for the APIs fetching different
  // data types such as int, boolean etc.
  //
  // It is also guaranteed that the `IsNull` or `IsNotNull` won't exists in column stats
  // predicate, so `isNullAt` will only work for pre-checking when evaluating the column.

  // TODO: support BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType,
  //  ShortType

  override def getSchema: StructType = statsSchema

  override def getLength: Int = statsSchema.length()

  /**
   * Check whether there is a column with supported data type in the stats schema.
   * This method is used for checking column-specific stats.
   *
   * Return TRUE if $statsType.$columnName exists in stats schema.
   * Return FALSE if it not exists or the data type is not supported.
   */
  private def isValidColumnNameAndType(columnName: String, statsType: String): Boolean = {
    if (!statsSchema.contains(statsType)) {
      // Ensure that such stats type exists in table schema.
      return false
    }
    val statType = statsSchema.get(statsType).getDataType
    if (!statType.isInstanceOf[StructType]) {
      // Ensure that the column-specific stats type contains multiple columns in stats schema.
      return false
    }
    val statsStruct = statType.asInstanceOf[StructType]
    if (!statsStruct.contains(columnName)) {
      // Ensure that the data type in table schema is supported.
      return false
    }
    // For now we only accept LongType.
    statsStruct.get(columnName).getDataType.equals(new LongType)
  }

  /** Return None if the stats in given filedName is not found, return Some(Long) if found. */
  private def getLongOrNone(fieldName: String): Option[Long] = {
    // Parse column name with stats type: MAX.a => Seq(MAX, a)
    // The first element in `pathToColumn` is stats type, and the second element should be
    // the data column name.
    val pathToColumn = SchemaUtils.parseAndValidateColumn(fieldName).getOrElse {
      return None
    }

    // In stats column the last element is stats type
    val statsType = pathToColumn.head

    statsType match {
      // For the file-level column, like NUM_RECORDS, we get value from fileStats map by
      // stats type.
      case NUM_RECORDS if pathToColumn.length == fileStatsPathLength =>
        // File-level column name should only have the stats type as name
        fileStats.get(fieldName)

      // For the column-level stats type, like MIN or MAX or NULL_COUNT, we get value from
      // columnStats map by the COMPLETE column name with stats type, like `a.MAX`.
      case NULL_COUNT if pathToColumn.length == columnStatsPathLength =>
        // Currently we only support non-nested columns, so the `pathToColumn` should only contain
        // 2 parts: the column name and the stats type.
        columnStats.get(fieldName)

      case MIN | MAX if pathToColumn.length == columnStatsPathLength =>
          val columnName = pathToColumn.last
          if (isValidColumnNameAndType(columnName, statsType)) {
            columnStats.get(fieldName)
          } else None

      case _ => None
    }
  }

  /**
   * Checks if the statistics do not exist for the given fieldName. Returns true if missing, else
   * returns false.
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
