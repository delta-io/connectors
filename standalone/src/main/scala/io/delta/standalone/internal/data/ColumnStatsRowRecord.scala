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
import io.delta.standalone.internal.util.SchemaUtils


private[internal] class ColumnStatsRowRecord(
    tableSchema: StructType,
    fileStatsValues: collection.Map[String, Long],
    columnStatsValues: collection.Map[String, Long]) extends RowRecordJ {

  override def getSchema: StructType = tableSchema

  override def getLength: Int = tableSchema.length()

  // if a column not exists either in `fileStatsValues` or `columnStatsValues`, then it is missing.
  override def isNullAt(fieldName: String): Boolean = {
    !fileStatsValues.exists(_._1 == fieldName) &&
    !columnStatsValues.exists(_._1 == fieldName)
  }

  // TODO: add extensible storage of column stats name and their data type.
  //  (if data type is fixed, like type of `NUM_RECORDS` is always LongType)
  /* The total number of records in the file. */
  final val NUM_RECORDS = "numRecords"
  /* The smallest (possibly truncated) value for a column. */
  final val MIN = "minValues"
  /* The largest (possibly truncated) value for a column. */
  final val MAX = "maxValues"
  /* The number of null values present for a column. */
  final val NULL_COUNT = "nullCount"

  override def getInt(fieldName: String): Int = {
    throw new UnsupportedOperationException("Int is not a supported column stats type.")
  }

  // Assume we have the following table schema,
  // - a
  // - b
  // and with the following nested column stats:
  // - a
  // - - MAX
  // - b
  // - - MAX
  override def getLong(fieldName: String): Long = {

    // Parse nested column name: a.MAX => Seq(a, MAX)
    val pathToColumn = SchemaUtils.parseAndValidateColumn(fieldName)

    // In stats column the last element is stats type
    val statsType = pathToColumn.last

    val res = statsType match {
      // For the file-level column, like NUM_RECORDS, we get value from fileStatsValues map by
      // stats type.
      case NUM_RECORDS if pathToColumn.length == 1 =>
        // File-level column name should only have the stats type as name
        fileStatsValues.get(fieldName)

      // For the column-level stats type, like MIN or MAX, we get value from columnStatsValues map
      // by the COMPLETE column name with stats type, like `a.MAX`.
      case NULL_COUNT if pathToColumn.length == 2 =>
        // Currently we only support non-nested columns, so the `pathToColumn` should only contain
        // 2 parts: the column name and the stats type.
        columnStatsValues.get(fieldName)

      case MIN | MAX if pathToColumn.length == 2 =>
        val columnName = pathToColumn.head
        if (tableSchema.getFieldNames.contains(columnName)) {
          // ensure that such column name exists in table schema
          val dataTypeInSchema = tableSchema.get(columnName).getDataType
          if (dataTypeInSchema.equals(new LongType)) {
            // ensure that the data type in table schema is the same as it in stats storage.
            // For now we only accept LongType.
            columnStatsValues.get(fieldName)
          } else None
        } else None

      case _ => None
    }

    // TODO: Not throwing error for missing column stats
    // Option 1:
    res.getOrElse(throw DeltaErrors.nullValueFoundForNonNullSchemaField(fieldName, tableSchema))
  }

  override def getByte(fieldName: String): Byte =
    throw new UnsupportedOperationException("Byte is not a supported column stats type.")

  override def getShort(fieldName: String): Short =
    throw new UnsupportedOperationException("Short is not a supported column stats type.")

  override def getBoolean(fieldName: String): Boolean =
    throw new UnsupportedOperationException("Boolean is not a supported column stats type.")

  override def getFloat(fieldName: String): Float =
    throw new UnsupportedOperationException("Float is not a supported column stats type.")

  override def getDouble(fieldName: String): Double =
    throw new UnsupportedOperationException("Double is not a supported column stats type.")

  override def getString(fieldName: String): String =
    throw new UnsupportedOperationException("String is not a supported column stats type.")

  override def getBinary(fieldName: String): Array[Byte] =
    throw new UnsupportedOperationException("Binary is not a supported column stats type.")

  override def getBigDecimal(fieldName: String): java.math.BigDecimal =
    throw new UnsupportedOperationException("BigDecimal is not a supported column stats type.")

  override def getTimestamp(fieldName: String): Timestamp =
    throw new UnsupportedOperationException("TimeStamp is not a supported column stats type.")

  override def getDate(fieldName: String): Date =
    throw new UnsupportedOperationException("Date is not a supported column stats type.")

  override def getRecord(fieldName: String): RowRecordJ =
    throw new UnsupportedOperationException("Record is not a supported column stats type.")

  override def getList[T](fieldName: String): util.List[T] =
    throw new UnsupportedOperationException("List is not a supported column stats type.")

  override def getMap[K, V](fieldName: String): util.Map[K, V] =
    throw new UnsupportedOperationException("Map is not a supported column stats type.")
}
