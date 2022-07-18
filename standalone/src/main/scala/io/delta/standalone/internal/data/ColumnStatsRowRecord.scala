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
 * [[fileStats]] will be like: {"NUM_RECORDS": 1}
 * [[columnStats]] will be like:
 * {
 *  "col1.MIN": 2,
 *  "col1.MAX": 2,
 *  "col1.NULL_COUNT": 0,
 *  "col2.MIN": 3,
 *  "col2.MIN": 3
 *  "col2.NULL_COUNT": 0,
 * }
 *
 * When evaluating the expression EqualTo(Column("NUM_RECORDS", Long), Column("col2.MIN", Long)),
 * [[getLong]] would be called twice to get the data from two maps.
 *
 * In [[getLong]], it will firstly distinguish the stats type, like MIN is a column-specific class
 * or NUM_RECORDS is a file-specific class, then find the stats value in the corresponding map, then
 * return the value for Column evaluation.
 *
 * When calling [[getLong]] for Column("NUM_RECORDS", Long), since NUM_RECORDS is a file-specific
 * stats, so returning fileValues["NUM_RECORDS"].
 *
 * When calling [[getLong]] for Column("col2.MIN", Long), since MIN is a column-specific class, so
 * returning columnValues["col2.MIN"].
 *
 * [[getLong]] is expected to return a non-null value. It is the responsibility of the called to
 * call [[isNullAt]] first before calling [[getLong]]. Similarly for the APIs fetching different
 * data types such as int, boolean etc.
 *
 * It is also guaranteed that the [[IsNull]] or [[IsNotNull]] won't appears in column stats
 * predicate, so [[isNullAt]] will only work for pre-checking when evaluating the column.
 *
 * @param tableSchema the schema of scanning table, loaded from table metadata.
 * @param fileStats   file-specific stats, like NUM_RECORDS.
 * @param columnStats column-specific stats, like MIN, MAX, or NULL_COUNT.
 */
private[internal] class ColumnStatsRowRecord(
    tableSchema: StructType,
    fileStats: collection.Map[String, Long],
    columnStats: collection.Map[String, Long]) extends RowRecordJ {

  override def getSchema: StructType = tableSchema

  override def getLength: Int = tableSchema.length()

  private def checkColumnNameAndType(columnName: String): Boolean = {
    if (!tableSchema.getFieldNames.contains(columnName)) {
      // ensure that such column name exists in table schema
      return false;
    }
    val dataTypeInSchema = tableSchema.get(columnName).getDataType
    // Ensure that the data type in table schema is supported.
    // For now we only accept LongType.
    dataTypeInSchema.equals(new LongType)
  }

  /** Return a None if the stats is not found, return Some(Long) if it's found. */
  private def getLongOrNone(fieldName: String): Option[Long] = {
    // Parse column name with stats type: a.MAX => Seq(a, MAX)
    val pathToColumn = SchemaUtils.parseAndValidateColumn(fieldName)

    // In stats column the last element is stats type
    val statsType = pathToColumn.last

    statsType match {
      // For the file-level column, like NUM_RECORDS, we get value from fileValues map by
      // stats type.
      case DataSkippingUtils.NUM_RECORDS if pathToColumn.length == 1 =>
        // File-level column name should only have the stats type as name
        fileStats.get(fieldName)

      // For the column-level stats type, like MIN or MAX or NULL_COUNT, we get value from
      // columnValues map by the COMPLETE column name with stats type, like `a.MAX`.
      case DataSkippingUtils.NULL_COUNT if pathToColumn.length == 2 =>
        // Currently we only support non-nested columns, so the `pathToColumn` should only contain
        // 2 parts: the column name and the stats type.
        columnStats.get(fieldName)

      case DataSkippingUtils.MIN | DataSkippingUtils.MAX if pathToColumn.length == 2 =>
        val columnName = pathToColumn.head
        if (checkColumnNameAndType(columnName)) {
          columnStats.get(fieldName)
        } else None

      case _ => None
    }
  }

  /** if a column not exists either in value map, then it is missing, will return true. */
  override def isNullAt(fieldName: String): Boolean = {
    getLongOrNone(fieldName).isEmpty
  }

  override def getInt(fieldName: String): Int = {
    throw new UnsupportedOperationException("Int is not a supported column stats type.")
  }

  /** getLongOrNone must return the field name here as we have pre-checked by [[isNullAt]] */
  override def getLong(fieldName: String): Long = getLongOrNone(fieldName).getOrElse {
    throw DeltaErrors.nullValueFoundForPrimitiveTypes(fieldName)
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
