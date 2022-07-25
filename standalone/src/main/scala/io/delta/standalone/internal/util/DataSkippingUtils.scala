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

import com.fasterxml.jackson.databind.JsonNode

import io.delta.standalone.expressions.{And, Column, EqualTo, Expression, GreaterThanOrEqual, LessThanOrEqual, Literal}
import io.delta.standalone.types.{DataType, LongType, StructField, StructType}

private[internal] object DataSkippingUtils {

  // TODO: add extensible storage of column stats name and their data type.
  //  (data type can be fixed, like type of `NUM_RECORDS` is always LongType)
  /* The total number of records in the file. */
  final val NUM_RECORDS = "numRecords"
  /* The smallest (possibly truncated) value for a column. */
  final val MIN = "minValues"
  /* The largest (possibly truncated) value for a column. */
  final val MAX = "maxValues"
  /* The number of null values present for a column. */
  final val NULL_COUNT = "nullCount"

  /* The file-specific stats column contains only the column type. e.g.: NUM_RECORD */
  final val fileStatsPathLength = 1
  /* The column-specific stats column contains column type and column name. e.g.: MIN.col1 */
  final val columnStatsPathLength = 2

  /**
   * Build stats schema based on the schema of data columns, the first layer
   * of stats schema is stats type. If it is a column-specific stats, it nested a second layer,
   * which contains the column name in table schema. Or if it is a column-specific stats, contains
   * a non-nested data type.
   *
   * The layout of stats schema is totally the same as the full stats string in JSON:
   * (for the table contains column `a` and `b`)
   * {
   *  "[[NUM_RECORDS]]": 3,
   *  "[[MIN]]": {
   *      "a": 2,
   *      "b": 1
   *   }, ... // other stats
   * }
   *
   * @param dataSchema The schema of data columns in table.
   * @return The schema storing the layout of stats columns.
   */
  def buildStatsSchema(dataSchema: StructType): StructType = {
    // TODO: add partial stats support as config `DATA_SKIPPING_NUM_INDEXED_COLS`
    val nonNestedColumns = dataSchema
      .getFields
      .filterNot(_.getDataType.isInstanceOf[StructType])
    nonNestedColumns.length match {
      case 0 => new StructType()
      case _ =>
        val nullCountColumns = nonNestedColumns.map { field =>
          new StructField(field.getName, new LongType)
        }
        new StructType(Array(
          // MIN and MAX are used the corresponding data column's type.
          new StructField(MIN, new StructType(nonNestedColumns)),
          new StructField(MAX, new StructType(nonNestedColumns)),

          // nullCount is using the LongType for all columns
          new StructField(NULL_COUNT, new StructType(nullCountColumns)),

          // numRecords is a file-specific Long value
          new StructField(NUM_RECORDS, new LongType)))
    }
  }

  /**
   * Parse the stats in data metadata files to two maps. The output contains two maps
   * distinguishing the file-specific stats and column-specific stats.
   *
   * For file-specific stats, like NUM_RECORDS, it only contains one value per file. The key
   * of file-specific stats map is the stats type. And the value of map is stats value.
   *
   * For column-specific stats, like MIN, MAX, or NULL_COUNT, they contains one value per column at
   * most, so the key of column-specific map is the stats type with COMPLETE column name, like
   * `MAX.a`. And the corresponding value of map is the stats value.
   *
   * If there is a column name not appears in the table schema, we won't store it.
   *
   * Example of `statsString` (for the table contains column `a` and `b`):
   * {
   *   "[[NUM_RECORDS]]": 3,
   *   "[[MIN]]": {
   *      "a": 2,
   *      "b": 1
   *   }, ... // other stats
   * }
   *
   * The corresponding output will be:
   * fileStats = Map("[[NUM_RECORDS]]" -> 3)
   * columnStats = Map("[[MIN]].a" -> 2, "[[MIN]].b" -> 1)
   *
   * Currently nested column is not supported, only [[LongType]] is the supported data type, if
   * encountered a wrong data type with a known stats type, the method will raise error and should
   * be handled by caller.
   *
   * @param dataSchema  The schema of data columns in table.
   * @param statsString The JSON-formatted stats in raw string type in table metadata files.
   * @return file-specific stats map:   The map stores file-specific stats, like [[NUM_RECORDS]].
   *         column-specific stats map: The map stores column-specific stats, like [[MIN]],
   *         [[MAX]], [[NULL_COUNT]].
   */
  def parseColumnStats(
      dataSchema: StructType,
      statsString: String): (Map[String, Long], Map[String, Long]) = {
    var fileStats = Map[String, Long]()
    var columnStats = Map[String, Long]()

    val dataColumns = dataSchema.getFields
    JsonUtils.fromJson[Map[String, JsonNode]](statsString).foreach { stats =>
      val statsType = stats._1
      val statsObj = stats._2
      if (!statsObj.isObject) {
        // This is an file-specific stats, like ROW_RECORDS.
        if (statsType == NUM_RECORDS) {
          fileStats += (statsType -> statsObj.asText.toLong)
        }
      } else {
        // This is an column-specific stats, like MIN_VALUE and MAX_VALUE, iterator through the
        // schema of data columns and fill the column-specific stats map column-by-column if the
        // column name appears in JSON string.
        dataColumns.filter(col => statsObj.has(col.getName)).foreach { dataColumn =>
          // Get stats value by column in data schema.
          val columnName = dataColumn.getName
          val statsVal = statsObj.get(columnName)
          if (statsVal != null) {
            val statsName = statsType + "." + dataColumn.getName
            statsType match {
              case MIN | MAX =>
                // Check the stats type for MIN and MAX, as we only accepting the LongType for now.
                if (dataColumn.getDataType.isInstanceOf[LongType]) {
                  columnStats += (statsName -> statsVal.asText.toLong)
                }
              case NULL_COUNT =>
                columnStats += (statsName -> statsVal.asText.toLong)
              case _ =>
            }
          }
        }
      }
    }
    (fileStats, columnStats)
  }

  /** Helper function of building [[Column]] referencing stats value */
  def statsColumnBuilder(statsType: String, columnName: String, dataType: DataType): Column =
    new Column(statsType + "." + columnName, dataType)

  /**
   * Build the column stats filter based on query predicate and the schema of data columns.
   *
   * Assume `col1` and `col2` are columns in query predicate, `l1` and `l2` are two
   * literal values in query predicate. Let `f` be this method `constructDataFilters`.
   * Now two rules are applied:
   * - (col1 == l1) -> (MIN.col1 <= l1 AND MAX.col1 >= l1)
   * - f(expr1 AND expr2) -> f(expr1) AND f(expr2)
   *
   * @param dataSchema      The schema of data columns in table.
   * @param dataConjunction The non-partition column query predicate.
   * @return columnStatsPredicate: Return the column stats filter predicate. Or it will return None
   *         if met unsupported data type, or unsupported expression type issues.
   */
  def constructDataFilters(
      dataSchema: StructType,
      dataConjunction: Option[Expression]): Option[Expression] =
    dataConjunction match {
      case Some(eq: EqualTo) => (eq.getLeft, eq.getRight) match {
        case (e1: Column, e2: Literal) =>
          val columnPath = e1.name
          if (!(dataSchema.contains(columnPath) &&
            dataSchema.get(columnPath).getDataType.isInstanceOf[LongType])) {
              // Only accepting the LongType column for now.
              return None
          }
          val minColumn = statsColumnBuilder(MIN, columnPath, new LongType)
          val maxColumn = statsColumnBuilder(MAX, columnPath, new LongType)

          Some(new And(
              new LessThanOrEqual(minColumn, e2),
              new GreaterThanOrEqual(maxColumn, e2)))
        case _ => None
      }
      case Some(and: And) =>
        val e1 = constructDataFilters(dataSchema, Some(and.getLeft))
        val e2 = constructDataFilters(dataSchema, Some(and.getRight))

        (e1, e2) match {
          case (Some(e1), Some(e2)) => Some(new And(e1, e2))
          case _ => None
        }

      // TODO: support full types of Expression
      case _ => None
    }
}
