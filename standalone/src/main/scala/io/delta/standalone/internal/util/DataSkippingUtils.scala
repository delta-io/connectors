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

import scala.collection.immutable

import com.fasterxml.jackson.databind.JsonNode

import io.delta.standalone.expressions.{And, Column, EqualTo, Expression, GreaterThanOrEqual, IsNotNull, LessThanOrEqual, Literal, Or}
import io.delta.standalone.types.{LongType, StructType}

/**
 * Results returned by [[DataSkippingUtils.constructDataFilters]].
 *
 * @param expr The transformed expression for column stats filter.
 * @param referencedStats Columns appears in [[expr]].
 */
private [internal] case class ColumnStatsPredicate(
    expr: Expression,
    referencedStats: immutable.Set[Column])

/**
 * Project Milestone 1: End-to-end implementation
 * - Implement basic column stats parsing and storage with only one-level schema with unit tests.
 * - Implement the basic `constructDataFilters` helper support `AND`, `EqualTo`, LiteralType, and
 *      Column with LongType, with unit tests.
 * - Implement the `verifyStatsForFilter` helper with unit tests.
 * - Implement the end to end stats skipping with integration tests.
 */
private[internal] object DataSkippingUtils {

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

  /**
   * Parse the stats in AddFile to two maps. The output contains two map distinguishing
   * the file-level stats and column-level stats.
   *
   * For file-level stats, like NUM_RECORDS, it only contains one value per file. The key
   * of file-level stats map is the stats type. And the value of map is stats value.
   *
   * For column-level stats, like MIN, MAX, or NULL_COUNT, they contains one value per column,
   * so that the key of column-level map is the COMPLETE column name with stats type, like `a.MAX`.
   * And the corresponding value of map is the stats value.
   *
   * Since the structured column don't have column stats, no worry about the duplicated column
   * names in column-level stats map. For example: if there is a column has the physical name
   * `a.MAX`, we don't need to worry about it is duplicated with the MAX stats of column `a`,
   * because the structured column `a` doesn't have column stats.
   *
   * If there is a column name not appears in the table schema, we won't store it.
   *
   * Example of `statsString`:
   * {
   *  "numRecords": 3,
   *  "minValues": {
   *      "a": 2,
   *      "b": 1
   *   }, ... // other stats
   * }
   *
   * Currently nested column is not supported, only [[LongType]] is the supported data type.
   *
   * @param tableSchema The table schema for this query.
   * @param statsString The json-formatted stats in raw string type in AddFile.
   * @return file-level stats map: the map stores file-level stats.
   *         column-level stats map: the map stores column-level stats, like MIN, MAX, NULL_COUNT.
   */
  def parseColumnStats(
      tableSchema: StructType,
      statsString: String): (immutable.Map[String, Long], immutable.Map[String, Long]) = {
    var fileLevelStats: Map[String, Long] = Map()
    var columnLevelStats: Map[String, Long] = Map()
    val columnNames = tableSchema.getFieldNames.toSeq
    // TODO: support nested columns

    JsonUtils.fromJson[Map[String, JsonNode]](statsString).foreach { stats =>
      if (!stats._2.isObject) {
        // This is an file-level stats, like ROW_RECORDS.
        if (stats._1 == NUM_RECORDS) {
          fileLevelStats += (stats._1 -> stats._2.asText.toLong)
        }
      } else {
        // This is an column-level stats, like MIN_VALUE and MAX_VALUE, iterator through the table
        // schema and fill the column-level stats map column-by-column if the column name appears
        // in json string.
        columnNames.foreach { columnName =>
          // Get stats value by column name
          val statsVal = stats._2.get(columnName)
          if (statsVal != null) {
            val statsType = stats._1
            val statsName = columnName + "." + statsType
            statsType match {
              case MIN | MAX =>
                // Check the stats type for MIN and MAX, as we only accepting the LongType for now.
                if (tableSchema.get(columnName).getDataType == new LongType) {
                  columnLevelStats += (statsName -> statsVal.asText.toLong)
                }
              case NULL_COUNT =>
                columnLevelStats += (statsName -> statsVal.asText.toLong)
            }
          }
        }
      }
    }
    (fileLevelStats, columnLevelStats)
  }

  /**
   * Build the data filter based on query predicate. Now two rules are applied:
   * - (col1 == Literal2) -> (col1.MIN <= Literal2 AND col1.MAX >= Literal2)
   * - constructDataFilters(expr1 AND expr2) ->
   *      constructDataFilters(expr1) AND constructDataFilters(expr2)
   *
   * @param expression The non-partition column query predicate.
   * @return columnStatsPredicate: Contains the column stats filter expression, and the set of stat
   *         column that appears in the filter expression, please see [[ColumnStatsPredicate]]
   *         definition.
   */
  def constructDataFilters(
      tableSchema: StructType,
      expression: Expression): Option[ColumnStatsPredicate] =
    expression match {
      case eq: EqualTo => (eq.getLeft, eq.getRight) match {
        case (e1: Column, e2: Literal) =>
          val columnPath = e1.name
          if (!(tableSchema.getFieldNames.contains(columnPath) &&
          tableSchema.get(columnPath).getDataType == new LongType)) {
            // Only accepting the LongType column for now.
            return None
          }
          val minColumn = new Column(columnPath + "." + MIN, new LongType)
          val maxColumn = new Column(columnPath + "." + MAX, new LongType)

          Some(ColumnStatsPredicate(
            new And(new LessThanOrEqual(minColumn, e2),
              new GreaterThanOrEqual(maxColumn, e2)),
            Set(minColumn, maxColumn)))
        case _ => None
      }
      case and: And =>
        val e1 = constructDataFilters(tableSchema, and.getLeft)
        val e2 = constructDataFilters(tableSchema, and.getRight)

        if (e1.isDefined && e2.isDefined) {
          Some(ColumnStatsPredicate(
            new And(e1.get.expr, e2.get.expr),
            e1.get.referencedStats ++ e2.get.referencedStats))
        } else None

      // TODO: support full types of Expression
      case _ => None
    }

  /**
   * Generate expression for validating the `referencedStats`. Following is the generation rules for
   * different stats types (take column `col1` as example):
   *
   * - For the MIN and MAX stats in one file, they can either be all null value or all non-null
   * value. So the validation expression is:
   * `(col1.MIN is not null) OR (col1.NULL_COUNT == NUM_RECORDS)`
   *
   * - For other stats type (like NUM_RECORDS or NULL_COUNT), the validation expression is:
   * `${STATS_COLUMN} is not null`
   *
   * @param referencedStats the set of stats columns, like `a.MAX`, that appears in the
   *                        `column stats filter` expression, which is generated by
   *                        [[constructDataFilters]]).
   * @return The validation expression
   */
  def verifyStatsForFilter(referencedStats: immutable.Set[Column]): Expression = {
    referencedStats.map { refStats =>
      val pathToColumn = SchemaUtils.parseAndValidateColumn(refStats.name)
      val statsType = pathToColumn.last
      statsType match {
        case MAX | MIN =>
          val nullCountName = (pathToColumn.dropRight(1) :+ NULL_COUNT).mkString(".")
          val nullCount = new Column(nullCountName, new LongType())
          val numRecords = new Column(NUM_RECORDS, new LongType())
          new Or(new IsNotNull(refStats), new EqualTo(nullCount, numRecords))

        // Skip column stats filter if some column is invalid
        case "" | null => Literal.False
        case _ => new IsNotNull(refStats)
      }
    }.reduceLeft(new And(_, _))
  }
}
