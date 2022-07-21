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
import io.delta.standalone.types.{LongType, StructField, StructType}

/**
 * The referenced stats column in column stats filter, used in
 * [[ColumnStatsPredicate]].
 *
 * @param pathToColumn the column name parsed by dot
 * @param column       the stats column
 */
private [internal] case class ReferencedStats(
    pathToColumn: Seq[String],
    column: Column)

/**
 * Results returned by [[DataSkippingUtils.constructDataFilters]]. Contains the column stats
 * predicate, and the set of stats columns appears in the column stats predicate.
 *
 * @param expr The transformed expression for column stats filter.
 * @param referencedStats Columns appears in [[expr]].
 */
private [internal] case class ColumnStatsPredicate(
    expr: Expression,
    referencedStats: immutable.Set[ReferencedStats])

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

  /**
   * Build [[statsSchema]] based on the table schema, the first layer of stats schema is stats type.
   * If is a column-specific stats, it nested a second layer, which contains the column name in data
   * schema. Or it is a column-specific stats, contains a non-nested data type.
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
   * We don't need this to prevent missing stats as they are prevented by
   * [[verifyStatsForFilter]].
   *
   * @param tableSchema the table schema in Snapshot
   * @return [[statsSchema]]
   */
  def buildStatsSchema(tableSchema: StructType): StructType = {
    // TODO: add partial stats support as config `DATA_SKIPPING_NUM_INDEXED_COLS`
    val nonNestedColumns = tableSchema.getFields.filterNot(_.getDataType.isInstanceOf[StructType])
    nonNestedColumns.length match {
      case 0 => new StructType()
      case _ =>
        val allLongColumns = nonNestedColumns.map(field =>
          new StructField(field.getName, new LongType))
        new StructType(Array(
          // MIN and MAX are used the corresponding data column's type.
          new StructField(MIN, new StructType(nonNestedColumns)),
          new StructField(MAX, new StructType(nonNestedColumns)),

          // nullCount is using the LongType for all columns
          new StructField(NULL_COUNT, new StructType(allLongColumns)),

          // numRecords is a file-specific Long value
          new StructField(NUM_RECORDS, new LongType)))
    }
  }

  /**
   * Parse the stats in AddFile to two maps. The output contains two map distinguishing
   * the file-specific stats and column-specific stats.
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
   *  "[[NUM_RECORDS]]": 3,
   *  "[[MIN]]": {
   *      "a": 2,
   *      "b": 1
   *   }, ... // other stats
   * }
   *
   * Currently nested column is not supported, only [[LongType]] is the supported data type.
   *
   * @param tableSchema The table schema describes data column (not stats column) for this query.
   * @param statsString The json-formatted stats in raw string type in AddFile.
   * @return file-specific stats map: the map stores file-specific stats, like [[NUM_RECORDS]]
   *         column-specific stats map: the map stores column-specific stats, like [[MIN]],
   *         [[MAX]], [[NULL_COUNT]].
   */
  def parseColumnStats(
      tableSchema: StructType,
      statsString: String): (immutable.Map[String, Long], immutable.Map[String, Long]) =
  {
    var fileStats = Map[String, Long]()
    var columnStats = Map[String, Long]()

    val dataColumns = tableSchema.getFields
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
        // table schema and fill the column-specific stats map column-by-column if the column name
        // appears in json string.
        dataColumns.filter(col => statsObj.has(col.getName)).foreach { dataColumn =>
          // Get stats value by column name, if the column is missing in this stat's struct, the
          val columnName = dataColumn.getName
          val statsVal = statsObj.get(columnName)
          if (statsVal != null) {
            val statsName = statsType + "." + dataColumn.getName
            statsType match {
              case MIN | MAX =>
                // Check the stats type for MIN and MAX, as we only accepting the LongType for now.
                val dataType = dataColumn.getDataType
                if (dataType == new LongType) {
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

  /**
   * Build the column stats filter based on query predicate. This expression builds only once per
   * query. Now two rules are applied:
   *
   * - (col1 == Literal2) -> (MIN.col1 <= Literal2 AND MAX.col1 >= Literal2)
   * - constructDataFilters(expr1 AND expr2) ->
   *      constructDataFilters(expr1) AND constructDataFilters(expr2)
   *
   * @param tableSchema The schema describes the structure of stats columns
   * @param expression  The non-partition column query predicate.
   * @return columnStatsPredicate: Contains the column stats filter expression, and the set of stat
   *         columns that are referenced in the filter expression, please see
   *         [[ColumnStatsPredicate]].
   */
  def constructDataFilters(
      tableSchema: StructType,
      expression: Expression): Option[ColumnStatsPredicate] =
    expression match {
      case eq: EqualTo => (eq.getLeft, eq.getRight) match {
        case (e1: Column, e2: Literal) =>
          val columnPath = e1.name
          if (!(tableSchema.hasFieldName(columnPath) &&
          tableSchema.get(columnPath).getDataType == new LongType)) {
            // Only accepting the LongType column for now.
            return None
          }
          val minColumn = ReferencedStats(Seq(MIN, columnPath),
            new Column(MIN + "." + columnPath, new LongType))
          val maxColumn = ReferencedStats(Seq(MAX, columnPath),
            new Column(MAX + "." + columnPath, new LongType))

          Some(ColumnStatsPredicate(
            new And(new LessThanOrEqual(minColumn.column, e2),
              new GreaterThanOrEqual(maxColumn.column, e2)),
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
   * This expression ensures if ANY stats column in column stats filter is missing in one AddFile,
   * disable the column stats filter for this file. This expression builds only once per query.
   *
   * @param   referencedStats All of stats column exists in the column stats filter.
   * @return  the verifying expression, evaluated as true if the column stats filter and stats value
   *          in current [[AddFile]] is valid, false when it is invalid.
   */
  def verifyStatsForFilter(
      referencedStats: Set[ReferencedStats]): Expression = {
    referencedStats.map { refStats =>
      val pathToColumn = refStats.pathToColumn
      pathToColumn.head match {
        case MAX | MIN if pathToColumn.length == 2 =>
          // We would allow MIN or MAX missing only if the corresponding data column value are all
          // NULL, e.g.: NUM_RECORDS == NULL_COUNT
          val nullCount = new Column(NULL_COUNT + "." + refStats.pathToColumn.last, new LongType())
          val numRecords = new Column(NUM_RECORDS, new LongType())
          new Or(new IsNotNull(refStats.column), new EqualTo(nullCount, numRecords))

        case _ if pathToColumn.length == 1 => new IsNotNull(refStats.column)
        case _ => return Literal.False
      }
    }.reduceLeft(new And(_, _))
  }
}
