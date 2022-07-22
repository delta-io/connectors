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

import io.delta.standalone.expressions.{And, Column, EqualTo, Expression, GreaterThanOrEqual, IsNotNull, LessThanOrEqual, Literal, Or}
import io.delta.standalone.types.{DataType, LongType, StructField, StructType}

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
 * @param expr            the transformed expression for column stats filter.
 * @param referencedStats columns appears in [[expr]].
 */
private [internal] case class ColumnStatsPredicate(
    expr: Expression,
    referencedStats: Set[ReferencedStats])

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
   * Build [[statsSchema]] based on the table schema, the first layer of stats schema is stats type.
   * If is a column-specific stats, it nested a second layer, which contains the column name in
   * table schema. Or if it is a column-specific stats, contains a non-nested data type.
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
   * Parse the stats in table metadata files to two maps. The output contains two map
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
   * Currently nested column is not supported, only [[LongType]] is the supported data type.
   *
   * @param tableSchema The table schema describes data column (not stats column) for this query.
   * @param statsString The json-formatted stats in raw string type in table metadata files.
   * @return file-specific stats map:   the map stores file-specific stats, like [[NUM_RECORDS]]
   *         column-specific stats map: the map stores column-specific stats, like [[MIN]],
   *         [[MAX]], [[NULL_COUNT]].
   */
  def parseColumnStats(
      tableSchema: StructType,
      statsString: String): (Map[String, Long], Map[String, Long]) = {
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

  /** Helper function of building [[ReferencedStats]] */
  def refStatsBuilder(
      statsType: String,
      columnName: String,
      dataType: DataType): ReferencedStats = {
    ReferencedStats(
      Seq(statsType, columnName),
      new Column(statsType + "." + columnName, dataType))
  }

  /**
   * Build the column stats filter based on query predicate.
   *
   * Assume `col1` and `col2` are columns in query predicate, `literal1` and `literal2` are two
   * literal values in query predicate. Now two rules are applied:
   * - (col1 == literal2) -> (MIN.col1 <= literal2 AND MAX.col1 >= literal2)
   * - constructDataFilters(expr1 AND expr2) ->
   *      constructDataFilters(expr1) AND constructDataFilters(expr2)
   *
   * @param statsSchema      The schema describes the structure of stats columns
   * @param dataConjunction  The non-partition column query predicate.
   * @return columnStatsPredicate: Return the column stats filter expression, and the set of stat
   *         columns that are referenced in the filter expression, please see
   *         [[ColumnStatsPredicate]].
   *         Or it will return None if met missing stats, unsupported data type, or unsupported
   *         expression type issues.
   */
  def constructDataFilters(
      statsSchema: StructType,
      dataConjunction: Expression): Option[ColumnStatsPredicate] =
    dataConjunction match {
      case eq: EqualTo => (eq.getLeft, eq.getRight) match {
        case (e1: Column, e2: Literal) =>
          val columnPath = e1.name
          if (!(statsSchema.contains(columnPath) &&
            statsSchema.get(columnPath).getDataType.isInstanceOf[LongType])) {
              // Only accepting the LongType column for now.
              return None
            }
          val minColumn = refStatsBuilder(MIN, columnPath, new LongType)
          val maxColumn = refStatsBuilder(MAX, columnPath, new LongType)

          Some(ColumnStatsPredicate(
            new And(
              new LessThanOrEqual(minColumn.column, e2),
              new GreaterThanOrEqual(maxColumn.column, e2)),
            Set(minColumn, maxColumn)))
        case _ => None
      }
      case and: And =>
        val e1 = constructDataFilters(statsSchema, and.getLeft)
        val e2 = constructDataFilters(statsSchema, and.getRight)

        if (e1.isDefined && e2.isDefined) {
          Some(ColumnStatsPredicate(
            new And(e1.get.expr, e2.get.expr),
            e1.get.referencedStats ++ e2.get.referencedStats))
        } else None

      // TODO: support full types of Expression
      case _ => None
    }

  /**
   * This expression ensures if ANY stats column in column stats filter is missing in one table
   * metadata file, disable the column stats filter for this file.
   *
   * @param   referencedStats All of stats column exists in the column stats filter.
   * @return  the verifying expression, evaluated as true if the column stats filter and stats value
   *          in current table metadata file is valid, false when it is invalid.
   */
  def verifyStatsForFilter(
      referencedStats: Set[ReferencedStats]): Expression = {
    referencedStats.map { refStats =>
      val pathToColumn = refStats.pathToColumn
      pathToColumn.head match {
        case MAX | MIN if pathToColumn.length == columnStatsPathLength =>
          // We would allow MIN or MAX missing only if the corresponding data column value are all
          // NULL, e.g.: NUM_RECORDS == NULL_COUNT
          val nullCount = new Column(NULL_COUNT + "." + refStats.pathToColumn.last, new LongType())
          val numRecords = new Column(NUM_RECORDS, new LongType())
          new Or(new IsNotNull(refStats.column), new EqualTo(nullCount, numRecords))

        case _ if pathToColumn.length == fileStatsPathLength => new IsNotNull(refStats.column)
        case _ => return Literal.False
      }
    }.reduceLeft(new And(_, _))
  }
}
