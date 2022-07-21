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

package io.delta.standalone.internal

import com.fasterxml.jackson.core.io.JsonEOFException
import org.scalatest.FunSuite

import io.delta.standalone.expressions.{And, Column, EqualTo, Expression, GreaterThanOrEqual, IsNotNull, LessThanOrEqual, Literal, Or}
import io.delta.standalone.types.{DataType, LongType, StringType, StructField, StructType}

import io.delta.standalone.internal.util.{DataSkippingUtils, ReferencedStats}
import io.delta.standalone.internal.util.DataSkippingUtils.{MAX, MIN, NULL_COUNT, NUM_RECORDS}

class DataSkippingUtilsSuite extends FunSuite {
  private val schema = new StructType(Array(
    new StructField("col1", new LongType(), true),
    new StructField("col2", new LongType(), true),
    new StructField("stringCol", new StringType(), true)))

  private val nestedSchema = new StructType(Array(
    new StructField("parentCol", new StructType(Array(
      new StructField("subCol1", new LongType(), true),
      new StructField("subCol2", new LongType(), true))), true)))

  private val columnStats = s"""{"$MIN":{"col1":1,"col2":2},"$NUM_RECORDS":3}"""
  private val brokenStats = columnStats.substring(0, 10)
  private val missingColumnStats = s"""{"$MIN":{"col1":1},"$NUM_RECORDS":2}"""
  private val nestedColStats =
   s"""{"$MIN":{"normalCol": 1, "parentCol":{"subCol1": 2, "subCol2": 3}}}"""


  test("unit test: build stats schema") {
    // build stats schema: basic table schema
    var output = DataSkippingUtils.buildStatsSchema(schema)
    assert(output.length() == 4)
    assert(output.hasFieldName(MAX) &&
      output.hasFieldName(MIN) &&
      output.hasFieldName(NULL_COUNT) &&
      output.hasFieldName(NUM_RECORDS))
    assert(output.get(MAX).getDataType == schema &&
      output.get(MIN).getDataType == schema &&
      output.get(NUM_RECORDS).getDataType.isInstanceOf[LongType])

    val ncSchema = output.get(NULL_COUNT).getDataType
    assert(ncSchema.isInstanceOf[StructType])
    val ncFields = ncSchema.asInstanceOf[StructType].getFields
    assert(ncFields.map(_.getDataType.isInstanceOf[LongType]).reduce(_ && _))
    assert(ncFields.map(_.getName) sameElements schema.getFieldNames)

    // build stats schema: ignore nested columns
    output = DataSkippingUtils.buildStatsSchema(nestedSchema)
    assert(output.length() == 0)
  }

  /**
   * The unit test method for [[DataSkippingUtils.constructDataFilters]].
   * @param statsString       the stats string in JSON format
   * @param fileStatsTarget   the target output of file-specific stats
   * @param columnStatsTarget the target output of column-specific stats
   * @param isNestedSchema    if we will use nested schema for column stats
   */
  def parseColumnStatsTest(
      statsString: String,
      fileStatsTarget: Map[String, Long],
      columnStatsTarget: Map[String, Long],
      isNestedSchema: Boolean = false): Unit = {
    val s = if (isNestedSchema) nestedSchema else schema
    val (fileStats, columnStats) = DataSkippingUtils.parseColumnStats(
      tableSchema = s, statsString = statsString)
    assert(fileStats == fileStatsTarget)
    assert(columnStats == columnStatsTarget)
  }

  test("unit test: parse column stats") {
    var fileStatsTarget = Map("numRecords" -> 3L)
    var columnStatsTarget = Map("minValues.col2" -> 2L, "minValues.col1" -> 1L)
    // Though `stringCol` is not LongType, its `nullCount` stats will be documented
    // while `minValues` and `maxValues` won't be.
    parseColumnStatsTest(columnStats, fileStatsTarget, columnStatsTarget)

    // parse column stats: ignore nested column
    fileStatsTarget = Map[String, Long]()
    columnStatsTarget = Map[String, Long]()
    parseColumnStatsTest(
      nestedColStats, fileStatsTarget, columnStatsTarget, isNestedSchema = true)

    // parse column stats: wrong JSON format
    fileStatsTarget = Map[String, Long]()
    columnStatsTarget = Map[String, Long]()
    val e = intercept[JsonEOFException] {
      parseColumnStatsTest(statsString = brokenStats,
        fileStatsTarget, columnStatsTarget)
    }
    assert(e.getMessage.contains("Unexpected end-of-input in field name"))

    // parse column stats: missing stats from schema
    fileStatsTarget = Map[String, Long](s"$NUM_RECORDS" -> 2)
    columnStatsTarget = Map[String, Long](s"$MIN.col1" -> 1)
    parseColumnStatsTest(missingColumnStats, fileStatsTarget, columnStatsTarget)

    // parse column stats: duplicated stats name
    val duplicatedStats = s"""{"$MIN":{"col1":1,"col1":2},"numRecords":3}"""
    fileStatsTarget = Map[String, Long](s"$NUM_RECORDS" -> 3)
    columnStatsTarget = Map[String, Long](s"$MIN.col1" -> 2)
    parseColumnStatsTest(duplicatedStats, fileStatsTarget, columnStatsTarget)

    // parse column stats: conflict stats type
    // Error will not raise because `minValues` will not used as the file-specific stats
    val conflictStatsType = s"""{"$MIN":{"col1":1,"col1":2},"$MIN":3}"""
    parseColumnStatsTest(conflictStatsType, Map[String, Long](), Map[String, Long]())
  }

  /**
   * The unit test method for successful constructDataFilter with valid output.
   * @param input           the query predicate as input
   * @param targetExpr      the target column stats filter as output
   * @param targetRefStats  the target referenced stats appears in the [[targetExpr]]
   */
  def successConstructDataFilterTests(
      input: Expression,
      targetExpr: Expression,
      targetRefStats: Set[ReferencedStats]): Unit = {
    val output = DataSkippingUtils.constructDataFilters(
      tableSchema = schema, expression = input)

    assert(targetExpr == output.get.expr)
    assert(targetRefStats == output.get.referencedStats)
  }

  /**
   * The unit test method for failed constructDataFilter.
   * @param input the query predicate as input
   */
  def failConstructDataFilterTests(input: Expression): Unit = {
    val output = DataSkippingUtils.constructDataFilters(
      tableSchema = schema, expression = input)
    assert(output.isEmpty)
  }

  /** Helper function for building the column stats filter from equalTo operation */
  def eqCast(colName: String, colType: DataType, l: Literal): Expression = {
    val colMin = new Column(s"$MIN.$colName", colType)
    val colMax = new Column(s"$MAX.$colName", colType)
    new And(new LessThanOrEqual(colMin, l), new GreaterThanOrEqual(colMax, l))
  }


  test("unit test: filter construction") {
    val col1 = new Column("col1", new LongType)
    val col2 = new Column("col2", new LongType)

    val long1 = Literal.of(1L)
    val long2 = Literal.of(2L)

    val col1Min = new Column(s"$MIN.col1", new LongType)
    val col1MinRef = ReferencedStats(Seq(MIN, "col1"), col1Min)
    val col1Max = new Column(s"$MAX.col1", new LongType)
    val col1MaxRef = ReferencedStats(Seq(MAX, "col1"), col1Max)
    val col2Min = new Column(s"$MIN.col2", new LongType)
    val col2MinRef = ReferencedStats(Seq(MIN, "col2"), col2Min)
    val col2Max = new Column(s"$MAX.col2", new LongType)
    val col2MaxRef = ReferencedStats(Seq(MAX, "col2"), col2Max)

    // col1 == 1
    successConstructDataFilterTests(
      input = new EqualTo(col1, long1),
      targetExpr = eqCast("col1", new LongType, long1),
      targetRefStats = Set(col1MinRef, col1MaxRef))

    // col1 == 1 AND col2 == 1
    successConstructDataFilterTests(
      input = new And(new EqualTo(col1, long1), new EqualTo(col2, long2)),
      targetExpr = new And(eqCast("col1", new LongType, long1),
        eqCast("col2", new LongType, long2)),
      targetRefStats = Set(col1MinRef, col1MaxRef, col2MinRef, col2MaxRef))

    // col1 >= 1, `>=` is not supported
    failConstructDataFilterTests(
      new GreaterThanOrEqual(col1, long1))

    // `col1 IS NOT NULL` is not supported
    failConstructDataFilterTests(new IsNotNull(col1))

    // stringCol = 1, StringType is not supported
    failConstructDataFilterTests(
      new EqualTo(new Column("stringCol", new StringType), Literal.of("1")))

    // empty expression will return if stats is missing
    failConstructDataFilterTests(new EqualTo(new Column("col3", new LongType), long1))
  }

  /**
   * Unit test method for method `verifyStatsFilter`
   * @param refStatsNames the referenced columns in stats
   * @param target        the target expression in string format
   */
  def verifyStatsFilterTest(refStatsNames: Set[Seq[String]], target: Expression): Unit = {
    val refStats = refStatsNames.map { refStatsName =>
      val columnName = refStatsName.mkString(".")
      ReferencedStats(refStatsName, new Column(columnName, new LongType))
    }
    val output = DataSkippingUtils.verifyStatsForFilter(refStats)
    assert(output == target)
  }
  /** Helper method for generating verifying expression for MIN/MAX stats */
  def verifyMinMax(statsType: String, colName: String, colType: DataType): Expression = {
    val notNullExpr = verifyStatsCol(statsType, Some(colName), colType)
    val nullCountCol = new Column(s"$NULL_COUNT.$colName", new LongType)
    val numRecordsCol = new Column(NUM_RECORDS, new LongType)
    new Or(notNullExpr, new EqualTo(nullCountCol, numRecordsCol))
  }

  /** Helper method for generating verifying expression */
  def verifyStatsCol(statsType: String, colName: Option[String], colType: DataType): Expression = {
    colName match {
      case Some(s) => new IsNotNull(new Column(s"$statsType.$s", colType))
      case None => new IsNotNull(new Column(statsType, colType))
      case _ => null // should not happen
    }
  }

  test("unit test: verifyStatsForFilter") {
    // verify col1.MIN
    verifyStatsFilterTest(Set(Seq(MIN, "col1")),
      target = verifyMinMax(MIN, "col1", new LongType))

    // verify NUM_RECORDS
    verifyStatsFilterTest(Set(Seq(NUM_RECORDS)),
      target = verifyStatsCol(NUM_RECORDS, None, new LongType))

    // NUM_RECORDS should be a file-specific stats, verification failed
    verifyStatsFilterTest(Set(Seq(NUM_RECORDS, "col1")),
      target = Literal.False)

    // Unidentified stats type, verification failed
    verifyStatsFilterTest(Set(Seq("wrong_stats", "col1")),
      target = Literal.False)

    // verify col1.MAX and NUM_RECORDS
    verifyStatsFilterTest(Set(Seq(MAX, "col1"), Seq(NUM_RECORDS)),
      target = new And(verifyMinMax(MAX, "col1", new LongType),
        verifyStatsCol(NUM_RECORDS, None, new LongType)))
  }

}
