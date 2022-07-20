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

import io.delta.standalone.expressions.{And, Column, EqualTo, Expression, GreaterThanOrEqual, IsNotNull, LessThanOrEqual, Literal}
import io.delta.standalone.types.{LongType, StringType, StructField, StructType}

import io.delta.standalone.internal.util.{DataSkippingUtils, ReferencedStats}
import io.delta.standalone.internal.util.DataSkippingUtils.{MAX, MIN, NULL_COUNT, NUM_RECORDS}

case class DataSkippingUtilsSuite() extends FunSuite {
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

  /**
   * The unit test method for constructDataFilter.
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
    val (_, fileStats, columnStats) = DataSkippingUtils.parseColumnStats(
      tableSchema = s, statsString = statsString)
    assert(fileStats == fileStatsTarget)
    assert(columnStats == columnStatsTarget)
  }

  /**
   * Unit test - parseColumnStats
   */
  test("parse column stats: basic") {
    val fileStatsTarget = Map("numRecords" -> 3L)
    val columnStatsTarget = Map("minValues.col2" -> 2L, "minValues.col1" -> 1L)
    // Though `stringCol` is not LongType, its `nullCount` stats will be documented
    // while `minValues` and `maxValues` won't be.
    parseColumnStatsTest(columnStats, fileStatsTarget, columnStatsTarget)
  }

  test("parse column stats: ignore nested columns") {
    val fileStatsTarget = Map[String, Long]()
    val columnStatsTarget = Map[String, Long]()
    parseColumnStatsTest(
      nestedColStats, fileStatsTarget, columnStatsTarget, isNestedSchema = true)
  }

  test("parse column stats: wrong JSON format") {
    val fileStatsTarget = Map[String, Long]()
    val columnStatsTarget = Map[String, Long]()
    val e = intercept[JsonEOFException] {
      parseColumnStatsTest(statsString = brokenStats,
        fileStatsTarget, columnStatsTarget)
    }
    assert(e.getMessage.contains("Unexpected end-of-input in field name"))
  }

  test("parse column stats: missing stats from schema") {
    val fileStatsTarget = Map[String, Long](s"$NUM_RECORDS" -> 2)
    val columnStatsTarget = Map[String, Long](s"$MIN.col1" -> 1)
    parseColumnStatsTest(missingColumnStats, fileStatsTarget, columnStatsTarget)
  }

  test("parse column stats: duplicated stats name") {
    // Error will not raise because only one `col1` key is available in map
    val duplicatedStats = s"""{"$MIN":{"col1":1,"col1":2},"numRecords":3}"""
    val fileStatsTarget = Map[String, Long](s"$NUM_RECORDS" -> 3)
    val columnStatsTarget = Map[String, Long](s"$MIN.col1" -> 2)
    parseColumnStatsTest(duplicatedStats, fileStatsTarget, columnStatsTarget)
  }

  test("parse column stats: conflict stats type") {
    // Error will not raise because `minValues` will not used as the file-specific stats
    val conflictStatsType = s"""{"$MIN":{"col1":1,"col1":2},"$MIN":3}"""
    parseColumnStatsTest(conflictStatsType, Map[String, Long](), Map[String, Long]())
  }

  /**
   * The unit test method for constructDataFilter.
   * @param hits    the valid expression with its target output
   * @param misses  the invalid expression, the expected output should be empty
   */
  def constructDataFilterTests(
      hits: Seq[(Expression, String, Set[ReferencedStats])],
      misses: Seq[Expression]): Unit = {
    hits.foreach { hit =>
      val output = DataSkippingUtils.constructDataFilters(
        tableSchema = schema,
        expression = hit._1)

      assert(hit._2 == output.get.expr.toString)
      assert(hit._3 == output.get.referencedStats)
    }

    misses.foreach { miss =>
      val output = DataSkippingUtils.constructDataFilters(
        tableSchema = schema,
        expression = miss)
      assert(output.isEmpty)
    }
  }

  test("unit test: filter construction") {
    val col1Min = new Column(s"$MIN.col1", new LongType)
    val col1MinRef = ReferencedStats(Seq(MIN, "col1"), col1Min)
    val col1Max = new Column(s"$MAX.col1", new LongType)
    val col1MaxRef = ReferencedStats(Seq(MAX, "col1"), col1Max)
    val col2Min = new Column(s"$MIN.col2", new LongType)
    val col2MinRef = ReferencedStats(Seq(MIN, "col2"), col2Min)
    val col2Max = new Column(s"$MAX.col2", new LongType)
    val col2MaxRef = ReferencedStats(Seq(MAX, "col2"), col2Max)

    constructDataFilterTests(
      hits = Seq(
        // col1 = 1
        (new EqualTo(new Column("col1", new LongType), Literal.of(1L)),
          s"((Column($MIN.col1) <= 1) && (Column($MAX.col1) >= 1))",
          Set(col1MinRef, col1MaxRef)),

        // col1 = 1 AND col2 = 1
        (new And(new EqualTo(new Column("col1", new LongType), Literal.of(1L)),
          new EqualTo(new Column("col2", new LongType), Literal.of(2L))),
          s"(((Column($MIN.col1) <= 1) && (Column($MAX.col1) >= 1)) &&" +
            s" ((Column($MIN.col2) <= 2) && (Column($MAX.col2) >= 2)))",
          Set(col1MinRef, col1MaxRef, col2MinRef, col2MaxRef)),
      ),
      misses = Seq(
        // col1 >= 1, '>=' is not supported
        new GreaterThanOrEqual(new Column("col1", new LongType), Literal.of(1L)),

        // col1 IS NOT NULL, is not supported
        new IsNotNull(new Column("col1", new LongType)),

        // stringCol = 1, StringType is not supported
        new EqualTo(new Column("stringCol", new StringType), Literal.of("1")),

        // empty expression will return if stats is missing
        new EqualTo(new Column("col3", new LongType), Literal.of(1L))
      ))
  }

  /**
   * Unit test method for method `verifyStatsFilter`
   * @param refStatsNames the referenced columns in stats
   * @param target        the target expression in string format
   */
  def verifyStatsFilterTest(refStatsNames: Set[Seq[String]], target: String): Unit = {
    val refStats = refStatsNames.map { refStatsName =>
      val columnName = refStatsName.mkString(".")
      ReferencedStats(refStatsName, new Column(columnName, new LongType))
    }
    val output = DataSkippingUtils.verifyStatsForFilter(refStats)
    assert(output.toString == target)
  }

  test("unit test: verifyStatsForFilter") {
    verifyStatsFilterTest(Set(Seq(MIN, "col1")),
      target = s"((Column($MIN.col1)) IS NOT NULL || " +
        s"(Column($NULL_COUNT.col1) = Column($NUM_RECORDS)))")

    verifyStatsFilterTest(Set(Seq(NUM_RECORDS)),
      target = s"(Column($NUM_RECORDS)) IS NOT NULL")

    verifyStatsFilterTest(Set(Seq(NUM_RECORDS, "col1")),
      target = s"false")

    verifyStatsFilterTest(Set(Seq("wrong_stats", "col1")),
      target = s"false")

    verifyStatsFilterTest(Set(Seq(MIN, "col1"), Seq(NUM_RECORDS)),
      target = s"(((Column($MIN.col1)) IS NOT NULL || " +
        s"(Column($NULL_COUNT.col1) = Column($NUM_RECORDS))) && " +
        s"(Column($NUM_RECORDS)) IS NOT NULL)")
  }

}
