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

import java.sql.{Date, Timestamp}

import com.fasterxml.jackson.core.io.JsonEOFException
import org.scalatest.FunSuite

import io.delta.standalone.expressions.{And, Column, EqualTo, Expression, GreaterThanOrEqual, IsNotNull, LessThanOrEqual, Literal}
import io.delta.standalone.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

import io.delta.standalone.internal.data.ColumnStatsRowRecord
import io.delta.standalone.internal.util.DataSkippingUtils
import io.delta.standalone.internal.util.DataSkippingUtils.{MAX, MIN, NULL_COUNT, NUM_RECORDS}

/** The unit tests of helper methods in [[DataSkippingUtils]]. */
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

  private def testException[T <: Throwable](f: => Any, messageContains: String)
      (implicit manifest: Manifest[T]) = {
    val e = intercept[T]{
      f
    }.getMessage
    assert(e.contains(messageContains))
  }

  test("unit test: build stats schema") {
    // build stats schema: basic table schema
    var output = DataSkippingUtils.buildStatsSchema(schema)
    val nullCountSchema = new StructType(schema.getFields.map { field =>
      new StructField(field.getName, new LongType)
    })
    val expectedStatsSchema = new StructType(Array(
      new StructField(MIN, schema),
      new StructField(MAX, schema),
      new StructField(NULL_COUNT, nullCountSchema),
      new StructField(NUM_RECORDS, new LongType)))
    assert(output == expectedStatsSchema)

    // build stats schema: ignore nested columns
    output = DataSkippingUtils.buildStatsSchema(nestedSchema)
    assert(output == new StructType())
  }

  test("unit test: parse column stats") {
    /**
     * @param statsString         The stats string in JSON format.
     * @param expectedFileStats   The expected output of file-specific stats.
     * @param expectedColumnStats The expected output of column-specific stats.
     * @param isNestedSchema      If we will use nested schema for column stats.
     */
    def testParseColumnStats(
        statsString: String,
        expectedFileStats: Map[String, Long],
        expectedColumnStats: Map[String, Long],
        isNestedSchema: Boolean = false): Unit = {
      val s = if (isNestedSchema) nestedSchema else schema
      val (fileStats, columnStats) = DataSkippingUtils.parseColumnStats(
        dataSchema = s, statsString = statsString)
      assert(fileStats == expectedFileStats)
      assert(columnStats == expectedColumnStats)
    }

    var expectedFileStats = Map("numRecords" -> 3L)
    var expectedColumnStats = Map("minValues.col2" -> 2L, "minValues.col1" -> 1L)
    // Though `stringCol` is not LongType, its `nullCount` stats will be documented
    // while `minValues` and `maxValues` won't be.
    testParseColumnStats(columnStats, expectedFileStats, expectedColumnStats)

    // parse column stats: ignore nested column
    expectedFileStats = Map[String, Long]()
    expectedColumnStats = Map[String, Long]()
    testParseColumnStats(
      nestedColStats, expectedFileStats, expectedColumnStats, isNestedSchema = true)

    // parse column stats: wrong JSON format
    expectedFileStats = Map[String, Long]()
    expectedColumnStats = Map[String, Long]()
    val e = intercept[JsonEOFException] {
      testParseColumnStats(statsString = brokenStats, expectedFileStats, expectedColumnStats)
    }
    assert(e.getMessage.contains("Unexpected end-of-input in field name"))

    // parse column stats: missing stats from schema
    expectedFileStats = Map[String, Long](s"$NUM_RECORDS" -> 2)
    expectedColumnStats = Map[String, Long](s"$MIN.col1" -> 1)
    testParseColumnStats(missingColumnStats, expectedFileStats, expectedColumnStats)

    // parse column stats: duplicated stats name
    val duplicatedStats = s"""{"$MIN":{"col1":1,"col1":2},"numRecords":3}"""
    expectedFileStats = Map[String, Long](s"$NUM_RECORDS" -> 3)
    expectedColumnStats = Map[String, Long](s"$MIN.col1" -> 2)
    testParseColumnStats(duplicatedStats, expectedFileStats, expectedColumnStats)

    // parse column stats: conflict stats type
    // Error will not raise because `minValues` will not be stored in the file-specific stats map.
    val conflictStatsType = s"""{"$MIN":{"col1":1,"col2":2},"$MIN":3}"""
    testParseColumnStats(conflictStatsType, Map[String, Long](), Map[String, Long]())

    // parse column stats: wrong data type for a known stats type
    // NUM_RECORDS should be LongType but is StringType here. The method raise error and should be
    // handle by caller.
    val wrongStatsDataType = s"""{"$MIN":{"col1":1,"col2":2},"$NUM_RECORDS":"a"}"""
    testException[NumberFormatException](
      testParseColumnStats(wrongStatsDataType, Map[String, Long](), Map[String, Long]()),
      "For input string: ")
  }

  test("unit test: filter construction") {
    /**
     * @param inputExpr          The query predicate as input.
     * @param expectedOutputExpr The expected column stats filter as output.
     */
    def testConstructDataFilter(
        inputExpr: Option[Expression],
        expectedOutputExpr: Option[Expression]): Unit = {
      val output = DataSkippingUtils.constructDataFilters(
        dataSchema = schema, dataConjunction = inputExpr)

      assert(expectedOutputExpr == output)
    }

    /** Building the column stats filter from equalTo operation. */
    def eqCast(colName: String, colType: DataType, l: Literal): Expression = {
      val colMin = new Column(s"$MIN.$colName", colType)
      val colMax = new Column(s"$MAX.$colName", colType)
      new And(
        new LessThanOrEqual(colMin, l),
        new GreaterThanOrEqual(colMax, l))
    }

    val col1 = new Column("col1", new LongType)
    val col2 = new Column("col2", new LongType)

    val long1 = Literal.of(1L)
    val long2 = Literal.of(2L)

    // col1 == 1
    testConstructDataFilter(
      inputExpr = Some(new EqualTo(col1, long1)),
      expectedOutputExpr = Some(eqCast("col1", new LongType, long1)))

    // col1 == 1 AND col2 == 1
    testConstructDataFilter(
      inputExpr = Some(new And(
        new EqualTo(col1, long1),
        new EqualTo(col2, long2))),
      expectedOutputExpr = Some(new And(
        eqCast("col1", new LongType, long1),
        eqCast("col2", new LongType, long2))))

    // col1 >= 1, `>=` is not supported
    testConstructDataFilter(
      inputExpr = Some(new GreaterThanOrEqual(col1, long1)),
      expectedOutputExpr = None)

    // `col1 IS NOT NULL` is not supported
    testConstructDataFilter(
      inputExpr = Some(new IsNotNull(col1)),
      expectedOutputExpr = None)

    // stringCol = 1, StringType is not supported
    testConstructDataFilter(
      inputExpr = Some(new EqualTo(new Column("stringCol", new StringType), Literal.of("1"))),
      expectedOutputExpr = None)

    // empty expression will return if stats is missing
    testConstructDataFilter(
      inputExpr = Some(new EqualTo(new Column("col3", new LongType), long1)),
      expectedOutputExpr = None)
  }

  test("unit test: column stats row record") {
    /**
     * @param dataType         The data type of testing field.
     * @param nullable         Whether this field is nullable.
     * @param fileStatsValue   The value of file-specific stats.
     * @param columnStatsValue The value of column-specific stats.
     * @param name             The field name.
     * @return
     */
    def buildColumnStatsRowRecord(
        dataType: DataType,
        nullable: Boolean,
        fileStatsValue: Long,
        columnStatsValue: Long,
        name: String = "test"): ColumnStatsRowRecord = {
      new ColumnStatsRowRecord(
        new StructType(Array(new StructField(name, dataType, nullable))),
        Map(name -> fileStatsValue), Map(name -> columnStatsValue))
    }

    val testStatsRowRecord = buildColumnStatsRowRecord(
      new LongType(), nullable = true, fileStatsValue = 10L, columnStatsValue = 5L)
    assert(buildColumnStatsRowRecord(new LongType(), nullable = true, fileStatsValue = 5L,
      columnStatsValue = 10L).isNullAt("test"))
    // non-nullable field
    assert(buildColumnStatsRowRecord(new LongType(), nullable = false, fileStatsValue = 5L,
      columnStatsValue = 5L).isNullAt("test"))

    assert(testStatsRowRecord.isNullAt("test"))

    // Since [[ColumnStatsRowRecord.isNullAt]] is used in the evaluation of IsNull and IsNotNull
    // expressions, it will return TRUE for IsNull(missingStats), which could be an incorrect
    // result. Here we avoid this problem by not using IsNull expression as a part of any column
    // stats filter.
    assert(testStatsRowRecord.isNullAt("foo"))
    // "Field \"foo\" does not exist."

    // primitive types can't be null
    // for primitive type T: (DataType, getter: ColumnStatsRowRecord => T, value: String, value: T)
    val primTypes = Seq(
      (new IntegerType, (x: ColumnStatsRowRecord) => x.getInt("test"), 0L, 0),
      (new ByteType, (x: ColumnStatsRowRecord) => x.getByte("test"), 0L, 0.toByte),
      (new ShortType, (x: ColumnStatsRowRecord) => x.getShort("test"), 0L, 0.toShort),
      (new BooleanType, (x: ColumnStatsRowRecord) => x.getBoolean("test"), 0L, true),
      (new FloatType, (x: ColumnStatsRowRecord) => x.getFloat("test"), 0L, 0.0F),
      (new DoubleType, (x: ColumnStatsRowRecord) => x.getDouble("test"), 0L, 0.0))

    primTypes.foreach {
      case (dataType: DataType, f: (ColumnStatsRowRecord => Any), l: Long, _) =>
        // reserves a dummy parameter for adding data type support later.
        testException[UnsupportedOperationException](
          f(buildColumnStatsRowRecord(dataType, nullable = true, l, l)),
          s"${dataType.getTypeName} is not a supported column stats type.")
    }

    val nonPrimTypes = Seq(
      (new BinaryType, (x: ColumnStatsRowRecord) => x.getBinary("test"), "\u0001\u0005\u0008"),
      (new DecimalType(1, 1), (x: ColumnStatsRowRecord) => x.getBigDecimal("test"), "0.123"),
      (new TimestampType, (x: ColumnStatsRowRecord) => x.getTimestamp("test"),
        new Timestamp(123456789)),
      (new DateType, (x: ColumnStatsRowRecord) => x.getDate("test"), Date.valueOf("1970-01-01")))

    nonPrimTypes.foreach {
      case (dataType: DataType, f: (ColumnStatsRowRecord => Any), _) =>
        testException[UnsupportedOperationException](
          f(buildColumnStatsRowRecord(dataType, nullable = true, 0L, 0L)),
          s"${dataType.getTypeName} is not a supported column stats type.")
    }

    testException[UnsupportedOperationException](
      testStatsRowRecord.getRecord("test"),
      "Struct is not a supported column stats type.")
    testException[UnsupportedOperationException](
      testStatsRowRecord.getList("test"),
      "List is not a supported column stats type.")
    testException[UnsupportedOperationException](
      testStatsRowRecord.getMap("test"),
      "Map is not a supported column stats type.")
  }
}
