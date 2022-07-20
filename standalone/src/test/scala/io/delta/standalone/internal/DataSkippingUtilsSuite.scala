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

import io.delta.standalone.internal.util.DataSkippingUtils
/*
case class DataSkippingUtilsSuite extends FunSuite {
  /**
   * The unit test method for constructDataFilter.
   * @param statsString the stats string in JSON format
   * @param fileStatsTarget the target output of file-specific stats
   * @param columnStatsTarget the target output of column-specific stats
   * @param isNestedSchema if we will use nested schema for column stats
   */
  def parseColumnStatsTest(
                            statsString: String,
                            fileStatsTarget: Map[String, Long],
                            columnStatsTarget: Map[String, Long],
                            isNestedSchema: Boolean = false): Unit = {
    val s = if (isNestedSchema) nestedSchema else schema
    val (_, fileStats, columnStats) = DataSkippingUtils.parseColumnStats(
      tableSchema = s, statsString = statsString)
    // TODO validate stats schema
    // TODO add verification test
    assert(fileStats == fileStatsTarget)
    assert(columnStats == columnStatsTarget)
  }

  /**
   * Unit test - parseColumnStats
   */
  test("parse column stats: basic") {
    val fileStatsTarget = Map("numRecords" -> 1L)
    val columnStatsTarget = Map(
      "maxValues.partitionCol" -> 1L, "nullCount.col2" -> 0L, "minValues.col2" -> 1L,
      "maxValues.col1" -> 1L, "minValues.partitionCol" -> 1L, "maxValues.col2" -> 1L,
      "nullCount.col1" -> 0L, "minValues.col1" -> 1L, "nullCount.stringCol" -> 1L,
      "nullCount.partitionCol" -> 0L)
    // Though `stringCol` is not LongType, its `nullCount` stats will be documented
    // while `minValues` and `maxValues` won't be.
    parseColumnStatsTest(unwrappedStats, fileStatsTarget, columnStatsTarget)
  }

  test("parse column stats: ignore nested columns") {
    val inputStats = """{"minValues":{"normalCol": 1, "parentCol":{"subCol1": 1, "subCol2": 2}}}"""
    val fileStatsTarget = Map[String, Long]()
    val columnStatsTarget = Map("minValues.normalCol" -> 1L)
    parseColumnStatsTest(
      inputStats, fileStatsTarget, columnStatsTarget, isNestedSchema = true)
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
    val inputStats = """{"minValues":{"partitionCol": 1, "col1": 2}}"""
    val fileStatsTarget = Map[String, Long]()
    val columnStatsTarget = Map[String, Long](
      "minValues.partitionCol" -> 1, "minValues.col1" -> 2)
    parseColumnStatsTest(inputStats, fileStatsTarget, columnStatsTarget)
  }
}
 */