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

package io.delta.standalone.internal.scan

import java.util.Optional

import scala.collection.mutable

import com.fasterxml.jackson.databind.JsonNode

import io.delta.standalone.expressions.Expression
import io.delta.standalone.types.{DataType, StructType}

import io.delta.standalone.internal.actions.{AddFile, MemoryOptimizedLogReplay}
import io.delta.standalone.internal.data.PartitionRowRecord
import io.delta.standalone.internal.util.{JsonUtils, PartitionUtils}

/**
 * The struct store column stats based on table schema, nested columns are supported.
 * @param dataType For column stores sub columns, this value is [[StructType]]. For columns stores
 *                 data, this value is the corresponding data type.
 * @param columnPath This is a sequence of string saves the complete path in current table.
 * @param nestedColumns The sub column that current column contained if this is an
 *                      structured column.
 * @param statsValue The stats that current column contained if this is a leaf column.
 */
private[internal] case class ColumnNode(
    dataType: DataType = new StructType(),
    columnPath: Seq[String] = Nil,
    nestedColumns: mutable.Map[String, ColumnNode]
    = mutable.Map.empty[String, ColumnNode],
    statsValue: mutable.Map[String, String] = mutable.Map.empty[String, String])

/**
 * An implementation of [[io.delta.standalone.DeltaScan]] that filters files and only returns
 * those that match the [[getPushedPredicate]].
 *
 * If the pushed predicate is empty, then all files are returned.
 */
final private[internal] class FilteredDeltaScanImpl(
    replay: MemoryOptimizedLogReplay,
    expr: Expression,
    partitionSchema: StructType) extends DeltaScanImpl(replay) {

  private val partitionColumns = partitionSchema.getFieldNames.toSeq

  private val (metadataConjunction, dataConjunction) =
    PartitionUtils.splitMetadataAndDataPredicates(expr, partitionColumns)

  /** The object saving column stats based on the structure of table schema */
  private var statsStore = ColumnNode()

  override protected def accept(addFile: AddFile): Boolean = {
    if (metadataConjunction.isEmpty) return true

    val partitionRowRecord = new PartitionRowRecord(partitionSchema, addFile.partitionValues)
    val result = metadataConjunction.get.eval(partitionRowRecord)
    result.asInstanceOf[Boolean]
  }

  override def getInputPredicate: Optional[Expression] = Optional.of(expr)

  override def getPushedPredicate: Optional[Expression] =
    Optional.ofNullable(metadataConjunction.orNull)

  override def getResidualPredicate: Optional[Expression] =
    Optional.ofNullable(dataConjunction.orNull)

  /** Get column stats from the given path */
  def findColumnNode(path: Seq[String]): Option[ColumnNode] = {
    var curSchema: Option[ColumnNode] = Some(statsStore)
    path foreach { x =>
      if (curSchema.isEmpty || !curSchema.get.dataType.isInstanceOf[StructType]) {
        return None
      }
      curSchema = curSchema.get.nestedColumns.get(x)
    }
    curSchema
  }

  /** Parse the entire [[AddFile.stats]] string based on table schema */
  def parseColumnStats(
      tableSchema: DataType,
      addFile: AddFile): Unit = {
    // Reset the store for current AddFile
    statsStore = ColumnNode()

    JsonUtils.fromJson[Map[String, JsonNode]](addFile.stats) foreach { stats =>
      if (!stats._2.isObject) {
        // This is an table-level stats, add to root node directly
        statsStore.statsValue += (stats._1 -> stats._2.asText)
      } else {
        // Update the store in the every stats type sequence
        statsStore = parseColumnStats(
          tableSchema,
          statsNode = stats._2,
          statsType = stats._1,
          statsStore)
      }
    }
  }

  /** Parse the column stats recursively based on table schema */
  def parseColumnStats(
      tableSchema: DataType,
      statsNode: JsonNode,
      statsType: String,
      structuredStats: ColumnNode): ColumnNode = tableSchema match {
    case structNode: StructType =>
      // This is a structured column, iterate through all the nested columns and parse them
      // recursively.

      // Get the previous stored column stats for this column, if there are.
      val originStats = structuredStats
      structNode.getFields.foreach { fieldName =>
        val originSubStatsNode = statsNode.get(fieldName.getName)
        if (originSubStatsNode != null) {
          // If there is not such sub column in table schema, skip it.

          // Get the previous stored column stats for this sub column, if there are.
          val originSubStats = originStats.nestedColumns
            .getOrElseUpdate(fieldName.getName,
              ColumnNode(columnPath = originStats.columnPath :+ fieldName.getName))

          // Parse and update the nested column nodes to the current column node.
          val newSubStats = parseColumnStats(
            fieldName.getDataType,
            originSubStatsNode,
            statsType,
            originSubStats)
          originStats.nestedColumns += (fieldName.getName -> newSubStats)
        }
      }
      originStats

    case dataNode: DataType =>
      // This is a leaf column (data column), parse the corresponding stats value in string format.
      ColumnNode(
        dataNode,
        structuredStats.columnPath,
        structuredStats.nestedColumns,
        structuredStats.statsValue + (statsType -> statsNode.asText))
  }
}
