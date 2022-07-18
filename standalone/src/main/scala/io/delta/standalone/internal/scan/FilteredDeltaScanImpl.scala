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

import scala.util.control.NonFatal

import io.delta.standalone.expressions.Expression
import io.delta.standalone.types.StructType

import io.delta.standalone.internal.actions.{AddFile, MemoryOptimizedLogReplay}
import io.delta.standalone.internal.data.{ColumnStatsRowRecord, PartitionRowRecord}
import io.delta.standalone.internal.util.{ColumnStatsPredicate, DataSkippingUtils, PartitionUtils}

/**
 * An implementation of [[io.delta.standalone.DeltaScan]] that filters files and only returns
 * those that match the [[getPushedPredicate]].
 *
 * If the pushed predicate is empty, then all files are returned.
 */
final private[internal] class FilteredDeltaScanImpl(
    replay: MemoryOptimizedLogReplay,
    expr: Expression,
    partitionSchema: StructType,
    tableSchema: StructType) extends DeltaScanImpl(replay) {

  private val partitionColumns = partitionSchema.getFieldNames.toSeq

  private val (metadataConjunction, dataConjunction) =
    PartitionUtils.splitMetadataAndDataPredicates(expr, partitionColumns)

  // The column stats filter, generated once per query.
  val columnStatsFilter: Option[ColumnStatsPredicate] = dataConjunction match {
    case Some(e: Expression) =>
      // Transform the query predicate based on filter, see `DataSkippingUtils.constructDataFilters`
      // If this passed, it means it is possible that the records in this file contains value can
      // pass the query predicate.
      DataSkippingUtils.constructDataFilters(tableSchema, e)
    case _ => None
  }

  override protected def accept(addFile: AddFile): Boolean = {
    // Evaluate the partition filter
    val partitionFilterResult = if (metadataConjunction.isDefined) {
      val partitionRowRecord = new PartitionRowRecord(partitionSchema, addFile.partitionValues)
      metadataConjunction.get.eval(partitionRowRecord) match {
        case null => true
        case evalOutput => evalOutput.asInstanceOf[Boolean]
      }
    } else {
      true
    }

    if (partitionFilterResult && columnStatsFilter.isDefined) {
      // Evaluate the column stats filter when partition filter passed and column stats filter is
      // not empty. This happens once per file.

      // Parse stats in AddFile, see `DataSkippingUtils.parseColumnStats`
      val (fileStats, columnStats) = try {
        DataSkippingUtils.parseColumnStats(tableSchema, addFile.stats)
      } catch {
        // If the stats parsing process failed, not skipping this file.
        case NonFatal(_) => return true
      }

      if (fileStats.isEmpty || columnStats.isEmpty) {
        // If we don't have any stats, not skipping this file.
        return true
      }

      // Instantiate the evaluate function based on the parsed column stats
      val columnStatsRecord = new ColumnStatsRowRecord(tableSchema, fileStats, columnStats)

      // Verify whether the stats column appears in `columnStatsPredicate` is valid. If we find any
      // stats column in the filter that not appears in row record, then the filter is not
      // usable. We should not skipping this file.
      columnStatsFilter.get.referencedStats.foreach { statsCol =>
        // now only support LongType.
        if (columnStatsRecord.isNullAt(statsCol.name)) {
          // If any stats in the column stats filter is null or missing, not skipping this file.
          return true
        }
      }
      // Evaluate the filter, this guarantees that all stats can be found in row record.
      val columnStatsFilterResult = columnStatsFilter.get.expr.eval(columnStatsRecord)

      columnStatsFilterResult match {
        // If the expression is not evaluated correctly, not skipping this file.
        case null => true

        case _ => columnStatsFilterResult.asInstanceOf[Boolean]
      }
    } else {
      partitionFilterResult
    }
  }

  override def getInputPredicate: Optional[Expression] = Optional.of(expr)

  override def getPushedPredicate: Optional[Expression] =
    Optional.ofNullable(metadataConjunction.orNull)

  override def getResidualPredicate: Optional[Expression] =
    Optional.ofNullable(dataConjunction.orNull)

}
