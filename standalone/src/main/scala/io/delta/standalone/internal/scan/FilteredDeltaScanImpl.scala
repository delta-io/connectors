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

import io.delta.standalone.expressions.{Expression, Not, Or}
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
  val columnStatsFilter: Option[Expression] = dataConjunction match {
    case Some(e: Expression) =>
      // Transform the query predicate based on filter, see `DataSkippingUtils.constructDataFilters`
      // If this passed, it means it is possible that the records in this file contains value can
      // pass the query predicate.
      val columnStatsPredicate = DataSkippingUtils.constructDataFilters(tableSchema, e)

      columnStatsPredicate match {
        case Some(_: ColumnStatsPredicate) =>

          // Append additional expression to verify the stats appears in `columnStatsPredicate` is
          // valid. If this is failed, it means that the stats is not usable, we should ignore the
          // column stats filter and accept this.
          val verificationPredicate = DataSkippingUtils.verifyStatsForFilter(
            columnStatsPredicate.get.referencedStats)

          // We will accept the file either `columnStatsPredicate` is passed, or the
          // `verifyStatsForFilter` is failed.
          Some(new Or(columnStatsPredicate.get.expr, new Not(verificationPredicate)))
        case _ => None
      }
    case _ => None
  }

  override protected def accept(addFile: AddFile): Boolean = {
    if (metadataConjunction.isEmpty) return true

    val partitionRowRecord = new PartitionRowRecord(partitionSchema, addFile.partitionValues)
    val partitionFilterResult = metadataConjunction
      .get
      .eval(partitionRowRecord)
      .asInstanceOf[Boolean]

    if (partitionFilterResult && columnStatsFilter.isDefined) {
      // Evaluate the column stats filter when partition filter passed and column stats filter is
      // not empty. This happens once per file.

      // Parse stats in AddFile, see `DataSkippingUtils.parseColumnStats`
      val (fileLevelStats, columnLevelStats) =
        DataSkippingUtils.parseColumnStats(tableSchema, addFile.stats)

      // Instantiate the evaluate function based on the parsed column stats
      val dataRowRecord = new ColumnStatsRowRecord(tableSchema, fileLevelStats, columnLevelStats)
      val columnStatsFilterResult = columnStatsFilter.get.eval(dataRowRecord)

      columnStatsFilterResult.asInstanceOf[Boolean]
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
