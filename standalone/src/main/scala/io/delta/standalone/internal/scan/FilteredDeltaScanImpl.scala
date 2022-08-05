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

import org.apache.hadoop.conf.Configuration

import io.delta.standalone.expressions.Expression
import io.delta.standalone.types.StructType

import io.delta.standalone.internal.actions.{AddFile, MemoryOptimizedLogReplay}
import io.delta.standalone.internal.data.{ColumnStatsRowRecord, PartitionRowRecord}
import io.delta.standalone.internal.sources.StandaloneHadoopConf
import io.delta.standalone.internal.util.{DataSkippingUtils, PartitionUtils}

/**
 * An implementation of [[io.delta.standalone.DeltaScan]] that filters files and only returns
 * those that match the [[getPushedPredicate]] and [[getResidualPredicate]].
 *
 * If all the predicates are empty, then all files are returned.
 */
final private[internal] class FilteredDeltaScanImpl(
    replay: MemoryOptimizedLogReplay,
    expr: Expression,
    partitionSchema: StructType,
    dataSchema: StructType,
    hadoopConf: Configuration) extends DeltaScanImpl(replay) {

  private val partitionColumns = partitionSchema.getFieldNames.toSeq

  private val (metadataConjunction, dataConjunction) =
    PartitionUtils.splitMetadataAndDataPredicates(expr, partitionColumns)

  /** Feature flag of stats based file pruning. */
  private val statsSkippingEnabled = hadoopConf
    .getBoolean(StandaloneHadoopConf.STATS_SKIPPING_KEY, true)

  /**
   * If column stats filter is evaluated as true, it means some row in this file may meet the query
   * condition, thus we need to return this file.
   *
   * If this is evaluated as false, then no row in this file meets the query condition and we will
   * skip this file.
   *
   * Sometimes this is evaluated as `null` because stats are invalid, then we accept and return this
   * file to client.
   */
  private val columnStatsFilter: Option[Expression] =
    DataSkippingUtils.constructDataFilters(dataSchema, dataConjunction)

  private val statsSchema = DataSkippingUtils.buildStatsSchema(dataSchema)

  override protected def accept(addFile: AddFile): Boolean = {
    // Evaluate the partition filter.
    val partitionFilterResult = if (metadataConjunction.isDefined) {
      val partitionRowRecord = new PartitionRowRecord(partitionSchema, addFile.partitionValues)
      metadataConjunction.get.eval(partitionRowRecord) match {
        case null => true
        case result => result.asInstanceOf[Boolean]
      }
    } else {
      true
    }

    if (statsSkippingEnabled && partitionFilterResult && columnStatsFilter.isDefined) {
      // Get stats value from each AddFile.
      val (fileStats, columnStats) = try {
        DataSkippingUtils.parseColumnStats(dataSchema, addFile.stats)
      } catch {
        // If the stats parsing process failed, accept this file.
        case NonFatal(_) => return true
      }

      if (fileStats.isEmpty && columnStats.isEmpty) {
        // If we don't have any stats, skip evaluation and accept this file.
        return true
      }

      // Instantiate the evaluate function based on the parsed column stats.
      val columnStatsRecord = new ColumnStatsRowRecord(statsSchema, fileStats, columnStats)

      val columnStatsFilterResult = columnStatsFilter.get.eval(columnStatsRecord)

      // During the evaluation, all the stats values will be checked by
      // `ColumnStatsRowRecord.isNullAt` before get the stats value. If any stats in column stats
      // filter is missing, the evaluation result will be null. In that case the file will be
      // accepted.
      //
      // Since `ColumnStatsRowRecord.isNullAt` will return null as evaluation result when stats
      // missing, it will make `IsNull` return true wrongly when `IsNull` is the parent expression
      // of some expression used `isNullAt`.
      // For example, `IsNull(MIN.col1)` will be true if MIN.col1 is missing. Meanwhile, `col1` can
      // be all non-null value.
      // We avoid this problem by not using `IsNull` expression in any column stats filter.
      columnStatsFilterResult match {
        case null => true
        case result => result.asInstanceOf[Boolean]
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
