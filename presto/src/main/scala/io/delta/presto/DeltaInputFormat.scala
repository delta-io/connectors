/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.presto

import io.delta.hive
import io.delta.hive.{DeltaInputSplit, DeltaRecordReaderWrapper, UseFileSplitsFromInputFormat, UseRecordReaderFromInputFormat}
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport
import org.apache.hadoop.io.{ArrayWritable, NullWritable}
import org.apache.hadoop.mapred._
import org.apache.parquet.hadoop.ParquetInputFormat
import org.slf4j.LoggerFactory

@UseRecordReaderFromInputFormat
@UseFileSplitsFromInputFormat
class DeltaInputFormat(realInput: ParquetInputFormat[ArrayWritable])
  extends hive.DeltaInputFormat(realInput) {

  private val LOG = LoggerFactory.getLogger(classOf[DeltaInputFormat])

  def this() {
    this(new ParquetInputFormat[ArrayWritable](classOf[DataWritableReadSupport]))
  }

  override def getRecordReader(
      split: InputSplit,
      job: JobConf,
      reporter: Reporter): RecordReader[NullWritable, ArrayWritable] = {
    split match {
      case deltaSplit: DeltaInputSplit =>
        if (Utilities.getIsVectorized(job)) {
          throw new UnsupportedOperationException(
            "Reading Delta tables using Parquet's VectorizedReader is not supported")
        } else {
          new DeltaRecordReaderWrapper(this.realInput, deltaSplit, job, reporter)
        }
      case _ =>
        throw new IllegalArgumentException("Expected DeltaInputSplit but it was: " + split)
    }
  }

  override def checkHiveConf(job: JobConf): Unit = LOG.debug("Skip checkHiveConf in PrestoDB.")

}
