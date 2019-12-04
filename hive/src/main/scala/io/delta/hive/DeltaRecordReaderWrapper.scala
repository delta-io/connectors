package io.delta.hive

import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.Reporter
import org.apache.parquet.hadoop.ParquetInputFormat
import org.slf4j.LoggerFactory

/**
 * A record reader that reads data from the underlying Parquet reader and inserts partition values
 * which don't exist in the Parquet files.
 */
class DeltaRecordReaderWrapper(
    inputFormat: ParquetInputFormat[ArrayWritable],
    split: DeltaInputSplit,
    jobConf: JobConf,
    reporter: Reporter) extends ParquetRecordReaderWrapper(inputFormat, split, jobConf, reporter) {

  private val LOG = LoggerFactory.getLogger(classOf[DeltaRecordReaderWrapper])

  /** The indices of partition columns in schema and their values. */
  private val partitionValues: Array[(Int, Writable)] =
    split.getPartitionColumns.map { partition =>
      val oi = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableObjectInspector(TypeInfoFactory
          .getPrimitiveTypeInfo(partition.tpe))
      partition.index -> ObjectInspectorConverters.getConverter(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector, oi)
        .convert(partition.value).asInstanceOf[Writable]
    }

  override def next(key: NullWritable, value: ArrayWritable): Boolean = {
    val hasNext = super.next(key, value)
    // TODO Figure out when the parent reader resets partition columns to null so that we may come
    // out a better solution to not insert partition values for each row.
    insertPartitionValues(value)
    hasNext
  }

  /**
   * As partition columns are not in the parquet files, they will be set to `null`s every time
   * `next` is called. We should insert partition values manually for each row.
   */
  private def insertPartitionValues(value: ArrayWritable): Unit = {
    val valueArray = value.get()
    var i = 0
    val n = partitionValues.length
    // Using while loop for better performance since this method is called for each row.
    while (i < n) {
      val partition = partitionValues(i)
      valueArray(partition._1) = partition._2
      i += 1
    }
  }
}
