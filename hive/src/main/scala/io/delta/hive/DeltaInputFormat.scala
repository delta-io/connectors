package io.delta.hive

import java.net.URI

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport
import org.apache.hadoop.io.{ArrayWritable, NullWritable, Writable}
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.sql.delta.DeltaHelper
import org.slf4j.LoggerFactory

/**
 * A special [[InputFormat]] to wrap [[ParquetInputFormat]] to read a Delta table.
 *
 * The underlying files in a Delta table are in Parquet format. However, we cannot use the existing
 * [[ParquetInputFormat]] to read them directly because they only store data for data columns.
 * The values of partition columns are in Delta's metadata. Hence, we need to read them from Delta's
 * metadata and re-assemble rows to include partition values and data values from the raw Parquet
 * files.
 *
 * Note: We cannot use the file name to infer partition values because Delta Transaction Log
 * Protocol requires "Actual partition values for a file must be read from the transaction log".
 *
 * In the current implementation, when listing files, we also read the partition values and put them
 * into an `Array[PartitionColumnInfo]`. Then create a temp `Map` to store the mapping from the file
 * path to `PartitionColumnInfo`s. When creating an [[InputSplit]], we will create a special
 * [[FileSplit]] called [[DeltaInputSplit]] to carry over `PartitionColumnInfo`s.
 *
 * For each reader created from a [[DeltaInputSplit]], we can get all partition column types, the
 * locations of a partition column in the schema, and their string values. The reader can build
 * [[Writable]] for all partition values, and insert them to the raw row returned by
 * [[org.apache.parquet.hadoop.ParquetRecordReader]].
 */
class DeltaInputFormat(realInput: ParquetInputFormat[ArrayWritable]) extends FileInputFormat[NullWritable, ArrayWritable] {

  private val LOG = LoggerFactory.getLogger(classOf[DeltaInputFormat])

  /**
   * A temp [[Map]] to store the path uri and its partition information. We build this map in
   * `listStatus` and `makeSplit` will use it to retrieve the partition information for each split.
   * */
  private var fileToPartition: Map[URI, Array[PartitionColumnInfo]] = Map.empty

  def this() {
    this(new ParquetInputFormat[ArrayWritable](classOf[DataWritableReadSupport]))
  }

  override def getRecordReader(split: InputSplit, job: JobConf, reporter: Reporter): RecordReader[NullWritable, ArrayWritable] = {
    split match {
      case deltaSplit: DeltaInputSplit =>
        if (Utilities.getUseVectorizedInputFileFormat(job)) {
          throw new UnsupportedOperationException(
            "Reading Delta tables using Parquet's VectorizedReader is not supported")
        } else {
          new DeltaRecordReaderWrapper(this.realInput, deltaSplit, job, reporter)
        }
      case _ =>
        throw new IllegalArgumentException("Expected DeltaInputSplit but it was: " + split)
    }
  }

  override def listStatus(job: JobConf): Array[FileStatus] = {
    val deltaRootPath = new Path(job.get(DeltaStorageHandler.DELTA_TABLE_PATH))
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), Array(deltaRootPath), job)
    val (files, partitions) = DeltaHelper.listDeltaFiles(deltaRootPath, job)
    fileToPartition = partitions.filter(_._2.nonEmpty)
    files
  }

  override def makeSplit(
      file: Path,
      start: Long,
      length: Long,
      hosts: Array[String]): FileSplit = {
    new DeltaInputSplit(file, start, length, hosts, fileToPartition.getOrElse(file.toUri, Array.empty))
  }

  override def makeSplit(
      file: Path,
      start: Long,
      length: Long,
      hosts: Array[String],
      inMemoryHosts: Array[String]): FileSplit = {
    new DeltaInputSplit(file, start, length, hosts, inMemoryHosts, fileToPartition.getOrElse(file.toUri, Array.empty))
  }

  override def getSplits(job: JobConf, numSplits: Int): Array[InputSplit] = {
    val splits = super.getSplits(job, numSplits)
    // Reset the temp [[Map]] to release the memory
    fileToPartition = Map.empty
    splits
  }
}
