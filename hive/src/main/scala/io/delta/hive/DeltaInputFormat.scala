package io.delta.hive

import java.net.URI

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.sql.delta.DeltaHelper
import org.slf4j.LoggerFactory

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
