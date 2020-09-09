package main.scala.storage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

class HDFSLogStore(hadoopConf: Configuration) extends LogStore {
  override def read(path: Path): Seq[String] = null

  override def write(path: Path, actions: Iterator[String], overwrite: Boolean): Unit = {

  }

  override def listFrom(path: Path): Iterator[FileStatus] = null

  override def invalidateCache(): Unit = {

  }
}
