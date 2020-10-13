package io.delta.alpine.internal

import java.io.File

import scala.concurrent.duration._
import scala.language.implicitConversions
import collection.JavaConverters._

import io.delta.alpine.{DeltaLog, Snapshot}
import io.delta.alpine.internal.exception.DeltaErrors
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.delta.{DeltaLog => DeltaLogOSS}
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}

class DeltaTimeTravelSuite extends QueryTest
  with SharedSparkSession {
  //  import testImplicits._

  private implicit def durationToLong(duration: FiniteDuration): Long = {
    duration.toMillis
  }

  /** Generate commits with the given timestamp in millis. */
  private def generateCommits(location: String, commits: Long*): Unit = {
    val deltaLog = DeltaLogOSS.forTable(spark, location)
    var startVersion = deltaLog.snapshot.version + 1
    commits.foreach { ts =>
      val rangeStart = startVersion * 10
      val rangeEnd = rangeStart + 10
      spark.range(rangeStart, rangeEnd).write.format("delta").mode("append").save(location)
      val file = new File(FileNames.deltaFile(deltaLog.logPath, startVersion).toUri)
      file.setLastModified(ts)
      startVersion += 1
    }
  }

  private def identifierWithVersion(identifier: String, v: Long): String = {
    s"$identifier@v$v"
  }

  test("versionAsOf") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)

      def readRowsFromSnapshotFiles(snapshot: Snapshot): Set[Row] = {
        snapshot.getAllFiles.asScala.map(_.getPath).flatMap { path =>
          spark.read.format("parquet").load(s"$tblLoc/$path").collect()
        }.toSet
      }

      def assertCorrectSnapshot(version: Long, expectedNumRows: Long): Unit = {
        val df = spark.read.format("delta").load(identifierWithVersion(tblLoc, version))
        val snapshot = alpineLog.getSnapshotForVersionAsOf(version)
        val rowsFromSnapshot = readRowsFromSnapshotFiles(snapshot)
        checkAnswer(df.groupBy().count(), Row(expectedNumRows))
        assert(rowsFromSnapshot.size == expectedNumRows)
        assert(df.collect().toSet == rowsFromSnapshot)
      }

      val start = 1540415658000L
      generateCommits(tblLoc, start, start + 20.minutes, start + 40.minutes)

      assertCorrectSnapshot(0, 10)
      assertCorrectSnapshot(1, 20)
      assertCorrectSnapshot(2, 30)

      // TODO: use more specfic exception? e.g. AnalysisException
      val e = intercept[Exception] {
        alpineLog.getSnapshotForVersionAsOf(3)
      }
      assert(e.getMessage == DeltaErrors.versionNotExistException(3, 0, 2).getMessage)

      // TODO: reproducible? Delete the log files?
    }
  }
}