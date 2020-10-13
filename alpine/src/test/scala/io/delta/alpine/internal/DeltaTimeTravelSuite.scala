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

package io.delta.alpine.internal

import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import scala.concurrent.duration._
import scala.language.implicitConversions
import collection.JavaConverters._

import io.delta.alpine.{DeltaLog, Snapshot}
import io.delta.alpine.internal.exception.DeltaErrors

import org.apache.spark.sql.delta.{DeltaLog => DeltaLogOSS}
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{QueryTest, Row}

class DeltaTimeTravelSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  private val timeFormatter = new SimpleDateFormat("yyyyMMddHHmmssSSS")

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

  private def identifierWithTimestamp(identifier: String, ts: Long): String = {
    s"$identifier@${timeFormatter.format(new Date(ts))}"
  }

  private def readRowsFromSnapshotFiles(snapshot: Snapshot, tblLoc: String): Set[Row] = {
    snapshot.getAllFiles.asScala.map(_.getPath).flatMap { path =>
      spark.read.format("parquet").load(s"$tblLoc/$path").collect()
    }.toSet
  }

  private def assertCorrectSnapshotUsingVersion(
      alpineLog: DeltaLog,
      tblLoc: String,
      version: Long,
      expectedNumRows: Long): Unit = {
    val df = spark.read.format("delta").option("versionAsOf", version).load(tblLoc)
    val snapshot = alpineLog.getSnapshotForVersionAsOf(version)
    val rowsFromSnapshot = readRowsFromSnapshotFiles(snapshot, tblLoc)
    checkAnswer(df.groupBy().count(), Row(expectedNumRows))
    assert(rowsFromSnapshot.size == expectedNumRows)
    assert(df.collect().toSet == rowsFromSnapshot)
  }

  private def assertCorrectSnapshotUsingTimestamp(
      alpineLog: DeltaLog,
      tblLoc: String,
      timestamp: Long,
      expectedNumRows: Long): Unit = {
    val df = spark.read.format("delta").load(identifierWithTimestamp(tblLoc, timestamp))
    val snapshot = alpineLog.getSnapshotForTimestampAsOf(timestamp)
    val rowsFromSnapshot = readRowsFromSnapshotFiles(snapshot, tblLoc)
    checkAnswer(df.groupBy().count(), Row(expectedNumRows))
    assert(rowsFromSnapshot.size == expectedNumRows)
    assert(df.collect().toSet == rowsFromSnapshot)
  }

  test("versionAsOf") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val start = 1540415658000L
      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)

      generateCommits(tblLoc, start, start + 20.minutes, start + 40.minutes)

      // Correct cases
      assertCorrectSnapshotUsingVersion(alpineLog, tblLoc, 0, 10)
      assertCorrectSnapshotUsingVersion(alpineLog, tblLoc, 1, 20)
      assertCorrectSnapshotUsingVersion(alpineLog, tblLoc, 2, 30)

      // Error case - version after latest commit version
      val e1 = intercept[DeltaErrors.DeltaTimeTravelException] {
        alpineLog.getSnapshotForVersionAsOf(3)
      }
      assert(e1.getMessage == DeltaErrors.versionNotExistException(3, 0, 2).getMessage)

      // Error case - version before earliest commit
      val e2 = intercept[DeltaErrors.DeltaTimeTravelException] {
        alpineLog.getSnapshotForVersionAsOf(-1)
      }
      assert(e2.getMessage == DeltaErrors.versionNotExistException(-1, 0, 2).getMessage)

      // Error case - not reproducible
      val logPath = alpineLog.getLogPath
      new File(FileNames.deltaFile(logPath, 0).toUri).delete()
      val e3 = intercept[DeltaErrors.DeltaTimeTravelException] {
        alpineLog.getSnapshotForVersionAsOf(-1)
      }
      assert(e3.getMessage == DeltaErrors.noReproducibleHistoryFound(logPath).getMessage)
    }
  }

  test("timestampAsOf with timestamp in between commits - should use commit before timestamp") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val start = 1540415658000L
      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)

      generateCommits(tblLoc, start, start + 20.minutes, start + 40.minutes)

      assertCorrectSnapshotUsingTimestamp(alpineLog, tblLoc, start + 10.minutes, 10)
      assertCorrectSnapshotUsingTimestamp(alpineLog, tblLoc, start + 30.minutes, 20)
    }
  }

  test("timestampAsOf with timestamp after last commit should fail") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val start = 1540415658000L
      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)

      generateCommits(tblLoc, start, start + 20.minutes, start + 40.minutes)

      val e = intercept[DeltaErrors.DeltaTimeTravelException] {
        alpineLog.getSnapshotForTimestampAsOf(start + 50.minutes)
      }

      val latestTimestamp = new Timestamp(start + 40.minutes)
      val usrTimestamp = new Timestamp(start + 50.minutes)
      assert(e.getMessage ==
        DeltaErrors.timestampLaterThanTableLastCommit(usrTimestamp, latestTimestamp).getMessage)
    }
  }

  test("timestampAsOf with timestamp before first commit should fail") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val start = 1540415658000L
      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)

      generateCommits(tblLoc, start, start + 20.minutes, start + 40.minutes)

      val e = intercept[DeltaErrors.DeltaTimeTravelException] {
        alpineLog.getSnapshotForTimestampAsOf(start - 10.minutes)
      }

      val latestTimestamp = new Timestamp(start)
      val usrTimestamp = new Timestamp(start - 10.minutes)
      assert(e.getMessage ==
        DeltaErrors.timestampEarlierThanTableFirstCommit(usrTimestamp, latestTimestamp).getMessage)
    }
  }

  test("timestampAsOf with timestamp on exact commit timestamp") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val start = 1540415658000L
      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)

      generateCommits(tblLoc, start, start + 20.minutes, start + 40.minutes)

      assertCorrectSnapshotUsingTimestamp(alpineLog, tblLoc, start, 10)
      assertCorrectSnapshotUsingTimestamp(alpineLog, tblLoc, start + 20.minutes, 20)
      assertCorrectSnapshotUsingTimestamp(alpineLog, tblLoc, start + 40.minutes, 30)
    }
  }

  test("time travel with schema changes - should instantiate old schema") {
    withTempDir { dir =>
      val tblLoc = dir.getCanonicalPath
      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLog.forTable(hadoopConf, tblLoc)

      spark.range(10).write.format("delta").mode("append").save(tblLoc)
      spark.range(10, 20).withColumn("part", 'id)
        .write.format("delta").mode("append").option("mergeSchema", true).save(tblLoc)

      assertCorrectSnapshotUsingVersion(alpineLog, tblLoc, 0, 10)
    }
  }
}
