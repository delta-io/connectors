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

import java.io.{File, FileNotFoundException}

import io.delta.alpine.internal.exception.{DeltaErrors => DeltaErrorsAlpine}
import io.delta.alpine.internal.util.ConversionUtils
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.{DeltaLog => DeltaLogOSS}
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.spark.sql.test.SharedSparkSession

class DeltaLogSuite extends QueryTest
  with SharedSparkSession
  with ConversionUtils {

  private val testOp = ManualUpdate

  test("checkpoint") {
    withTempDir { dir =>
      val log1 = DeltaLogOSS.forTable(spark, new Path(dir.getCanonicalPath))

      (1 to 15).foreach { i =>
        val txn = log1.startTransaction()
        val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
        val delete: Seq[Action] = if (i > 1) {
          RemoveFile(i - 1 toString, Some(System.currentTimeMillis()), true) :: Nil
        } else {
          Nil
        }
        txn.commit(delete ++ file, testOp)
      }

      DeltaLogOSS.clearCache()
      val log2 = DeltaLogOSS.forTable(spark, new Path(dir.getCanonicalPath))
      assert(log2.snapshot.version == log1.snapshot.version)
      assert(log2.snapshot.allFiles.count == 1)

      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLogImpl.forTable(hadoopConf, new Path(dir.getCanonicalPath))
      assert(alpineLog.snapshot.version == log1.snapshot.version)
      assert(alpineLog.snapshot.allFilesScala.size == 1)
      assert(alpineLog.snapshot.numOfFiles == 1)
//      assert(alpineLog.snapshot.numOfRemoves == 14)
    }
  }

  test("snapshot") {
    import testImplicits._

    withTempDir { dir =>
      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLogImpl.forTable(hadoopConf, new Path(dir.getCanonicalPath))
      val log = DeltaLogOSS.forTable(spark, new Path(dir.getCanonicalPath))

      def writeData(data: Seq[(Int, String)], mode: String): Unit = {
        data.toDS
          .toDF("col1", "col2")
          .write
          .mode(mode)
          .format("delta")
          .save(dir.getCanonicalPath)
      }

      def getDirSnapshotFiles: Array[File] = {
        dir.listFiles().filter(_.isFile).filter(_.getName.endsWith("snappy.parquet"))
      }

      def updateLogs(): Unit = {
        log.update()
        alpineLog.update()
      }

      def compareSnapshots(correctSnapshotFiles: Array[File], correctSnapshotVersion: Int): Unit = {
        val logAllFiles = log.snapshot.allFiles.collect()
        val alpineLogAllFiles = alpineLog.snapshot.allFilesScala

        // Alpine snapshot version is correct and is same as DeltaLogImpl snapshot version
        assert(alpineLog.snapshot.version == correctSnapshotVersion &&
          alpineLog.snapshot.version == log.snapshot.version)

        // Alpine snapshot files matches correctSnapshotFiles and matches DeltaLogImpl all files
        assert(alpineLogAllFiles.size == correctSnapshotFiles.length
          && alpineLogAllFiles.size == logAllFiles.length
          && alpineLogAllFiles.forall { f =>
          correctSnapshotFiles.exists(_.getName == f.path) && logAllFiles.exists(_.path == f.path)
        })

        assert(convertMetaData(alpineLog.snapshot.metadataScala) == log.snapshot.metadata)
        assert(convertProtocol(alpineLog.snapshot.protocolScala) == log.snapshot.protocol)
      }

      // Step 1: append data
      val data0 = (0 until 10).map(x => (x, s"data-0-$x"))
      writeData(data0, "append")
      val snapshotFiles0 = getDirSnapshotFiles // includes data0 snapshots
      assert(alpineLog.snapshot.version == -1)
      updateLogs()
      compareSnapshots(snapshotFiles0, 0)

      // Step 2: append data again
      val data1 = (0 until 10).map(x => (x, s"data-1-$x"))
      writeData(data1, "append")
      val snapshotFiles1 = getDirSnapshotFiles // includes data0 & data1 snapshots
      updateLogs()
      compareSnapshots(snapshotFiles1, 1)

      // Step 3: overwrite data
      val data2 = (0 until 10).map(x => (x, s"data-2-$x"))
      writeData(data2, "overwrite")
      // we have overwritten snapshots for data0,1; so only data2 snapshots should remain
      val snapshotFiles2 = getDirSnapshotFiles
        .filterNot(f => snapshotFiles1.exists(_.getName == f.getName))
      updateLogs()
      compareSnapshots(snapshotFiles2, 2)

      // Step 4: append data again
      val data3 = (0 until 20).map(x => (x, s"data-3-$x"))
      writeData(data3, "append")
      // we have overwritten snapshots for data0,1; so only data2,3 snapshots should remain
      val snapshotFiles3 = getDirSnapshotFiles
        .filterNot(f => snapshotFiles1.exists(_.getName == f.getName))
      updateLogs()
      compareSnapshots(snapshotFiles3, 3)

      // Step 5: delete data-2-* data
      DeltaTable.forPath(spark, dir.getCanonicalPath).delete("col2 like 'data-2-%'")
      val snapshotFiles4 = getDirSnapshotFiles
        .filterNot(f => snapshotFiles1.exists(_.getName == f.getName))
        .filterNot(f => snapshotFiles2.exists(_.getName == f.getName))
      updateLogs()
      compareSnapshots(snapshotFiles4, 4)

      // Step 7: compact
      val preCompactionFiles = getDirSnapshotFiles
      spark.read
        .format("delta")
        .load(dir.getCanonicalPath)
        .repartition(2)
        .write
        .option("dataChange", "false")
        .format("delta")
        .mode("overwrite")
        .save(dir.getCanonicalPath)
      val snapshotFiles5 = getDirSnapshotFiles
        .filterNot(f => preCompactionFiles.exists(_.getName == f.getName))
      updateLogs()
      compareSnapshots(snapshotFiles5, 5)

      // Step 8: vacuum
      withSQLConf(DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
        DeltaTable.forPath(spark, dir.getCanonicalPath).vacuum(0.0)
        updateLogs()
        compareSnapshots(snapshotFiles5, 5) // snapshots & version same as before
      }
    }
  }

  test("SC-8078: update deleted directory") {
    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath)
      val log = DeltaLogOSS.forTable(spark, path)

      // Commit data so the in-memory state isn't consistent with an empty log.
      val txn = log.startTransaction()
      val files = (1 to 10).map(f => AddFile(f.toString, Map.empty, 1, 1, true))
      txn.commit(files, testOp)
      log.checkpoint()

      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLogImpl.forTable(hadoopConf, new Path(dir.getCanonicalPath))

      val fs = path.getFileSystem(hadoopConf)
      fs.delete(path, true)

      val ex = intercept[FileNotFoundException] {
        log.update()
      }
      assert(ex.getMessage().contains("No delta log found"))
      assert(alpineLog.update().version == -1)
    }
  }

  test("update shouldn't pick up delta files earlier than checkpoint") {
    withTempDir { dir =>
      val log1 = DeltaLogOSS.forTable(spark, new Path(dir.getCanonicalPath))

      def addDeleteCommit(range: Range.Inclusive): Unit = {
        range.foreach { i =>
          val txn = log1.startTransaction()
          val file = AddFile(i.toString, Map.empty, 1, 1, true) :: Nil
          val delete: Seq[Action] = if (i > 1) {
            RemoveFile(i - 1 toString, Some(System.currentTimeMillis()), true) :: Nil
          } else {
            Nil
          }
          txn.commit(delete ++ file, testOp)
        }
      }

      addDeleteCommit(1 to 5)

      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLogImpl.forTable(hadoopConf, new Path(dir.getCanonicalPath))

      addDeleteCommit(6 to 15)

      // Since alpineLog is a separate instance, it shouldn't be updated to version 15
      assert(alpineLog.snapshot.version == 4)
      val alpineUpdateLog2 = alpineLog.update()
      assert(alpineUpdateLog2.version == log1.snapshot.version, "Did not update to correct version")

      val deltas = alpineLog.snapshot.logSegment.deltas
      assert(deltas.length === 4, "Expected 4 files starting at version 11 to 14")
      val versions = deltas.map(f => FileNames.deltaVersion(f.getPath)).sorted
      assert(versions === Seq[Long](11, 12, 13, 14), "Received the wrong files for update")
    }
  }

// TODO test("handle corrupted '_last_checkpoint' file") { }

  test("paths should be canonicalized - normal and special characters") {
    Seq("file:", "file://").foreach { scheme =>
      Seq("/some/unqualified/absolute/path",
        new Path("/some/unqualified/with space/p@#h").toUri.toString).foreach { path =>
        withTempDir { dir =>
          val log = DeltaLogOSS.forTable(spark, dir)
          assert(new File(log.logPath.toUri).mkdirs())

          JavaUtils.deleteRecursively(dir)

          val hadoopConf = spark.sessionState.newHadoopConf()
          val alpineLog = DeltaLogImpl.forTable(hadoopConf, new Path(dir.getCanonicalPath))
          assert(new File(alpineLog.logPath.toUri).mkdirs())

          val add = AddFile(path, Map.empty, 100L, 10L, dataChange = true)
          val rm = RemoveFile(s"$scheme$path", Some(200L), dataChange = false)

          log.store.write(
            FileNames.deltaFile(log.logPath, 0L),
            Iterator(Protocol(), Metadata(), add).map(a => JsonUtils.toJson(a.wrap)))
          log.store.write(
            FileNames.deltaFile(log.logPath, 1L),
            Iterator(JsonUtils.toJson(rm.wrap)))

          assert(log.update().version === 1)
          assert(log.snapshot.numOfFiles === 0)

          assert(alpineLog.update().version === 1)
          assert(alpineLog.snapshot.numOfFiles === 0)
        }
      }
    }
  }

//  test("do not relativize paths in RemoveFiles") {
//    withTempDir { dir =>
//      val log = DeltaLogOSS.forTable(spark, dir)
//      assert(new File(log.logPath.toUri).mkdirs())
//
//      JavaUtils.deleteRecursively(dir)
//
//      val hadoopConf = spark.sessionState.newHadoopConf()
//      val alpineLog = DeltaLogImpl.forTable(hadoopConf, new Path(dir.getCanonicalPath))
//      assert(new File(alpineLog.logPath.toUri).mkdirs())
//
//      val path = new File(dir, "a/b/c").getCanonicalPath
//      val rm = RemoveFile(path, Some(System.currentTimeMillis()), dataChange = true)
//      log.startTransaction().commit(Seq(rm), testOp)
//
//      val committedRemove = log.update(stalenessAcceptable = false).tombstones.collect()
//      assert(committedRemove.head.path === s"file://$path")
//
//      val alpineCommittedRemove = alpineLog.update().tombstones
//      assert(alpineCommittedRemove.head.path === s"file://$path")
//    }
//  }

  test("delete and re-add the same file in different transactions") {
    withTempDir { dir =>
      val hadoopConf = spark.sessionState.newHadoopConf()
      val alpineLog = DeltaLogImpl.forTable(hadoopConf, new Path(dir.getCanonicalPath))

      val log = DeltaLogOSS.forTable(spark, dir)
      assert(new File(log.logPath.toUri).mkdirs())

      val add1 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(add1 :: Nil, testOp)

      val rm = add1.remove
      log.startTransaction().commit(rm :: Nil, testOp)

      val add2 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(add2 :: Nil, testOp)

      // Add a new transaction to replay logs using the previous snapshot. If it contained
      // AddFile("foo") and RemoveFile("foo"), "foo" would get removed and fail this test.
      val otherAdd = AddFile("bar", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(otherAdd :: Nil, testOp)

      assert(Some(convertAddFile(alpineLog.update().allFilesScala.find(_.path == "foo").get))
        // `dataChange` is set to `false` after replaying logs.
        === Some(add2.copy(dataChange = false)))
    }
  }

  test("error - versions not contiguous") {
    withTempDir { dir =>
      val log = DeltaLogOSS.forTable(spark, dir)
      assert(new File(log.logPath.toUri).mkdirs())

      val add1 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(add1 :: Nil, testOp)

      val add2 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(add2 :: Nil, testOp)

      val add3 = AddFile("foo", Map.empty, 1L, System.currentTimeMillis(), dataChange = true)
      log.startTransaction().commit(add3 :: Nil, testOp)

      new File(new Path(log.logPath, "00000000000000000001.json").toUri).delete()

      DeltaLogOSS.clearCache()
      val ex = intercept[IllegalStateException] {
        val hadoopConf = spark.sessionState.newHadoopConf()
        DeltaLogImpl.forTable(hadoopConf, new Path(dir.getCanonicalPath))
      }

      assert(ex.getMessage ===
        DeltaErrorsAlpine.deltaVersionsNotContiguousException(Vector(0, 2)).getMessage)
    }
  }
}
