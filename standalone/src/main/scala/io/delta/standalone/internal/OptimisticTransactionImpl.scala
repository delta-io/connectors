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

package io.delta.standalone.internal

import java.util.ConcurrentModificationException

import scala.collection.JavaConverters._

import io.delta.standalone.{CommitResult, Operation, OptimisticTransaction}
import io.delta.standalone.actions.{Action => ActionJ, AddFile => AddFileJ, Metadata => MetadataJ}
import io.delta.standalone.expressions.Expression
import io.delta.standalone.internal.actions.{Action, AddFile, CommitInfo, Metadata, Protocol, RemoveFile}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.sources.StandaloneHadoopConf
import io.delta.standalone.internal.util.{ConversionUtils, FileNames, SchemaMergingUtils, SchemaUtils}

private[internal] class OptimisticTransactionImpl(
    deltaLog: DeltaLogImpl,
    snapshot: SnapshotImpl) extends OptimisticTransaction {

  /** Tracks if this transaction has already committed. */
  private var committed = false

  /** Stores the updated metadata (if any) that will result from this txn. */
  private var newMetadata: Option[Metadata] = None

  /** Stores the updated protocol (if any) that will result from this txn. */
  private var newProtocol: Option[Protocol] = None

  // Whether this transaction is creating a new table.
  private var isCreatingNewTable: Boolean = false

  /** The protocol of the snapshot that this transaction is reading at. */
  def protocol: Protocol = newProtocol.getOrElse(snapshot.protocolScala)

  /**
   * Returns the metadata for this transaction. The metadata refers to the metadata of the snapshot
   * at the transaction's read version unless updated during the transaction.
   */
  def metadata: Metadata = newMetadata.getOrElse(snapshot.metadataScala)

  ///////////////////////////////////////////////////////////////////////////
  // Public Java API Methods
  ///////////////////////////////////////////////////////////////////////////

  override def commit(
      actionsJ: java.lang.Iterable[ActionJ],
      op: Operation,
      writerId: String): CommitResult = {
    val actions = actionsJ.asScala.map(ConversionUtils.convertActionJ).toSeq

    // Try to commit at the next version.
    var finalActions = prepareCommit(actions)

    // TODO blind append check & create commitInfo using writerId

    val commitVersion = doCommit(snapshot.version + 1, finalActions)

    postCommit(commitVersion)

    new CommitResult(commitVersion)
  }

  override def markFilesAsRead(
      readPredicates: java.lang.Iterable[Expression]): java.util.List[AddFileJ] = {
    // TODO
    null
  }

  override def updateMetadata(metadata: MetadataJ): Unit = {

  }

  override def readWholeTable(): Unit = {

  }

  override def txnVersion(id: String): Long = {
    // TODO
    0L
  }

  ///////////////////////////////////////////////////////////////////////////
  // Critical Internal-Only Methods
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Prepare for a commit by doing all necessary pre-commit checks and modifications to the actions.
   * @return The finalized set of actions.
   */
  private def prepareCommit(actions: Seq[Action]): Seq[Action] = {
    assert(!committed, "Transaction already committed.")

    val userCommitInfo = actions.exists(_.isInstanceOf[CommitInfo])
    assert(!userCommitInfo,
      "CommitInfo actions are created automatically. Users shouldn't try to commit them directly")

    // If the metadata has changed, add that to the set of actions
    var finalActions = newMetadata.toSeq ++ actions
    val metadataChanges = finalActions.collect { case m: Metadata => m }
    assert(metadataChanges.length <= 1,
      "Cannot change the metadata more than once in a transaction.")

    metadataChanges.foreach(m => verifyNewMetadata(m))
    finalActions = newProtocol.toSeq ++ finalActions

    if (snapshot.version == -1) {
      deltaLog.ensureLogDirectoryExist()

      // If this is the first commit and no protocol is specified, initialize the protocol version.
      if (!finalActions.exists(_.isInstanceOf[Protocol])) {
        finalActions = protocol +: finalActions
      }

      // If this is the first commit and no metadata is specified, throw an exception
      if (!finalActions.exists(_.isInstanceOf[Metadata])) {
        throw DeltaErrors.metadataAbsentException()
      }
    }

    val partitionColumns = metadata.partitionColumns.toSet
    finalActions.foreach {
      case newVersion: Protocol =>
        require(newVersion.minReaderVersion > 0, "The reader version needs to be greater than 0")
        require(newVersion.minWriterVersion > 0, "The writer version needs to be greater than 0")
        if (!isCreatingNewTable) {
          val currentVersion = snapshot.protocolScala
          if (newVersion.minReaderVersion < currentVersion.minReaderVersion ||
              newVersion.minWriterVersion < currentVersion.minWriterVersion) {
            throw DeltaErrors.protocolDowngradeException(currentVersion, newVersion)
          }
        }
      case a: AddFile if partitionColumns != a.partitionValues.keySet =>
        throw DeltaErrors.addFilePartitioningMismatchException(
          a.partitionValues.keySet.toSeq, partitionColumns.toSeq)
      case _ => // nothing
    }

    deltaLog.assertProtocolWrite(snapshot.protocolScala)

    // We make sure that this isn't an appendOnly table as we check if we need to delete files.
    val removes = actions.collect { case r: RemoveFile => r }
    if (removes.exists(_.dataChange)) deltaLog.assertRemovable()

    finalActions
  }

  /**
   * Commit `actions` using `attemptVersion` version number.
   *
   * If you detect any conflicts, try to resolve logical conflicts and commit using a new version.
   *
   * @return the real version that was committed.
   * @throws IllegalStateException if the attempted commit version is ahead of the current delta log
   *                               version
   * @throws ConcurrentModificationException if any conflicts are detected
   */
  private def doCommit(attemptVersion: Long, actions: Seq[Action]): Long = lockCommitIfEnabled {
    try {
      deltaLog.store.write(
        FileNames.deltaFile(deltaLog.logPath, attemptVersion),
        actions.map(_.json).toIterator
      )

      val postCommitSnapshot = deltaLog.update()
      if (postCommitSnapshot.version < attemptVersion) {
        throw new IllegalStateException(
          s"The committed version is $attemptVersion " +
            s"but the current version is ${postCommitSnapshot.version}.")
      }

      attemptVersion
    } catch {
      case _: java.nio.file.FileAlreadyExistsException =>
        throw new DeltaErrors.DeltaConcurrentModificationException("TODO msg")
    }
  }

  /**
   * Perform post-commit operations
   */
  private def postCommit(commitVersion: Long): Unit = {
    committed = true

    // TODO: checkpoint
  }

  ///////////////////////////////////////////////////////////////////////////
  // Helper Methods
  ///////////////////////////////////////////////////////////////////////////

  private def verifyNewMetadata(metadata: Metadata): Unit = {
    SchemaMergingUtils.checkColumnNameDuplication(metadata.schema, "in the metadata update")
    SchemaUtils.checkFieldNames(SchemaMergingUtils.explodeNestedFieldNames(metadata.dataSchema))

    try {
      SchemaUtils.checkFieldNames(metadata.partitionColumns)
    } catch {
      // TODO: case e: AnalysisException ?
      case e: RuntimeException => throw DeltaErrors.invalidPartitionColumn(e)
    }

    // TODO: this function is still incomplete
    val needsProtocolUpdate = Protocol.checkProtocolRequirements(metadata, protocol)

    if (needsProtocolUpdate.isDefined) {
      newProtocol = needsProtocolUpdate
    }
  }

  private def isCommitLockEnabled: Boolean = {
// TODO:
//    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COMMIT_LOCK_ENABLED).getOrElse(
//      deltaLog.store.isPartialWriteVisible(deltaLog.logPath))
    true
  }

  private def lockCommitIfEnabled[T](body: => T): T = {
    if (isCommitLockEnabled) {
      deltaLog.lockInterruptibly(body)
    } else {
      body
    }
  }

  /**
   * Perform post-commit operations
   */
  private def postCommit(commitVersion: Long): Unit = {
    // TODO
  }

  ///////////////////////////////////////////////////////////////////////////
  // Helper Methods
  ///////////////////////////////////////////////////////////////////////////

  private def verifyNewMetadata(metadata: Metadata): Unit = {
    // TODO
  }
}
