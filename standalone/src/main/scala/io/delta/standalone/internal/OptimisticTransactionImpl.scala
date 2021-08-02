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

import scala.collection.JavaConverters._

import io.delta.standalone.OptimisticTransaction
import io.delta.standalone.actions.{Action => ActionJ}
import io.delta.standalone.operations.{Operation => OperationJ}
import io.delta.standalone.internal.actions.{Action, AddFile, CommitInfo, Metadata, Protocol, RemoveFile}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.sources.StandaloneHadoopConf
import io.delta.standalone.internal.util.ConversionUtils

private[internal] class OptimisticTransactionImpl(
    deltaLog: DeltaLogImpl,
    snapshot: SnapshotImpl) extends OptimisticTransaction {

  /** Tracks if this transaction has already committed. */
  private var committed = false

  /** Stores the updated metadata (if any) that will result from this txn. */
//  private var newMetadata: Option[Metadata] = None
  private val newMetadata: Option[Metadata] = None // TODO: remove?

  /** Stores the updated protocol (if any) that will result from this txn. */
//  private var newProtocol: Option[Protocol] = None
  private val newProtocol: Option[Protocol] = None // TODO: remove?

  // Whether this transaction is creating a new table.
//  private var isCreatingNewTable: Boolean = false
  private val isCreatingNewTable: Boolean = false // TODO: remove?

  /** The protocol of the snapshot that this transaction is reading at. */
  def protocol: Protocol = newProtocol.getOrElse(snapshot.protocolScala) // TODO: only snapshot.___?

  /**
   * Returns the metadata for this transaction. The metadata refers to the metadata of the snapshot
   * at the transaction's read version unless updated during the transaction. TODO: update comment?
   */
  def metadata: Metadata = newMetadata.getOrElse(snapshot.metadataScala) // TODO: only snapshot.___?

  ///////////////////////////////////////////////////////////////////////////
  // Public API Methods
  ///////////////////////////////////////////////////////////////////////////

  override def commit(actionsJ: java.util.List[ActionJ]): Long =
    commit(actionsJ, Option.empty[DeltaOperations.Operation])

  override def commit(actionsJ: java.util.List[ActionJ], opJ: OperationJ): Long = {
    val op: DeltaOperations.Operation = null // convert opJ to scala
    commit(actionsJ, Some(op))
  }

  ///////////////////////////////////////////////////////////////////////////
  // Key Internal-Only Methods
  ///////////////////////////////////////////////////////////////////////////

  private def commit(
      actionsJ: java.util.List[ActionJ],
      op: Option[DeltaOperations.Operation]): Long = {
    val actions = actionsJ.asScala.map(ConversionUtils.convertActionJ)

    val version = try {
      // Try to commit at the next version.
      var finalActions = prepareCommit(actions)

      if (deltaLog.hadoopConf.getBoolean(StandaloneHadoopConf.DELTA_COMMIT_INFO_ENABLED, true)) {
        val commitInfo = CommitInfo.empty() // TODO
        finalActions = commitInfo +: finalActions // prepend commitInfo
      }

      val commitVersion = doCommitRetryIteratively(
        snapshot.version + 1,
        finalActions)

      commitVersion
    } catch {
      case _ => null // TODO
    }

    version

    0L
  }

  /**
   * Prepare for a commit by doing all necessary pre-commit checks and modifications to the actions.
   * @return The finalized set of actions.
   */
  private def prepareCommit(actions: Seq[Action]): Seq[Action] = {
    assert(!committed, "Transaction already committed.")
    val commitValidationEnabled =
      deltaLog.hadoopConf.getBoolean(StandaloneHadoopConf.DELTA_COMMIT_VALIDATION_ENABLED, true)

    // If the metadata has changed, add that to the set of actions
    var finalActions = newMetadata.toSeq ++ actions
    val metadataChanges = finalActions.collect { case m: Metadata => m }
    assert(
      metadataChanges.length <= 1, "Cannot change the metadata more than once in a transaction.")

    metadataChanges.foreach(m => verifyNewMetadata(m))
    finalActions = newProtocol.toSeq ++ finalActions

    if (snapshot.version == -1) {
      deltaLog.ensureLogDirectoryExist()

      // If this is the first commit and no protocol is specified, initialize the protocol version.
      if (!finalActions.exists(_.isInstanceOf[Protocol])) {
        finalActions = protocol +: finalActions
      }

      // If this is the first commit and no metadata is specified, throw an exception
      if (commitValidationEnabled && !finalActions.exists(_.isInstanceOf[Metadata])) {
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
      case a: AddFile if commitValidationEnabled && partitionColumns != a.partitionValues.keySet =>
        throw DeltaErrors.addFilePartitioningMismatchException(
          a.partitionValues.keySet.toSeq, partitionColumns.toSeq)
    }

    deltaLog.assertProtocolWrite(snapshot.protocolScala)

    // We make sure that this isn't an appendOnly table as we check if we need to delete files.
    val removes = actions.collect { case r: RemoveFile => r }
    if (removes.exists(_.dataChange)) deltaLog.assertRemovable()

    finalActions
  }

  /**
   * Commit `actions` using `attemptVersion` version number. If there are any conflicts that are
   * found, we will retry a fixed number of times.
   *
   * @return the real version that was committed
   */
  private def doCommitRetryIteratively(attemptVersion: Long, actions: Seq[Action]): Long = {
    // TODO
    0L
  }

  /**
   * Commit `actions` using `attemptVersion` version number. Throws a FileAlreadyExistsException
   * if any conflicts are detected.
   *
   * @return the real version that was committed.
   */
  private def doCommit(attemptVersion: Long, actions: Seq[Action], attemptNumber: Int): Long = {
    // TODO
    attemptVersion
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
