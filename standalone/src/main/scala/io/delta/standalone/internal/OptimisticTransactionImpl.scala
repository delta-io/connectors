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

import java.lang
import java.nio.file.FileAlreadyExistsException
import java.util.ConcurrentModificationException

import scala.collection.JavaConverters._
import scala.collection.mutable

import io.delta.standalone.{CommitConflictChecker, OptimisticTransaction}
import io.delta.standalone.actions.{Action => ActionJ, AddFile => AddFileJ}
import io.delta.standalone.data.{RowRecord => RowRecordJ}
import io.delta.standalone.operations.{Operation => OperationJ}
import io.delta.standalone.internal.actions.{Action, AddFile, CommitInfo, FileAction, Metadata, Protocol, RemoveFile}
import io.delta.standalone.internal.data.{ParquetDataWriter, RowParquetRecordImpl}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.sources.StandaloneHadoopConf
import io.delta.standalone.internal.util.{ConversionUtils, FileNames, SchemaMergingUtils, SchemaUtils}

private[internal] class OptimisticTransactionImpl(
    deltaLog: DeltaLogImpl,
    snapshot: SnapshotImpl) extends OptimisticTransaction {

  import OptimisticTransactionImpl._

  /** Tracks specific files that have been seen by this transaction. */
  protected val readFiles = new mutable.HashSet[AddFile]

  /** Tracks if this transaction has already committed. */
  private var committed = false

  private var isBlindAppend = false

  /** Stores the updated metadata (if any) that will result from this txn. */
//  private var newMetadata: Option[Metadata] = None
  private val newMetadata: Option[Metadata] = None // TODO: remove?

  /** Stores the updated protocol (if any) that will result from this txn. */
  private var newProtocol: Option[Protocol] = None

  /**
   * Tracks the start time since we started trying to write a particular commit.
   * Used for logging duration of retried transactions.
   */
  private var commitAttemptStartTime: Long = _

  private var externalConflictChecker: Option[CommitConflictChecker] = None

  // Whether this transaction is creating a new table.
//  private var isCreatingNewTable: Boolean = false
  private val isCreatingNewTable: Boolean = false // TODO: remove?

  /** The protocol of the snapshot that this transaction is reading at. */
  def protocol: Protocol = newProtocol.getOrElse(snapshot.protocolScala)

  /**
   * Returns the metadata for this transaction. The metadata refers to the metadata of the snapshot
   * at the transaction's read version unless updated during the transaction. TODO: update comment?
   */
  def metadata: Metadata = newMetadata.getOrElse(snapshot.metadataScala) // TODO: only snapshot.___?

  ///////////////////////////////////////////////////////////////////////////
  // Public Java API Methods
  ///////////////////////////////////////////////////////////////////////////

  override def commit(actionsJ: java.util.List[ActionJ], opJ: OperationJ): Long = {
    val actions = actionsJ.asScala.map(ConversionUtils.convertActionJ)
    val op: DeltaOperations.Operation = null // TODO convert opJ to scala
    commit(actions, None) // TODO: Some(op)
  }

  /**
   * We either keep RowRecord.java as an interface and RowRecordImpl.scala implements it
   * (rename RowParquetRecordImpl -> RowRecordImpl). In this function we cast it to the Scala
   * implementation.
   *
   * Or RowRecord.java becomes a class. And HAS a RowRecordImpl. and getImpl is a public method
   * In this function we map each java instance to _.getImpl to get the scala references.
   */
  // TODO: document how if this is the 1st commit, then txn.updateMetadata() must have been called
  // TODO: should be iter
  override def writeRecordsAndCommit(data: java.util.List[RowRecordJ]): Long = {
    // TODO maybe return current snapshot version? edge case?
//    require(!data.isEmpty, "cannot write and commit empty record data list")

    val addFile = ParquetDataWriter.write(
      deltaLog.dataPath,
      data.asScala.map(_.getUnderlyingRecord),
      metadata.schema
    )

    commit(addFile :: Nil, null)
  }

  override def setConflictResolutionMeta(
      readFilesJ: java.lang.Iterable[AddFileJ],
      checker: CommitConflictChecker): Unit = {
    // TODO: check if we have already committed? If so, why is the user doing this?
    require(!isBlindAppend,
      "`setIsBlindAppend` has already been called. There shouldn't be any need to call " +
        "`setConflictResolutionMeta`.")

    readFiles ++= readFilesJ.asScala.map(ConversionUtils.convertAddFileJ)
    externalConflictChecker = Some(checker)
  }

  override def setIsBlindAppend(): Unit = {
    // TODO: check if we have already committed? If so, why is the user doing this?
    require(readFiles.isEmpty && externalConflictChecker.isEmpty,
      "`setConflictResolutionMeta` has already been called. There shouldn't be any need to call " +
        "`setIsBlindAppend`.")
    isBlindAppend = true
  }

  ///////////////////////////////////////////////////////////////////////////
  // Critical Internal-Only Methods
  ///////////////////////////////////////////////////////////////////////////

  private def commit(
      actions: Seq[Action],
      op: Option[DeltaOperations.Operation]): Long = {

    // Try to commit at the next version.
    var finalActions = prepareCommit(actions)

    // Find the isolation level to use for this commit
    val noDataChanged = actions.collect { case f: FileAction => f.dataChange }.forall(_ == false)
    val isolationLevelToUse = if (noDataChanged) {
      // If no data has changed (i.e. its is only being rearranged), then SnapshotIsolation
      // provides Serializable guarantee. Hence, allow reduced conflict detection by using
      // SnapshotIsolation of what the table isolation level is.
      SnapshotIsolation
    } else {
      Serializable
    }

    if (deltaLog.hadoopConf.getBoolean(StandaloneHadoopConf.DELTA_COMMIT_INFO_ENABLED, true)) {
//      val isBlindAppend = false // TODO
//      val commitInfo = CommitInfo.empty() // TODO CommitInfo(...
//      finalActions = commitInfo +: finalActions // prepend commitInfo
    }

    commitAttemptStartTime = System.currentTimeMillis()

    val commitVersion = doCommitRetryIteratively(
      snapshot.version + 1,
      finalActions,
      isolationLevelToUse)

    postCommit(commitVersion)

    // TODO: runPostCommitHooks ?

    commitVersion
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
      case _ => // nothing
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
  protected def doCommitRetryIteratively(
      attemptVersion: Long,
      actions: Seq[Action],
      isolationLevel: IsolationLevel): Long = lockCommitIfEnabled {
    var tryCommit = true
    var commitVersion = attemptVersion
    var attemptNumber = 0

    while (tryCommit) {
      try {
        if (attemptNumber == 0) {
          doCommit(commitVersion, actions)
        } else if (attemptNumber > DELTA_MAX_RETRY_COMMIT_ATTEMPTS) {
          val totalCommitAttemptTime = System.currentTimeMillis() - commitAttemptStartTime
          throw DeltaErrors.maxCommitRetriesExceededException(
            attemptNumber,
            commitVersion,
            attemptVersion,
            actions.length,
            totalCommitAttemptTime)
        } else {
          commitVersion = checkForConflicts(commitVersion, actions, isolationLevel)
          doCommit(commitVersion, actions)
        }
        tryCommit = false
      } catch {
        case _: FileAlreadyExistsException => attemptNumber += 1
      }
    }
    commitVersion
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
        // TODO: DeltaErrors... ?
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

    if (shouldCheckpoint(commitVersion)) {
      // We checkpoint the version to be committed to so that no two transactions will checkpoint
      // the same version.
      deltaLog.checkpoint(deltaLog.getSnapshotForVersionAsOf(commitVersion))
    }
  }

  /**
   * Looks at actions that have happened since the txn started and checks for logical
   * conflicts with the read/writes. If no conflicts are found return the commit version to attempt
   * next.
   */
  private def checkForConflicts(
      checkVersion: Long,
      actions: Seq[Action],
      commitIsolationLevel: IsolationLevel): Long = {
    val nextAttemptVersion = getNextAttemptVersion

    val conflictCheckerToUse = externalConflictChecker
      .getOrElse(getDefaultExternalConflictChecker(isBlindAppend))

    val currentTransactionInfo = CurrentTransactionInfo(
      externalConflictChecker = conflictCheckerToUse,
      readFiles = readFiles.toSet,
      readWholeTable = false, // TODO readTheWholeTable
      readAppIds = Nil.toSet, // TODO: readTxn.toSet,
      metadata = metadata,
      actions = actions,
      deltaLog = deltaLog)

    (checkVersion until nextAttemptVersion).foreach { otherCommitVersion =>
      val conflictChecker = new ConflictChecker(
        currentTransactionInfo,
        otherCommitVersion,
        commitIsolationLevel)

      conflictChecker.checkConflicts()
    }

    nextAttemptVersion
  }

  private def verifyNewMetadata(metadata: Metadata): Unit = {
    // TODO assert(!CharVarcharUtils.hasCharVarchar...) ?
    SchemaMergingUtils.checkColumnNameDuplication(metadata.schema, "in the metadata update")
    SchemaUtils.checkFieldNames(SchemaMergingUtils.explodeNestedFieldNames(metadata.dataSchema))
    val partitionColCheckIsFatal = deltaLog.hadoopConf.getBoolean(
      StandaloneHadoopConf.DELTA_PARTITION_COLUMN_CHECK_ENABLED, true)

    try {
      SchemaUtils.checkFieldNames(metadata.partitionColumns)
    } catch {
      // TODO: case e: AnalysisException ?
      case e: RuntimeException if (partitionColCheckIsFatal) =>
        throw DeltaErrors.invalidPartitionColumn(e)
    }

    // TODO: this function is still incomplete
    val needsProtocolUpdate = Protocol.checkProtocolRequirements(metadata, protocol)

    if (needsProtocolUpdate.isDefined) {
      newProtocol = needsProtocolUpdate
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Helper Methods
  ///////////////////////////////////////////////////////////////////////////

  /** Returns the next attempt version given the last attempted version */
  private def getNextAttemptVersion: Long = {
    deltaLog.update()
    deltaLog.snapshot.version + 1
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
   * Returns true if we should checkpoint the version that has just been committed.
   */
  private def shouldCheckpoint(committedVersion: Long): Boolean = {
    committedVersion != 0 && committedVersion % deltaLog.checkpointInterval == 0
  }
}

private[internal] object OptimisticTransactionImpl {

  val DELTA_MAX_RETRY_COMMIT_ATTEMPTS = 10000000

  /**
   * if isBlindAppend is TRUE, then doesConflict should return FALSE. (i.e. blind appends should
   * have no conflicts)
   *
   * if isBlindAppend is FALSE, then doesConflict should return TRUE. (i.e. by default, always
   * assume there is a conflict)
   */
  def getDefaultExternalConflictChecker(isBlindAppend: Boolean): CommitConflictChecker =
    (_: java.util.List[AddFileJ]) => !isBlindAppend
}
