package io.delta.standalone.internal

// scalastyle:off funsuite
import org.scalatest.FunSuite

class OptimisticTransactionSuite extends FunSuite {
  // scalastyle:on funsuite

  // TODO: test prepareCommit > assert not already committed

  // TODO: test prepareCommit > have more than 1 Metadata in transaction

  // TODO: test prepareCommit > ensureLogDirectoryExist throws

  // TODO: test prepareCommit > commitValidationEnabled & metadataAbsentException

  // TODO: test prepareCommit > !commitValidationEnabled & no metadataAbsentException

  // TODO: test prepareCommit > protocolDowngradeException

  // TODO: test prepareCommit > commitValidationEnabled & addFilePartitioningMismatchException

  // TODO: test prepareCommit > !commitValidationEnabled & no addFilePartitioningMismatchException

  // TODO: test prepareCommit > assertProtocolWrite

  // TODO: test prepareCommit > assertRemovable

  // TODO: test verifyNewMetadata > SchemaMergingUtils.checkColumnNameDuplication

  // TODO: test verifyNewMetadata > SchemaUtils.checkFieldNames(dataSchema) > ParquetSchemaConverter

  // TODO: test verifyNewMetadata > SchemaUtils.checkFieldNames(dataSchema) > invalidColumnName

  // TODO: test verifyNewMetadata > ...checkFieldNames(partitionColumns) > ParquetSchemaConverter

  // TODO: test verifyNewMetadata > ...checkFieldNames(partitionColumns) > invalidColumnName

  // TODO: test verifyNewMetadata > Protocol.checkProtocolRequirements

  // TODO: test commit > DELTA_COMMIT_INFO_ENABLED & commitInfo is actually added to final actions
}
