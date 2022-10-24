/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

import java.util.{Locale, UUID}

import scala.collection.mutable

import io.delta.standalone.DeltaLog
import io.delta.standalone.actions.{Metadata => MetadataJ}
import io.delta.standalone.types.{FieldMetadata, StructField, StructType}

import io.delta.standalone.internal.actions.{Metadata, Protocol}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.util.{CaseInsensitiveMap, ConversionUtils, SchemaMergingUtils, SchemaUtils}

trait DeltaColumnMappingBase {
  val MIN_WRITER_VERSION = 5
  val MIN_READER_VERSION = 2
  val MIN_PROTOCOL_VERSION = new Protocol(MIN_READER_VERSION, MIN_WRITER_VERSION)

  val PARQUET_FIELD_ID_METADATA_KEY = "parquet.field.id"
  val COLUMN_MAPPING_METADATA_PREFIX = "delta.columnMapping."
  val COLUMN_MAPPING_METADATA_ID_KEY = COLUMN_MAPPING_METADATA_PREFIX + "id"
  val COLUMN_MAPPING_PHYSICAL_NAME_KEY = COLUMN_MAPPING_METADATA_PREFIX + "physicalName"

  /**
   * This list of internal columns (and only this list) is allowed to have missing
   * column mapping metadata such as field id and physical name because
   * they might not be present in user's table schema.
   *
   * These fields, if materialized to parquet, will always be matched by their display name in the
   * downstream parquet reader even under column mapping modes.
   *
   * For future developers who want to utilize additional internal columns without generating
   * column mapping metadata, please add them here.
   *
   * This list is case-insensitive.
   *
   * TODO: Currently this list is empty. We will be adding to the list in future.
   */
  protected val DELTA_INTERNAL_COLUMNS: Set[String] = Set.empty

  val supportedModes: Set[DeltaColumnMappingMode] =
      Set(NoMapping, NameMapping)

  def isInternalField(field: StructField): Boolean = DELTA_INTERNAL_COLUMNS
      .contains(field.getName.toLowerCase(Locale.ROOT))

  def requiresNewProtocol(metadataJ: MetadataJ): Boolean = {
    val metadata = ConversionUtils.convertMetadataJ(metadataJ)
    metadata.columnMappingMode match {
      case IdMapping => true
      case NameMapping => true
      case NoMapping => false
    }
  }

  def satisfyColumnMappingProtocol(protocol: Protocol): Boolean =
    protocol.minWriterVersion >= MIN_WRITER_VERSION &&
        protocol.minReaderVersion >= MIN_READER_VERSION

  /**
   * The only allowed mode change is from NoMapping to NameMapping. Other changes
   * would require re-writing Parquet files and are not supported right now.
   */
  private def allowMappingModeChange(
      oldMode: DeltaColumnMappingMode,
      newMode: DeltaColumnMappingMode): Boolean = {
    if (oldMode == newMode) true
    else oldMode == NoMapping && newMode == NameMapping
  }

  def isColumnMappingUpgrade(
      oldMode: DeltaColumnMappingMode,
      newMode: DeltaColumnMappingMode): Boolean = {
    oldMode == NoMapping && newMode != NoMapping
  }

  /**
   * If the table is already on the column mapping protocol, we block:
   *     - changing column mapping config
   * otherwise, we block
   *     - upgrading to the column mapping Protocol through configurations
   */
  def verifyAndUpdateMetadataChange(
      deltaLog: DeltaLog,
      oldProtocol: Protocol,
      oldMetadataJ: MetadataJ,
      newMetadataJ: MetadataJ,
      isCreatingNewTable: Boolean): MetadataJ = {

    val oldMetadata = ConversionUtils.convertMetadataJ(oldMetadataJ)
    val newMetadata = ConversionUtils.convertMetadataJ(newMetadataJ)

    // field in new metadata should have been dropped
    val oldMappingMode = oldMetadata.columnMappingMode
    val newMappingMode = newMetadata.columnMappingMode

    if (!supportedModes.contains(newMappingMode)) {
      throw DeltaErrors.unsupportedColumnMappingMode(newMappingMode.name)
    }

    val isChangingModeOnExistingTable = oldMappingMode != newMappingMode && !isCreatingNewTable
    if (isChangingModeOnExistingTable) {
      if (!allowMappingModeChange(oldMappingMode, newMappingMode)) {
        throw DeltaErrors.changeColumnMappingModeNotSupported(
          oldMappingMode.name, newMappingMode.name)
      } else {
        // legal mode change, now check if protocol is upgraded before or part of this txn
        val caseInsensitiveMap = CaseInsensitiveMap(newMetadata.configuration)
        val newProtocol = new Protocol(
          minReaderVersion = caseInsensitiveMap
              .get(Protocol.MIN_READER_VERSION_PROP).map(_.toInt)
              .getOrElse(oldProtocol.minReaderVersion),
          minWriterVersion = caseInsensitiveMap
              .get(Protocol.MIN_WRITER_VERSION_PROP).map(_.toInt)
              .getOrElse(oldProtocol.minWriterVersion))

        // TODO: Need to properly upgrade the protocol
//        if (!satisfyColumnMappingProtocol(newProtocol)) {
//          throw DeltaErrors.changeColumnMappingModeOnOldProtocol(oldProtocol)
//        }
      }
    }

    val updatedMetadata = tryFixMetadata(oldMetadata, newMetadata, isChangingModeOnExistingTable)

    // record column mapping table creation/upgrade
    if (newMappingMode != NoMapping) {
      // TODO: do we need events in standalone?
      // if (isCreatingNewTable) {
      //   recordDeltaEvent(deltaLog, "delta.columnMapping.createTable")
      // } else if (oldMappingMode != newMappingMode) {
      //   recordDeltaEvent(deltaLog, "delta.columnMapping.upgradeTable")
      // }
    }

    ConversionUtils.convertMetadata(updatedMetadata)
  }

  def hasColumnId(field: StructField): Boolean =
    field.getMetadata().contains(COLUMN_MAPPING_METADATA_ID_KEY)

  def getColumnId(field: StructField): Int =
    field.getMetadata().get(COLUMN_MAPPING_METADATA_ID_KEY).asInstanceOf[java.lang.Long].toInt

  def hasPhysicalName(field: StructField): Boolean =
    field.getMetadata().contains(COLUMN_MAPPING_PHYSICAL_NAME_KEY)

  /**
   * Gets the required column metadata for each column based on the column mapping mode.
   */
  def getColumnMappingMetadata(field: StructField, mode: DeltaColumnMappingMode): FieldMetadata = {
    mode match {
      case NoMapping =>
        // drop all column mapping related fields
        FieldMetadata.builder()
            .withMetadata(field.getMetadata().getEntries())
            .remove(COLUMN_MAPPING_METADATA_ID_KEY)
            .remove(PARQUET_FIELD_ID_METADATA_KEY)
            .remove(COLUMN_MAPPING_PHYSICAL_NAME_KEY)
            .build()

      case IdMapping =>
        if (!hasColumnId(field)) {
          throw DeltaErrors.missingColumnId(IdMapping, field.getName())
        }
        if (!hasPhysicalName(field)) {
          throw DeltaErrors.missingPhysicalName(IdMapping, field.getName())
        }
        FieldMetadata.builder()
            .withMetadata(field.getMetadata().getEntries())
            .putLong(PARQUET_FIELD_ID_METADATA_KEY, getColumnId(field))
            .build()

      case NameMapping =>
        if (!hasPhysicalName(field)) {
          throw DeltaErrors.missingPhysicalName(NameMapping, field.getName())
        }
        FieldMetadata.builder()
            .withMetadata(field.getMetadata().getEntries())
            .remove(COLUMN_MAPPING_METADATA_ID_KEY)
            .remove(PARQUET_FIELD_ID_METADATA_KEY)
            .build()

      case mode =>
        throw DeltaErrors.unsupportedColumnMappingMode(mode.name)
    }
  }

  /**
   * Prepares the table schema, to be used by the readers and writers of the table.
   *
   * In the new Delta protocol that supports column mapping, we persist various column mapping
   * metadata in the serialized schema of the Delta log. This method performs the necessary
   * transformation and filtering on these metadata based on the column mapping mode set for the
   * table.
   *
   * @param schema the raw schema directly deserialized from the Delta log, with various column
   *               mapping metadata.
   * @param mode column mapping mode of the table
   *
   * @return the table schema for the readers and writers. Columns will need to be renamed
   *         by using the `renameColumns` function.
   */
  def setColumnMetadata(schema: StructType, mode: DeltaColumnMappingMode): StructType = {
    SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
      field.withNewMetadata(getColumnMappingMetadata(field, mode))
    }
  }

  /** Recursively renames columns in the given schema with their physical schema. */
  def renameColumns(schema: StructType): StructType = {
    SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
      field.withNewName(getPhysicalName(field))
    }
  }

  def assignPhysicalNames(schema: StructType): StructType = {
    SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
      val existingName = if (hasPhysicalName(field)) Option(getPhysicalName(field)) else None
      val metadata = FieldMetadata.builder()
          .withMetadata(field.getMetadata().getEntries())
          .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, existingName.getOrElse(generatePhysicalName))
          .build()
      field.withNewMetadata(metadata)
    }
  }

  def generatePhysicalName: String = "col-" + UUID.randomUUID()

  def getPhysicalName(field: StructField): String = {
    if (field.getMetadata().contains(COLUMN_MAPPING_PHYSICAL_NAME_KEY)) {
      field.getMetadata().get(COLUMN_MAPPING_PHYSICAL_NAME_KEY).toString
    } else {
      field.getName()
    }
  }

  def tryFixMetadata(
      oldMetadata: Metadata,
      newMetadata: Metadata,
      isChangingModeOnExistingTable: Boolean): Metadata = {
    val newMappingMode = DeltaConfigs.COLUMN_MAPPING_MODE.fromMetadata(newMetadata)
    newMappingMode match {
      case IdMapping | NameMapping =>
        assignColumnIdAndPhysicalName(newMetadata, oldMetadata, isChangingModeOnExistingTable)
      case NoMapping =>
        newMetadata
      case mode =>
        throw DeltaErrors.unsupportedColumnMappingMode(mode.name)
    }
  }

  def findMaxColumnId(schema: StructType): Long = {
    var maxId: Long = 0
    SchemaMergingUtils.transformColumns(schema)((_, f, _) => {
      if (hasColumnId(f)) {
        maxId = maxId max getColumnId(f)
      }
      f
    })
    maxId
  }

  def checkColumnIdAndPhysicalNameAssignments(
      schema: StructType,
      mode: DeltaColumnMappingMode): Unit = {
    // physical name/column id -> full field path
    val columnIds = mutable.Set[Int]()
    val physicalNames = mutable.Set[String]()

    // use id mapping to keep all column mapping metadata
    // this method checks for missing physical name & column id already
    val physicalSchema = createPhysicalSchema(schema, schema, NoMapping, checkSupportedMode = false)

    SchemaMergingUtils.transformColumns(physicalSchema) ((parentPhysicalPath, field, _) => {
      // field.name is now physical name
      // We also need to apply backticks to column paths with dots in them to prevent a possible
      // false alarm in which a column `a.b` is duplicated with `a`.`b`
      val curFullPhysicalPath = SchemaUtils.prettyFieldName(parentPhysicalPath :+ field.getName())
      val columnId = getColumnId(field)
      if (columnIds.contains(columnId)) {
        throw DeltaErrors.duplicatedColumnId(mode, columnId, schema)
      }
      columnIds.add(columnId)

      // We should check duplication by full physical name path, because nested fields
      // such as `a.b.c` shouldn't conflict with `x.y.c` due to same column name.
      if (physicalNames.contains(curFullPhysicalPath)) {
        throw DeltaErrors.duplicatedPhysicalName(mode, curFullPhysicalPath, schema)
      }
      physicalNames.add(curFullPhysicalPath)

      field
    })
  }

  /**
   * For each column/field in a Metadata's schema, assign id using the current maximum id
   * as the basis and increment from there, and assign physical name using UUID
   * @param newMetadata The new metadata to assign Ids and physical names
   * @param oldMetadata The old metadata
   * @param isChangingModeOnExistingTable whether this is part of a commit that changes the
   *                                      mapping mode on a existing table
   * @return new metadata with Ids and physical names assigned
   */
  def assignColumnIdAndPhysicalName(
      newMetadata: Metadata,
      oldMetadata: Metadata,
      isChangingModeOnExistingTable: Boolean): Metadata = {
    val rawSchema = newMetadata.schema
    var maxId = DeltaConfigs.COLUMN_MAPPING_MAX_ID.fromMetadata(newMetadata) max
        findMaxColumnId(rawSchema)
    val newSchema =
      SchemaMergingUtils.transformColumns(rawSchema)((path, field, _) => {
        val builder = FieldMetadata.builder()
          .withMetadata(field.getMetadata().getEntries())
        if (!hasColumnId(field)) {
          maxId += 1
          builder.putLong(COLUMN_MAPPING_METADATA_ID_KEY, maxId)
        }
        if (!hasPhysicalName(field)) {
          val physicalName = if (isChangingModeOnExistingTable) {
            val fullName = path :+ field.getName()
            val existingField =
              SchemaUtils.findNestedFieldIgnoreCase(
                oldMetadata.schema, fullName, includeCollections = true)
            if (existingField.isEmpty) {
              throw DeltaErrors.schemaChangeDuringMappingModeChangeNotSupported(
                oldMetadata.schema, newMetadata.schema)
            } else {
              // When changing from NoMapping to NameMapping mode, we directly use old display names
              // as physical names. This is by design: 1) We don't need to rewrite the
              // existing Parquet files, and 2) display names in no-mapping mode have all the
              // properties required for physical names: unique, stable and compliant with Parquet
              // column naming restrictions.
              existingField.get.getName()
            }
          } else {
            generatePhysicalName
          }

          builder.putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, physicalName)
        }
        field.withNewMetadata(builder.build())
      })

    newMetadata.copy(
      schemaString = newSchema.toJson,
      configuration =
        newMetadata.configuration ++ Map(DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> maxId.toString)
      )
  }

  /**
   * Create a physical schema for the given schema using the Delta table schema as a reference.
   *
   * @param schema the given logical schema (potentially without any metadata)
   * @param referenceSchema the schema from the delta log, which has all the metadata
   * @param columnMappingMode column mapping mode of the delta table, which determines which
   *                          metadata to fill in
   * @param checkSupportedMode whether we should check of the column mapping mode is supported
   */
  def createPhysicalSchema(
      schema: StructType,
      referenceSchema: StructType,
      columnMappingMode: DeltaColumnMappingMode,
      checkSupportedMode: Boolean = true): StructType = {
    if (columnMappingMode == NoMapping) {
      return schema
    }

    // createPhysicalSchema is the narrow-waist for both read/write code path
    // so we could check for mode support here
    if (checkSupportedMode && !supportedModes.contains(columnMappingMode)) {
      throw DeltaErrors.unsupportedColumnMappingMode(columnMappingMode.name)
    }

    SchemaMergingUtils.transformColumns(schema) { (path, field, _) =>
      val fullName = path :+ field.getName()
      val inSchema = SchemaUtils
          .findNestedFieldIgnoreCase(referenceSchema, fullName, includeCollections = true)
      inSchema.map { refField =>
        val sparkMetadata = getColumnMappingMetadata(refField, columnMappingMode)
        field.withNewMetadata(sparkMetadata).withNewName(getPhysicalName(refField))
      }.getOrElse {
        if (isInternalField(field)) {
          field
        } else {
          throw DeltaErrors.columnNotFound(fullName, referenceSchema)
        }
      }
    }
  }
}

object DeltaColumnMapping extends DeltaColumnMappingBase

/**
 * A trait for Delta column mapping modes.
 */
sealed trait DeltaColumnMappingMode {
  def name: String
}

/**
 * No mapping mode uses a column's display name as its true identifier to
 * read and write data.
 *
 * This is the default mode and is the same mode as Delta always has been.
 */
case object NoMapping extends DeltaColumnMappingMode {
  val name = "none"
}

/**
 * Id Mapping uses column ID as the true identifier of a column. Column IDs are stored as
 * StructField metadata in the schema and will be used when reading and writing Parquet files.
 * The Parquet files in this mode will also have corresponding field Ids for each column in their
 * file schema.
 *
 * This mode is used for tables converted from Iceberg.
 */
case object IdMapping extends DeltaColumnMappingMode {
  val name = "id"
}

/**
 * Name Mapping uses the physical column name as the true identifier of a column. The physical name
 * is stored as part of StructField metadata in the schema and will be used when reading and writing
 * Parquet files. Even if id mapping can be used for reading the physical files, name mapping is
 * used for reading statistics and partition values in the DeltaLog.
 */
case object NameMapping extends DeltaColumnMappingMode {
  val name = "name"
}

object DeltaColumnMappingMode {
  def apply(name: String): DeltaColumnMappingMode = {
    name.toLowerCase(Locale.ROOT) match {
      case NoMapping.name => NoMapping
      case IdMapping.name => IdMapping
      case NameMapping.name => NameMapping
      case mode => throw DeltaErrors.unsupportedColumnMappingMode(mode)
    }
  }
}
