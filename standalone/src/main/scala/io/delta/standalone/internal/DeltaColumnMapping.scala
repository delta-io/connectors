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

import io.delta.standalone.types.{FieldMetadata, StructField, StructType}

import io.delta.standalone.internal.actions.{Metadata, Protocol}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.exception.DeltaErrors._
import io.delta.standalone.internal.logging.Logging
import io.delta.standalone.internal.util.{CaseInsensitiveMap, SchemaMergingUtils, SchemaUtils}

trait DeltaColumnMappingBase extends Logging {

  // Minimum writer and reader version required for column mapping support
  val MIN_WRITER_VERSION = 5
  val MIN_READER_VERSION = 2
  val MIN_PROTOCOL_VERSION = new Protocol(MIN_READER_VERSION, MIN_WRITER_VERSION)

  // Field id that maps to the field id in the Parquet file. This is used
  // when the column mapping mode is ID
  val PARQUET_FIELD_ID_METADATA_KEY = "parquet.field.id"

  private val COLUMN_MAPPING_METADATA_PREFIX = "delta.columnMapping."

  // Id metadata key for the column - used incase of ID and NAME column mapping modes
  val COLUMN_MAPPING_METADATA_ID_KEY = COLUMN_MAPPING_METADATA_PREFIX + "id"

  // Physical name metadata key for the column - used incase of NAME column mapping mode
  // This could be different from the column name in schema.
  val COLUMN_MAPPING_PHYSICAL_NAME_KEY = COLUMN_MAPPING_METADATA_PREFIX + "physicalName"

  private val supportedModes: Set[DeltaColumnMappingMode] = Set(NoMapping, NameMapping)

  /**
   * Update the metadata based on valid column mapping transition. If the column mapping
   * transition is not valid an error is thrown.
   */
  def verifyAndUpdateMetadataChange(
      oldProtocol: Protocol,
      oldMetadata: Metadata,
      newMetadata: Metadata,
      isCreatingNewTable: Boolean): Metadata = {

    // field in new metadata should have been dropped
    val oldMappingMode = oldMetadata.columnMappingMode
    val newMappingMode = newMetadata.columnMappingMode

    if (!supportedModes.contains(newMappingMode)) {
      throw unsupportedColumnMappingMode(newMappingMode.name)
    }

    val isChangingModeOnExistingTable = oldMappingMode != newMappingMode && !isCreatingNewTable
    if (isChangingModeOnExistingTable) {
      if (!allowMappingModeChange(oldMappingMode, newMappingMode)) {
        throw changeColumnMappingModeNotSupported(oldMappingMode.name, newMappingMode.name)
      }
      // legal mode change, now check if protocol is upgraded before or part of this update
      val caseInsensitiveMap = CaseInsensitiveMap(newMetadata.configuration)
      val newProtocol = new Protocol(
        minReaderVersion = caseInsensitiveMap
            .get(Protocol.MIN_READER_VERSION_PROP).map(_.toInt)
            .getOrElse(oldProtocol.minReaderVersion),
        minWriterVersion = caseInsensitiveMap
            .get(Protocol.MIN_WRITER_VERSION_PROP).map(_.toInt)
            .getOrElse(oldProtocol.minWriterVersion))

        if (!satisfyColumnMappingProtocol(newProtocol)) {
          throw changeColumnMappingModeOnOldProtocol(oldProtocol)
      }
    }

    tryFixMetadata(oldMetadata, newMetadata, isChangingModeOnExistingTable)
  }

  private def tryFixMetadata(
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
        throw unsupportedColumnMappingMode(mode.name)
    }
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
  private def assignColumnIdAndPhysicalName(
      newMetadata: Metadata,
      oldMetadata: Metadata,
      isChangingModeOnExistingTable: Boolean): Metadata = {
    val rawSchema = newMetadata.schema
    var maxId = DeltaConfigs.COLUMN_MAPPING_MAX_ID.fromMetadata(newMetadata) max
        findMaxColumnId(rawSchema)
    val newSchema =
      SchemaMergingUtils.transformColumns(rawSchema)((path, field, _) => {
        // Start with existing field metadata and add needed column mapping metadata
        val builder = FieldMetadata.builder().withMetadata(field.getMetadata())

        if (!hasColumnId(field)) {
          maxId += 1
          builder.putLong(COLUMN_MAPPING_METADATA_ID_KEY, maxId)
        }

        if (!hasPhysicalName(field)) {
          val physicalName = if (isChangingModeOnExistingTable) {
            val fullName = path :+ field.getName()
            val existingField = SchemaUtils.findNestedFieldIgnoreCase(oldMetadata.schema, fullName)
            if (existingField.isEmpty) {
              throw schemaChangeDuringMappingModeChangeNotSupported(
                oldMetadata.schema, newMetadata.schema)
            }
            // When changing from NoMapping to NameMapping mode, we directly use old display names
            // as physical names. This is by design: 1) We don't need to rewrite the
            // existing Parquet files, and 2) display names in no-mapping mode have all the
            // properties required for physical names: unique, stable and compliant with Parquet
            // column naming restrictions.
            existingField.get.getName()
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
        newMetadata.configuration ++ Map(DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> maxId.toString))
  }

  /**
   * The only allowed mode change is from NoMapping to NameMapping. Other changes
   * would require re-writing Parquet files and are not supported right now.
   */
  private def allowMappingModeChange(
      oldMode: DeltaColumnMappingMode, newMode: DeltaColumnMappingMode): Boolean = {
    if (oldMode == newMode) true
    else oldMode == NoMapping && newMode == NameMapping
  }

  private def satisfyColumnMappingProtocol(protocol: Protocol): Boolean =
    protocol.minWriterVersion >= MIN_WRITER_VERSION &&
        protocol.minReaderVersion >= MIN_READER_VERSION

  private def findMaxColumnId(schema: StructType): Long = {
    var maxId: Long = 0
    SchemaMergingUtils.transformColumns(schema)((_, f, _) => {
      if (hasColumnId(f)) {
        maxId = maxId max getColumnId(f)
      }
      f
    })
    maxId
  }

  private def hasColumnId(field: StructField): Boolean =
    field.getMetadata().contains(COLUMN_MAPPING_METADATA_ID_KEY)

  private def getColumnId(field: StructField): Int =
    field.getMetadata().get(COLUMN_MAPPING_METADATA_ID_KEY).asInstanceOf[java.lang.Long].toInt

  private def hasPhysicalName(field: StructField): Boolean =
    field.getMetadata().contains(COLUMN_MAPPING_PHYSICAL_NAME_KEY)

  private def generatePhysicalName: String = "col-" + UUID.randomUUID()
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
