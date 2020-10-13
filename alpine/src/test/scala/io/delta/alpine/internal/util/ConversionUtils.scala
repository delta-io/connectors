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

package io.delta.alpine.internal.util

import io.delta.alpine.internal
import io.delta.alpine.internal.CheckpointMetaData
import io.delta.alpine.internal.actions.{AddFile, Metadata, Protocol}
import org.apache.spark.sql.delta.actions.{AddFile => AddFileOSS, Format => FormatOSS, Metadata => MetadataOSS, Protocol => ProtocolOSS}

trait ConversionUtils {

  def convertAddFile(file: AddFile): AddFileOSS = {
    AddFileOSS(
      file.path,
      file.partitionValues,
      file.size,
      file.modificationTime,
      file.dataChange,
      file.stats,
      file.tags)
  }

  def convertMetaData(metadata: Metadata): MetadataOSS = {
    MetadataOSS(
      metadata.id,
      metadata.name,
      metadata.description,
      FormatOSS(metadata.format.provider, metadata.format.options),
      metadata.schemaString,
      metadata.partitionColumns,
      metadata.configuration,
      metadata.createdTime)
  }

  def convertProtocol(protocol: Protocol): ProtocolOSS = {
    ProtocolOSS(protocol.minReaderVersion, protocol.minWriterVersion)
  }

  def convertCheckpointMetaData(data: internal.CheckpointMetaData): CheckpointMetaData = {
    CheckpointMetaData(data.version, data.size, data.parts)
  }
}
