/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package compatibility

import scala.collection.JavaConverters._

trait ComparisonUtil {

  def compareMetadata(
      standalone: io.delta.standalone.actions.Metadata,
      oss: org.apache.spark.sql.delta.actions.Metadata): Unit = {

    assert(standalone.getId == oss.id)
    assert(standalone.getName == oss.name)
    assert(standalone.getDescription == oss.description)
    compareFormat(standalone.getFormat, oss.format)
    assert(standalone.getSchema.toJson == oss.schemaString)
    assert(standalone.getPartitionColumns.asScala == oss.partitionColumns)
    assert(standalone.getConfiguration.asScala == oss.configuration)
    assert(standalone.getCreatedTime.isPresent == oss.createdTime.isDefined)
    if (oss.createdTime.isDefined) {
      assert(standalone.getCreatedTime.get == oss.createdTime.get)
    }
  }

  def compareFormat(
      standalone: io.delta.standalone.actions.Format,
      oss: org.apache.spark.sql.delta.actions.Format): Unit = {

    assert(standalone.getProvider == oss.provider)
    assert(standalone.getOptions.asScala == oss.options)
  }

  def compareCommitInfo(
      standalone: io.delta.standalone.actions.CommitInfo,
      oss: org.apache.spark.sql.delta.actions.CommitInfo): Unit = {

  }
}
