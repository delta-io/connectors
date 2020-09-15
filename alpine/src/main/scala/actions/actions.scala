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

package main.scala.actions

import java.net.URI
import java.sql.Timestamp

import scala.main.util.JsonUtils

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude, JsonRawValue}
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}

object Action {
  val readerVersion = 1
  val writerVersion = 2
  val protocolVersion: Protocol = Protocol(readerVersion, writerVersion)

  def fromJson(json: String): Action = {
    JsonUtils.mapper.readValue[SingleAction](json).unwrap
  }
}

sealed trait Action {
  def wrap: SingleAction

  def json: String = JsonUtils.toJson(wrap)
}

case class Protocol(
  minReaderVersion: Int = Action.readerVersion,
  minWriterVersion: Int = Action.writerVersion) extends Action {
  override def wrap: SingleAction = SingleAction(protocol = this)

  @JsonIgnore
  def simpleString: String = s"($minReaderVersion,$minWriterVersion)"
}

case class SetTransaction(
  appId: String,
  version: Long,
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  lastUpdated: Option[Long]) extends Action {
  override def wrap: SingleAction = SingleAction(txn = this)
}

sealed trait FileAction extends Action {
  val path: String
  val dataChange: Boolean
  @JsonIgnore
  lazy val pathAsUri: URI = new URI(path)
}

case class AddFile(
  path: String,
  @JsonInclude(JsonInclude.Include.ALWAYS)
  partitionValues: Map[String, String],
  size: Long,
  modificationTime: Long,
  dataChange: Boolean,
  @JsonRawValue
  stats: String = null,
  tags: Map[String, String] = null) extends FileAction {
  require(path.nonEmpty)

  override def wrap: SingleAction = SingleAction(add = this)

  def remove: RemoveFile = removeWithTimestamp()

  def removeWithTimestamp(
    timestamp: Long = System.currentTimeMillis(),
    dataChange: Boolean = true): RemoveFile = {
    // scalastyle:off
    RemoveFile(path, Some(timestamp), dataChange)
    // scalastyle:on
  }
}

case class RemoveFile(
  path: String,
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  deletionTimestamp: Option[Long],
  dataChange: Boolean = true) extends FileAction {
  override def wrap: SingleAction = SingleAction(remove = this)

  @JsonIgnore
  val delTimestamp: Long = deletionTimestamp.getOrElse(0L)
}

case class Format(
  provider: String = "parquet",
  options: Map[String, String] = Map.empty)

case class Metadata(
  id: String = java.util.UUID.randomUUID().toString,
  name: String = null,
  description: String = null,
  format: Format = Format(),
  schemaString: String = null,
  partitionColumns: Seq[String] = Nil,
  configuration: Map[String, String] = Map.empty,
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  createdTime: Option[Long] = Some(System.currentTimeMillis())) extends Action {

  override def wrap: SingleAction = SingleAction(metaData = this)
}

trait CommitMarker {
  /** Get the timestamp of the commit as millis after the epoch. */
  def getTimestamp: Long
  /** Return a copy object of this object with the given timestamp. */
  def withTimestamp(timestamp: Long): CommitMarker
  /** Get the version of the commit. */
  def getVersion: Long
}

case class JobInfo(
  jobId: String,
  jobName: String,
  runId: String,
  jobOwnerId: String,
  triggerType: String)

object JobInfo {
  def fromContext(context: Map[String, String]): Option[JobInfo] = {
    context.get("jobId").map { jobId =>
      JobInfo(
        jobId,
        context.get("jobName").orNull,
        context.get("runId").orNull,
        context.get("jobOwnerId").orNull,
        context.get("jobTriggerType").orNull)
    }
  }
}

case class NotebookInfo(notebookId: String)

case class CommitInfo(
  // The commit version should be left unfilled during commit(). When reading a delta file, we can
  // infer the commit version from the file name and fill in this field then.
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  version: Option[Long],
  timestamp: Timestamp,
  userId: Option[String],
  userName: Option[String],
  operation: String,
  @JsonSerialize(using = classOf[JsonMapSerializer])
  operationParameters: Map[String, String],
  job: Option[JobInfo],
  notebook: Option[NotebookInfo],
  clusterId: Option[String],
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  readVersion: Option[Long],
  isolationLevel: Option[String],
  /** Whether this commit has blindly appended without caring about existing files */
  isBlindAppend: Option[Boolean],
  operationMetrics: Option[Map[String, String]],
  userMetadata: Option[String]) extends Action with CommitMarker {
  override def wrap: SingleAction = SingleAction(commitInfo = this)

  override def withTimestamp(timestamp: Long): CommitInfo = {
    this.copy(timestamp = new Timestamp(timestamp))
  }

  override def getTimestamp: Long = timestamp.getTime
  @JsonIgnore
  override def getVersion: Long = version.get
}

case class SingleAction(
  txn: SetTransaction = null,
  add: AddFile = null,
  remove: RemoveFile = null,
  metaData: Metadata = null,
  protocol: Protocol = null,
  commitInfo: CommitInfo = null) {

  def unwrap: Action = {
    if (add != null) {
      add
    } else if (remove != null) {
      remove
    } else if (metaData != null) {
      metaData
    } else if (txn != null) {
      txn
    } else if (protocol != null) {
      protocol
    } else if (commitInfo != null) {
      commitInfo
    } else {
      null
    }
  }
}

class JsonMapSerializer extends JsonSerializer[Map[String, String]] {
  def serialize(
    parameters: Map[String, String],
    jgen: JsonGenerator,
    provider: SerializerProvider): Unit = {
    jgen.writeStartObject()
    parameters.foreach { case (key, value) =>
      if (value == null) {
        jgen.writeNullField(key)
      } else {
        jgen.writeFieldName(key)
        // Write value as raw data, since it's already JSON text
        jgen.writeRawValue(value)
      }
    }
    jgen.writeEndObject()
  }
}