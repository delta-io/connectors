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

package io.delta.standalone.internal.data

import java.util.Collections

import scala.collection.JavaConverters._

import com.github.mjakubowski84.parquet4s.{ParquetWriter, RowParquetRecord}
import io.delta.standalone.actions.{AddFile => AddFileJ}
import io.delta.standalone.types.{DataType, IntegerType, StructField}
import org.apache.hadoop.fs.Path
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, INT32, INT64}
import org.apache.parquet.schema.Type.Repetition.{OPTIONAL, REQUIRED}
import org.apache.parquet.schema.{MessageType, OriginalType, Types, Type => ParquetType}

object ParquetDataWriter {

  def sparkToParquetType(dataType: DataType, fieldName: String): ParquetType = dataType match {
    case _: IntegerType =>
      Types.primitive(INT32, REQUIRED).as(OriginalType.INT_32).named(fieldName)
    case _ =>
      throw new Exception(s"unsure how to cast spark type ${dataType.getTypeName} to parquet")
  }

  // should be iter
  def write(dataPath: Path, data: Seq[RowParquetRecordImpl]): java.util.List[AddFileJ] = {
    assert(data.nonEmpty)

    var schemaBuilder: Types.GroupBuilder[MessageType] = Types.buildMessage()

    data.head.getSchema.getFields.foreach { field =>
      val parquetTypeWithName = sparkToParquetType(field.getDataType, field.getName)
      schemaBuilder = schemaBuilder.addField(parquetTypeWithName)
    }

    implicit val schema: MessageType = schemaBuilder.named("schema")

    // TODO: there must be a proper way to do this
    val newDataFilePath =
      new Path(dataPath, java.util.UUID.randomUUID().toString).toString + ".parquet"

    val writer = ParquetWriter.writer[RowParquetRecord](newDataFilePath, ParquetWriter.Options())
    try {
      data.foreach { row => writer.write(row.record) }
    } finally {
      writer.close()
    }

    // TODO: size (B) written?
    val addFile =
      new AddFileJ(newDataFilePath, Collections.emptyMap(), 500, 100L, false, null, null)

    (addFile :: Nil).asJava
  }
}
