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

import scala.collection.mutable

import org.scalatest.FunSuite

import io.delta.standalone.exceptions.ColumnMappingUnsupportedException
import io.delta.standalone.types.{ArrayType, DataType, FieldMetadata, FloatType, IntegerType, LongType, MapType, StringType, StructField, StructType}
import io.delta.standalone.types.FieldMetadata.builder

import io.delta.standalone.internal.DeltaColumnMapping._
import io.delta.standalone.internal.actions.{Metadata, Protocol}
import io.delta.standalone.internal.util.{SchemaMergingUtils, SchemaUtils}


class DeltaColumnMappingSuite extends FunSuite {

  // Set the column mapping mode to 'id' or unknown and expect unsupported error
  test("unsupported column mapping mode") {
    val ex = intercept[ColumnMappingUnsupportedException] {
      verifyAndUpdateMetadataChange(
        oldProtocol = protocol(1, 2),
        oldMetadata = metadata(simpleSchema, Map.empty),
        newMetadata = metadata(simpleSchema, withMode(Map.empty, "id")),
        isCreatingNewTable = false)
    }
    assert(ex.getMessage contains "The column mapping mode `id` is not supported for this " +
        "Delta version. Please upgrade if you want to use this mode.")
  }

  test("unsupported mode change: example: name to none") {
    val ex = intercept[ColumnMappingUnsupportedException] {
      verifyAndUpdateMetadataChange(
        oldProtocol = protocol(1, 2),
        oldMetadata = metadata(simpleSchema, withMode(Map.empty, "name")),
        newMetadata = metadata(simpleSchema, withMode(Map.empty, "none")),
        isCreatingNewTable = false)
    }
    assert(ex.getMessage contains
        "Changing column mapping mode from name to none is not supported.")
  }

  test("unsupported protocol version in existing table and no protocol upgrade in new metadata") {
    val ex = intercept[ColumnMappingUnsupportedException] {
      verifyAndUpdateMetadataChange(
        oldProtocol = protocol(1, 2),
        oldMetadata = metadata(simpleSchema, Map.empty),
        newMetadata = metadata(simpleSchema, withMode(Map.empty, "name")),
        isCreatingNewTable = false)
    }
    assert(ex.getMessage contains "Your current table protocol version does not support " +
        "changing column mapping modes using delta.columnMapping.mode.")
    assert(ex.getMessage contains
        "Required Delta protocol version for column mapping: Protocol(2,5)")
    assert(ex.getMessage contains
        "Your table's current Delta protocol version: Protocol(1,2)")
  }

  test("unsupported protocol version in existing table and in new metadata") {
    val ex = intercept[ColumnMappingUnsupportedException] {
      verifyAndUpdateMetadataChange(
        oldProtocol = protocol(1, 2),
        oldMetadata = metadata(simpleSchema, Map.empty),
        newMetadata = metadata(simpleSchema,
                               withProtocol(withMode(Map.empty, "name"), readerV = 1, writerV = 5)),
        isCreatingNewTable = false)
    }
    assert(ex.getMessage contains "Your current table protocol version does not support " +
        "changing column mapping modes using delta.columnMapping.mode.")
    assert(ex.getMessage contains
        "Required Delta protocol version for column mapping: Protocol(2,5)")
    assert(ex.getMessage contains
        "Your table's current Delta protocol version: Protocol(1,2)")
  }

  test("change mode=name on an existing table") {
    val updatedMetadata = verifyAndUpdateMetadataChange(
      oldProtocol = protocol(1, 2),
      oldMetadata = metadata(simpleSchema, Map.empty),
      newMetadata = metadata(simpleSchema,
                            withProtocol(withMode(Map.empty, "name"), readerV = 2, writerV = 5)),
      isCreatingNewTable = false)

    val (actIds, actPhyNames) = extractIdsAndPhyNames(updatedMetadata.schema)
    assert(actIds === simpleSchemaExpIds)
    assert(actPhyNames === simpleSchemaExpPhyNames)
  }

  test("change mode=name on an existing table - complex types") {
    val updatedMetadata = verifyAndUpdateMetadataChange(
      oldProtocol = protocol(1, 2),
      oldMetadata = metadata(complexSchema, Map.empty),
      newMetadata = metadata(complexSchema,
                             withProtocol(withMode(Map.empty, "name"), readerV = 2, writerV = 5)),
      isCreatingNewTable = false)

    val (actIds, actPhyNames) = extractIdsAndPhyNames(updatedMetadata.schema)
    assert(actIds === complexSchemaExpIds)
    assert(actPhyNames === complexSchemaExpPhyNames)
  }

  test("change mode=name on an new table") {
    val updatedMetadata = verifyAndUpdateMetadataChange(
      oldProtocol = protocol(1, 2),
      oldMetadata = metadata(simpleSchema, Map.empty),
      newMetadata = metadata(simpleSchema,
                             withProtocol(withMode(Map.empty, "name"), readerV = 2, writerV = 5)),
      isCreatingNewTable = true)

    val (actIds, actPhyNames) = extractIdsAndPhyNames(updatedMetadata.schema)
    assert(actIds === simpleSchemaExpIds)
    actPhyNames.values.foreach(assertUUIDColumnName(_))
  }

  test("change mode=name on an new table - complex types") {
    val updatedMetadata = verifyAndUpdateMetadataChange(
      oldProtocol = protocol(1, 2),
      oldMetadata = metadata(complexSchema, Map.empty),
      newMetadata = metadata(complexSchema,
                             withProtocol(withMode(Map.empty, "name"), readerV = 2, writerV = 5)),
      isCreatingNewTable = true)

    val newSchema = updatedMetadata.schema
    val (actIds, actPhyNames) = extractIdsAndPhyNames(newSchema)
    assert(actIds === complexSchemaExpIds)
    actPhyNames.values.foreach(assertUUIDColumnName(_))
  }

  test("existing field metadata is preserved") {
    val schema = struct(
      field("a", new IntegerType)
          .withNewMetadata(builder().putString("test", "testValue").build()),
      field("b", new StringType)
          .withNewMetadata(builder().putString("test2", "testValue2").build()))

    val updatedMetadata = verifyAndUpdateMetadataChange(
      oldProtocol = protocol(1, 2),
      oldMetadata = metadata(schema, Map.empty),
      newMetadata = metadata(schema,
                             withProtocol(withMode(Map.empty, "name"), readerV = 2, writerV = 5)),
      isCreatingNewTable = true)

    val (actIds, actPhyNames) = extractIdsAndPhyNames(updatedMetadata.schema)
    assert(actIds === simpleSchemaExpIds)
    actPhyNames.values.foreach(assertUUIDColumnName(_))

    assert(updatedMetadata.schema.get("a").getMetadata.get("test") == "testValue")
    assert(updatedMetadata.schema.get("b").getMetadata.get("test2") == "testValue2")

    // expect three entries (two from column mapping and one existing one)
    assert(updatedMetadata.schema.get("a").getMetadata.getEntries.size() == 3)
    assert(updatedMetadata.schema.get("b").getMetadata.getEntries.size() == 3)
  }

  private def struct(fields: StructField *): StructType = new StructType(fields.toArray)

  private def map(keyType: DataType, valueType: DataType): MapType =
    new MapType(keyType, valueType, true)

  private def array(elementType: DataType): ArrayType = new ArrayType(elementType, true)

  private def field(name: String, dataType: DataType): StructField = new StructField(name, dataType)

  private def metadata(schema: StructType, conf: Map[String, String]): Metadata = {
    Metadata(
      schemaString = schema.toJson,
      configuration = conf
    )
  }

  private def protocol(readerVersion: Int, writerVersion: Int): Protocol =
    new Protocol(readerVersion, writerVersion)

  private def withMode(existingConf: Map[String, String], newMode: String): Map[String, String] = {
    val newConf = mutable.Map[String, String]() ++ existingConf
    newConf.put(DeltaConfigs.COLUMN_MAPPING_MODE.key, newMode)
    newConf.toMap
  }

  private def withProtocol(
      existingConf: Map[String, String], readerV: Int, writerV: Int): Map[String, String] = {
    val newConf = mutable.Map[String, String]() ++ existingConf
    newConf.put(Protocol.MIN_READER_VERSION_PROP, readerV.toString)
    newConf.put(Protocol.MIN_WRITER_VERSION_PROP, writerV.toString)
    newConf.toMap
  }

  private def assertColumnMappingMetadata(
      actualMetadata: FieldMetadata, expId: Long, expPhyName: String): Unit = {
    assert(actualMetadata.get(COLUMN_MAPPING_METADATA_ID_KEY) == expId)
    val actualPhyName = actualMetadata.get(COLUMN_MAPPING_PHYSICAL_NAME_KEY).toString
    // For new tables the physical column name is a UUID. For existing tables, we
    // try to keep the physical column name same as the one in the schema
    if (expPhyName == "UUID") {
      assertUUIDColumnName(actualPhyName)
    } else {
      assert(actualMetadata.get(COLUMN_MAPPING_PHYSICAL_NAME_KEY) == expPhyName)
    }
  }

  private def assertUUIDColumnName(physicalName: String): Unit =
    assert(physicalName.toString.startsWith("col-"),
      s"Physical UUID column name doesn't start with col-: $physicalName")

  private def extractIdsAndPhyNames(
      schema: StructType): (Map[Seq[String], Long], Map[Seq[String], String]) = {
    val actIds = mutable.Map[Seq[String], Long]()
    val actPhyNames = mutable.Map[Seq[String], String]()
    SchemaMergingUtils.transformColumns(schema)((path, field, _) => {
      val colPath = path :+ field.getName
      actIds.put(colPath, field.getMetadata.get(COLUMN_MAPPING_METADATA_ID_KEY).asInstanceOf[Long])
      actPhyNames.put(colPath, field.getMetadata.get(COLUMN_MAPPING_PHYSICAL_NAME_KEY).toString)
      field
    })
    (actIds.toMap, actPhyNames.toMap)
  }

  private val simpleSchema = struct(field("a", new IntegerType), field("b", new StringType))

  /** Expected column Id for test schema [[simpleSchema]] */
  private val simpleSchemaExpIds = Map(Seq("a") -> 1, Seq("b") -> 2)

  /** Expected physical column names for test schema [[simpleSchema]] */
  private val simpleSchemaExpPhyNames = Map(Seq("a") -> "a", Seq("b") -> "b")

  private val complexSchema = struct(
    field("i", new IntegerType),
    field("str", new StringType),
    field("m", map(new FloatType, new StringType)),
    field("ms", map(
      new FloatType,
      struct(field("msi", new IntegerType), field("msf", new FloatType))
      )
    ),
    field("a", array(new LongType)),
    field("s",
          struct(
            field("sa", new IntegerType),
            field("ss", struct(field("ssstr", new StringType)))
            )
          )
    )

  /** Expected column Id for test schema [[complexSchema]] */
  private val complexSchemaExpIds = Map(
    Seq("i") -> 1,
    Seq("str") -> 2,
    Seq("m") -> 3,
    Seq("ms") -> 4,
    Seq("ms", "value", "msi") -> 5,
    Seq("ms", "value", "msf") -> 6,
    Seq("a") -> 7,
    Seq("s") -> 8,
    Seq("s", "sa") -> 9,
    Seq("s", "ss") -> 10,
    Seq("s", "ss", "ssstr") -> 11
  )

  /** Expected physical column names for test schema [[complexSchema]] */
  private val complexSchemaExpPhyNames = Map(
    Seq("i") -> "i",
    Seq("str") -> "str",
    Seq("m") -> "m",
    Seq("ms") -> "ms",
    Seq("ms", "value", "msi") -> "msi",
    Seq("ms", "value", "msf") -> "msf",
    Seq("a") -> "a",
    Seq("s") -> "s",
    Seq("s", "sa") -> "sa",
    Seq("s", "ss") -> "ss",
    Seq("s", "ss", "ssstr") -> "ssstr"
  )
}
