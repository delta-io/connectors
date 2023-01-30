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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{GroupType, MessageType, PrimitiveType}
import org.apache.parquet.schema.LogicalTypeAnnotation._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.scalatest.FunSuite

import io.delta.standalone.{DeltaLog, Operation}
import io.delta.standalone.actions.{Metadata => MetadataJ}
import io.delta.standalone.types._

import io.delta.standalone.internal.actions.{AddCDCFile, AddFile, RemoveFile}
import io.delta.standalone.internal.util.FileNames
import io.delta.standalone.internal.util.TestUtils._


class CheckpointsSuite extends FunSuite {

  import scala.collection.JavaConverters._

  private val engineInfo = "test-engine-info"
  private val manualUpdate = new Operation(Operation.Name.MANUAL_UPDATE)

  private def getParquetSchema(checkpointPath: Path): MessageType = {
    val parquetReader = ParquetFileReader.open(
      HadoopInputFile.fromPath(checkpointPath, new Configuration()))
    try {
      val footer = parquetReader.getFooter
      footer.getFileMetaData.getSchema
    } finally {
      parquetReader.close()
    }
  }

  test("checkpoint does not contain CDC field or remove.tags") {
    withTempDir { tempDir =>

      val logPath = new Path(tempDir.getCanonicalPath, "_delta_log")
      val log = DeltaLog.forTable(new Configuration(), tempDir.getCanonicalPath)

      val txn = log.startTransaction()
      txn.updateMetadata(
        MetadataJ.builder()
          .schema(new StructType().add("x", new IntegerType()))
          .build())
      txn.commit(AddFile("foo", Map(), 0, 0, dataChange = true) :: Nil, manualUpdate, engineInfo)

      log.startTransaction().commit(
        AddFile("foo2", Map(), 0, 0, dataChange = true) ::
          RemoveFile("foo", Some(System.currentTimeMillis()),
            extendedFileMetadata = true, tags = Map("foo" -> "value")) ::
          AddCDCFile("changes", Map(), 0) :: Nil,
        manualUpdate, engineInfo
      )

      log.update()
      log.asInstanceOf[DeltaLogImpl].checkpoint()
      val checkpointPath = FileNames.checkpointFileSingular(logPath, log.snapshot.getVersion)
      val schema = getParquetSchema(checkpointPath)

      val expectedParentCols = Set("txn", "add", "remove", "metaData", "protocol")
      assert(schema.getFields.asScala.map(_.getName).toSet == expectedParentCols)
      assert(
        !schema.getType(Seq("remove"): _*).asInstanceOf[GroupType].containsField("tags"))
    }
  }

  val tablePropertiesWithStructAndJsonStats = Seq(
    Map(
      DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_JSON.key -> "true",
      DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.key -> "true"
    ),
    Map(
      DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.key -> "true"
    )
  )

  val tablePropertiesWithStructWithoutJsonStats = Seq(
    Map(
      DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_JSON.key -> "false",
      DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.key -> "true"
    )
  )

  val tablePropertiesWithoutStructWithJsonStats = Seq(
    Map(
      DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_JSON.key -> "true",
      DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.key -> "false"
    ),
    Map(
      DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.key -> "false"
    )
  )

  val tablePropertiesWithoutStats = Seq(
    Map(
      DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_JSON.key -> "false",
      DeltaConfigs.CHECKPOINT_WRITE_STATS_AS_STRUCT.key -> "false"
    )
  )

  /**
   * Checks that the checkpoint schema is as expected.
   * - Parent columns are "txn", "add", "remove", "metaData", "protocol"
   * - Add schema has "path", "partitionValues", "size", "modificationTime", "dataChange", "tags"
   *   and they are of the expected type
   * - The *add* schema *does not* contains any of the `unexpectedAddCols`
   * - The *overall* schema *does* contain all the `additionalCols` and are of the expected type
   *
   * - When `partitioned` = true and `customPartitionColumns` = None: a default partition column
   *   [part INT] will be used.
   */
  private def testCheckpointSchema(
      tableProperties: Map[String, String],
      partitioned: Boolean,
      additionalCols: Seq[(Array[String], PrimitiveType.PrimitiveTypeName)] = Seq.empty,
      unexpectedAddCols: Seq[String] = Seq.empty,
      customPartitionColumns: Option[Seq[(String, DataType)]] = None,
      customPartitionValues: Option[Map[String, String]] = None): Unit = {

    // If customPartitionColumns is provided, customPartitionValues must be provided
    assert((customPartitionColumns.isEmpty && customPartitionValues.isEmpty) ||
        (customPartitionColumns.nonEmpty && customPartitionValues.nonEmpty))
    // If customPartitionColumns is provided, partitioned = true
    assert(customPartitionColumns.isEmpty || partitioned)

    withTempDir { tempDir =>

      val (partCols, partitionValues) = if (partitioned) {
        customPartitionColumns.getOrElse(Seq(("part", new IntegerType()))) ->
          customPartitionValues.getOrElse(Map("part" -> "0"))
      } else {
        (Seq.empty, Map.empty[String, String])
      }
      val structType = new StructType(
        Array(new StructField("x", new IntegerType())) ++
          partCols.map(c => new StructField(c._1, c._2))
      )

      val log = DeltaLog.forTable(new Configuration(), tempDir.getCanonicalPath)
      val txn = log.startTransaction()
      txn.updateMetadata(MetadataJ.builder()
        .schema(structType)
        .partitionColumns(partCols.map(_._1).asJava)
        .configuration(tableProperties.asJava)
        .build()
      )
      txn.commit(AddFile("foo", partitionValues, 0, 0, dataChange = true, stats = "") :: Nil,
        manualUpdate, engineInfo)
      log.update()
      log.asInstanceOf[DeltaLogImpl].checkpoint()

      val checkpointPath = FileNames.checkpointFileSingular(
        new Path(tempDir.getCanonicalPath, "_delta_log"), log.snapshot.getVersion)
      val schema = getParquetSchema(checkpointPath)

      // Check the parent schema
      val expectedParentCols = Set("txn", "add", "remove", "metaData", "protocol")
      assert(schema.getFields.asScala.map(_.getName).toSet == expectedParentCols)

      // Check the add schema
      val addType = schema.getType(Seq("add"): _*)
      assert(addType.isInstanceOf[GroupType],
        s"add column should be of group type but is: $addType")
      val addGroup = addType.asInstanceOf[GroupType]

      // These are of the form (name, LogicalTypeAnnotation, PrimitiveTypeName) since some columns
      // either aren't a primitive type, or don't have a LogicalTypeAnnotation (i.e. dataChange)
      val requiredAddCols = Seq(
        ("path", Some(stringType()), Some(BINARY)),
        ("partitionValues", Some(mapType()), None),
        ("size", Some(intType(64, true)), Some(INT64)),
        ("modificationTime", Some(intType(64, true)), Some(INT64)),
        ("dataChange", None, Some(BOOLEAN)),
        ("tags", Some(mapType()), None)
      )

      // Check for the expected required add columns
      requiredAddCols.foreach {
        case (expectedField, expectedLogicalType, expectedPrimitiveType) =>

        assert(addGroup.containsField(expectedField))
        // Check the data type
        assert(expectedLogicalType.forall(
          _ == addGroup.getType(expectedField).getLogicalTypeAnnotation))
        assert(expectedPrimitiveType.forall(
          _ == addGroup.getType(expectedField).asPrimitiveType().getPrimitiveTypeName))
      }

      // Check that the unexpectedAddCols do not exist
      unexpectedAddCols.foreach { colName =>
        assert(!addGroup.containsField(colName))
      }

      // Check for the existence and datatype of additionalCols
      additionalCols.foreach { case (path, expectedDataType) =>
        assert(schema.containsPath(path))
        assert(expectedDataType ==
          schema.getColumnDescription(path).getPrimitiveType.getPrimitiveTypeName)
      }
    }
  }

  test("checkpoint with struct and json stats - unpartitioned") {
    tablePropertiesWithStructAndJsonStats.foreach { tblProperties =>
      testCheckpointSchema(
        tblProperties,
        partitioned = false,
        additionalCols = Seq((Array("add", "stats"), BINARY)),
        unexpectedAddCols = Seq("partitionValues_parsed")
      )
    }
  }

  test("checkpoint with struct and without json stats - unpartitioned") {
    tablePropertiesWithStructWithoutJsonStats.foreach { tblProperties =>
      testCheckpointSchema(
        tblProperties,
        partitioned = false,
        unexpectedAddCols = Seq("stats", "partitionValues_parsed")
      )
    }
  }

  test("checkpoint without struct and with json stats - unpartitioned") {
    tablePropertiesWithoutStructWithJsonStats.foreach { tblProperties =>
      testCheckpointSchema(
        tblProperties,
        partitioned = false,
        additionalCols = Seq((Array("add", "stats"), BINARY)),
        unexpectedAddCols = Seq("partitionValues_parsed")
      )
    }
  }

  test("checkpoint without struct and without json stats - unpartitioned") {
    tablePropertiesWithoutStats.foreach { tblProperties =>
      testCheckpointSchema(
        tblProperties,
        partitioned = false,
        unexpectedAddCols = Seq("stats", "partitionValues_parsed")
      )
    }
  }

  test("checkpoint with struct and json stats - partitioned") {
    tablePropertiesWithStructAndJsonStats.foreach { tblProperties =>
      testCheckpointSchema(
        tblProperties,
        partitioned = true,
        additionalCols = Seq(
          (Array("add", "stats"), BINARY),
          (Array("add", "partitionValues_parsed", "part"), INT32))
      )
    }
  }

  test("checkpoint with struct and without json stats - partitioned") {
    tablePropertiesWithStructWithoutJsonStats.foreach { tblProperties =>
      testCheckpointSchema(
        tblProperties,
        partitioned = true,
        additionalCols = Seq((Array("add", "partitionValues_parsed", "part"), INT32)),
        unexpectedAddCols = Seq("stats")
      )
    }
  }

  test("checkpoint without struct and with json stats - partitioned") {
    tablePropertiesWithoutStructWithJsonStats.foreach { tblProperties =>
      testCheckpointSchema(
        tblProperties,
        partitioned = true,
        additionalCols = Seq((Array("add", "stats"), BINARY)),
        unexpectedAddCols = Seq("partitionValues_parsed")
      )
    }
  }

  test("checkpoint without struct and without json stats - partitioned") {
    tablePropertiesWithoutStats.foreach { tblProperties =>
      testCheckpointSchema(
        tblProperties,
        partitioned = true,
        unexpectedAddCols = Seq("stats", "partitionValues_parsed")
      )
    }
  }

  test("special characters") {
    val weirdName1 = "part%!@#_$%^&*-"
    val weirdName2 = "part?_.+<>|/"

    tablePropertiesWithStructAndJsonStats.foreach { tblProperties =>
      testCheckpointSchema(
        tblProperties,
        partitioned = true,
        additionalCols = Seq(
          (Array("add", "stats"), BINARY),
          (Array("add", "partitionValues_parsed", weirdName1), INT32),
          (Array("add", "partitionValues_parsed", weirdName2), INT32)
        ),
        customPartitionColumns =
          Some(Seq((weirdName1, new IntegerType()), (weirdName2, new IntegerType()))),
        customPartitionValues = Some(Map(weirdName1 -> "0", weirdName2 -> "0"))
      )
    }
  }

  test("timestamps as partition values") {
    tablePropertiesWithStructAndJsonStats.foreach { tblProperties =>
      testCheckpointSchema(
        tblProperties,
        partitioned = true,
        additionalCols = Seq(
          (Array("add", "stats"), BINARY),
          (Array("add", "partitionValues_parsed", "time"), INT96)
        ),
        customPartitionColumns = Some(Seq(("time", new TimestampType()))),
        customPartitionValues = Some(Map("time" -> "2012-12-31 16:00:10.011"))
      )
    }
  }

  test("null partition value") {
    tablePropertiesWithStructAndJsonStats.foreach { tblProperties =>
      testCheckpointSchema(
        tblProperties,
        partitioned = true,
        additionalCols = Seq(
          (Array("add", "stats"), BINARY),
          (Array("add", "partitionValues_parsed", "part1"), INT32),
          (Array("add", "partitionValues_parsed", "part2"), BINARY)
        ),
        customPartitionColumns =
          Some(Seq(("part1", new IntegerType()), ("part2", new StringType()))),
        customPartitionValues = Some(Map("part1" -> null, "part2" -> null))
      )
    }
  }

  test("all partition data types") {
    tablePropertiesWithStructAndJsonStats.foreach { tblProperties =>
      testCheckpointSchema(
        tblProperties,
        partitioned = true,
        additionalCols = Seq(
          (Array("add", "stats"), BINARY),
          (Array("add", "partitionValues_parsed", "string"), BINARY),
          (Array("add", "partitionValues_parsed", "long"), INT64),
          (Array("add", "partitionValues_parsed", "int"), INT32),
          (Array("add", "partitionValues_parsed", "short"), INT32),
          (Array("add", "partitionValues_parsed", "byte"), INT32),
          (Array("add", "partitionValues_parsed", "float"), FLOAT),
          (Array("add", "partitionValues_parsed", "double"), DOUBLE),
          (Array("add", "partitionValues_parsed", "decimal"), FIXED_LEN_BYTE_ARRAY),
          (Array("add", "partitionValues_parsed", "boolean"), BOOLEAN),
          (Array("add", "partitionValues_parsed", "binary"), BINARY),
          (Array("add", "partitionValues_parsed", "date"), INT32),
          (Array("add", "partitionValues_parsed", "timestamp"), INT96)
        ),
        customPartitionColumns = Some(Seq(
          ("string", new StringType()),
          ("long", new LongType()),
          ("int", new IntegerType()),
          ("short", new ShortType()),
          ("byte", new ByteType()),
          ("float", new FloatType()),
          ("double", new DoubleType()),
          ("decimal", new DecimalType(10, 0)),
          ("boolean", new BooleanType()),
          ("binary", new BinaryType()),
          ("date", new DateType()),
          ("timestamp", new TimestampType())
        )),
        customPartitionValues = Some(Map(
          "string" -> "foo",
          "long" -> "0",
          "int" -> "0",
          "short" -> "0",
          "byte" -> "0",
          "float" -> "0.1",
          "double" -> "0.1",
          "decimal" -> "1.0",
          "boolean" -> "true",
          "binary" -> "\\u0001",
          "date" -> "2000-01-01",
          "timestamp" -> "2000-01-01 00:00:00"
        ))
      )
    }
  }
}
