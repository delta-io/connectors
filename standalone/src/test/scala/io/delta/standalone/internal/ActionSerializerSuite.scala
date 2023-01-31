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

import org.scalatest.FunSuite

import io.delta.standalone.types.{IntegerType, StructType}

import io.delta.standalone.internal.actions._

class ActionSerializerSuite extends FunSuite {

  roundTripCompare("Add",
    AddFile("test", Map.empty, 1, 1, dataChange = true))
  roundTripCompare("Add with partitions",
    AddFile("test", Map("a" -> "1"), 1, 1, dataChange = true))
  roundTripCompare("Add with stats",
    AddFile("test", Map.empty, 1, 1, dataChange = true, stats = "stats"))
  roundTripCompare("Add with tags",
    AddFile("test", Map.empty, 1, 1, dataChange = true, tags = Map("a" -> "1")))
  roundTripCompare("Add with empty tags",
    AddFile("test", Map.empty, 1, 1, dataChange = true, tags = Map.empty))

  test("partitionValues_parsed is not serialized") {
    val addFile = AddFile("foo", Map("x" -> "b"), 0, 0, true)
      .withPartitionValuesParsed(new StructType().add("x", new IntegerType))
    assert(!addFile.json.contains("partitionValues_parsed"))
    assert(Action.fromJson(addFile.json).asInstanceOf[AddFile].partitionValues_parsed == null)
  }

  roundTripCompare("Remove",
    RemoveFile("test", Some(2)))

  test("AddFile tags") {
    val action1 =
      AddFile(
        path = "a",
        partitionValues = Map.empty,
        size = 1,
        modificationTime = 2,
        dataChange = false,
        stats = null,
        tags = Map("key1" -> "val1", "key2" -> "val2"))
    val json1 =
      """{
        |  "add": {
        |    "path": "a",
        |    "partitionValues": {},
        |    "size": 1,
        |    "modificationTime": 2,
        |    "dataChange": false,
        |    "tags": {
        |      "key1": "val1",
        |      "key2": "val2"
        |    }
        |  }
        |}""".stripMargin
    assert(action1 === Action.fromJson(json1))
    assert(action1.json === json1.replaceAll("\\s", ""))

    val json2 =
      """{
        |  "add": {
        |    "path": "a",
        |    "partitionValues": {},
        |    "size": 1,
        |    "modificationTime": 2,
        |    "dataChange": false,
        |    "tags": {}
        |  }
        |}""".stripMargin
    val action2 =
      AddFile(
        path = "a",
        partitionValues = Map.empty,
        size = 1,
        modificationTime = 2,
        dataChange = false,
        stats = null,
        tags = Map.empty)
    assert(action2 === Action.fromJson(json2))
    assert(action2.json === json2.replaceAll("\\s", ""))
  }

  test("remove file deserialization") {
    val removeJson = RemoveFile("a", Some(2L)).json
    assert(removeJson.contains(""""deletionTimestamp":2"""))
    assert(!removeJson.contains("""delTimestamp"""))
    val json1 = """{"remove":{"path":"a","deletionTimestamp":2,"dataChange":true}}"""
    val json2 = """{"remove":{"path":"a","dataChange":false}}"""
    assert(Action.fromJson(json1) === RemoveFile("a", Some(2L), dataChange = true))
    assert(Action.fromJson(json2) === RemoveFile("a", None, dataChange = false))
  }

  roundTripCompare("SetTransaction",
    SetTransaction("a", 1, Some(1234L)))

  roundTripCompare("SetTransaction without lastUpdated",
    SetTransaction("a", 1, None))

  roundTripCompare("MetaData",
    Metadata(
      "id",
      "table",
      "testing",
      Format("parquet", Map.empty),
      new StructType().toJson,
      Seq("a")))

  test("deserialization of CommitInfo without tags") {
    val expectedCommitInfo = CommitInfo(
      time = 123L,
      operation = "CONVERT",
      operationParameters = Map.empty,
      commandContext = Map.empty,
      readVersion = Some(23),
      isolationLevel = Some("SnapshotIsolation"),
      isBlindAppend = Some(true),
      operationMetrics = Some(Map("m1" -> "v1", "m2" -> "v2")),
      userMetadata = Some("123"),
      engineInfo = None)

    // json of commit info actions without tag or engineInfo field
    val json1 =
      """{"commitInfo":{"timestamp":123,"operation":"CONVERT",""" +
        """"operationParameters":{},"readVersion":23,""" +
        """"isolationLevel":"SnapshotIsolation","isBlindAppend":true,""" +
        """"operationMetrics":{"m1":"v1","m2":"v2"},"userMetadata":"123"}}""".stripMargin
    assert(Action.fromJson(json1) === expectedCommitInfo)
  }

  private def roundTripCompare(name: String, actions: Action*) = {
    test(name) {
      val asJson = actions.map(_.json)
      val asObjects = asJson.map(Action.fromJson)

      assert(actions === asObjects)
    }
  }
}
