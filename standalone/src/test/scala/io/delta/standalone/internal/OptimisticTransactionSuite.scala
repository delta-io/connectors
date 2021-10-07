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

package io.delta.standalone.internal

import java.util.Collections

import scala.collection.JavaConverters._

import io.delta.standalone.actions.{AddFile => AddFileJ, Metadata => MetadataJ, SetTransaction => SetTransactionJ, RemoveFile => RemoveFileJ}
import io.delta.standalone.expressions.{EqualTo, Literal}
import io.delta.standalone.types.{IntegerType, StructField, StructType}

class OptimisticTransactionSuite extends OptimisticTransactionSuiteBase {
  private val addA = new AddFileJ("a", Collections.emptyMap(), 1, 1, true, null, null)
  private val addB = new AddFileJ("b", Collections.emptyMap(), 1, 1, true, null, null)

  /* ************************** *
   * Allowed concurrent actions *
   * ************************** */

  check(
    "append / append",
    conflicts = false,
    reads = Seq(
      t => t.metadata()
    ),
    concurrentWrites = Seq(
      addA),
    actions = Seq(
      addB))

  check(
    "disjoint txns",
    conflicts = false,
    reads = Seq(
      t => t.txnVersion("t1")
    ),
    concurrentWrites = Seq(
      new SetTransactionJ("t2", 0, java.util.Optional.of(1234L))),
    actions = Nil)

  {
    val schema = new StructType(Array(new StructField("x", new IntegerType())))

    check(
      "disjoint delete / read",
      conflicts = false,
      setup = Seq(
        MetadataJ.builder()
          .schema(schema)
          .partitionColumns(Seq("x").asJava)
          .build(),
        new AddFileJ("a", Map("x" -> "2").asJava, 1, 1, true, null, null)
      ),
      reads = Seq(
        t => t.markFilesAsRead(new EqualTo(schema.column("x"), Literal.of(1)))
      ),
      concurrentWrites = Seq(
        RemoveFileJ.builder("a").deletionTimestamp(4L).build()
      ),
      actions = Seq()
    )
  }


}
