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

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import io.delta.standalone.{Constraint, Operation}
import io.delta.standalone.actions.Metadata
import io.delta.standalone.types.{IntegerType, StructField, StructType}

import io.delta.standalone.internal.util.TestUtils._

class DeltaConstraintsSuite extends FunSuite {

  // todo: should we add a test-suite trait storing helpers like this?
  private def testException[T <: Throwable](f: => Any, messageContains: String)
      (implicit manifest: Manifest[T]) = {
    val e = intercept[T]{
      f
    }.getMessage
    assert(e.contains(messageContains))
  }

  def testGetConstraints(configuration: Map[String, String],
      expectedConstraints: Seq[Constraint]): Unit = {
    val metadata = Metadata.builder().configuration(configuration.asJava).build()
    assert(metadata.getConstraints.asScala == expectedConstraints)
  }

  test("getConstraints") {
    // no constraints
    assert(Metadata.builder().build().getConstraints.isEmpty)

    // retrieve one check constraints
    testGetConstraints(
      Map("delta.constraints.constraint1" -> "expression1"),
      Seq(new Constraint("constraint1", "expression1"))
    )

    // retrieve two check constraints
    testGetConstraints(
      Map(
        "delta.constraints.constraint1" -> "expression1",
        "delta.constraints.constraint2" -> "expression2"
      ),
      Seq(new Constraint("constraint1", "expression1"),
        new Constraint("constraint2", "expression2"))
    )

    // check constraint key format
    testGetConstraints(
      Map(
        // should be retrieved, preserves expression case
        "delta.constraints.constraints" -> "EXPRESSION3",
        // should not be retrieved
        "constraint1" -> "expression1",
        "delta.constraint.constraint2" -> "expression2",
        "constraints.constraint3" -> "expression3",
        "DELTA.CONSTRAINTS.constraint4" -> "expression4"
      ),
      Seq(new Constraint("constraints", "EXPRESSION3"))
    )
  }

  test("addCheckConstraint") {
    // add a constraint
    var metadata = Metadata.builder().build().addCheckConstraint("name", "expression")
    assert(metadata.getConfiguration.get("delta.constraints.name").contains("expression"))

    // add an already existing constraint
    testException[IllegalArgumentException](
      metadata.addCheckConstraint("name", "expression2"),
      "Constraint 'name' already exists. Please remove the old constraint first.\n" +
        "Old constraint: expression"
    )

    // not-case sensitive
    testException[IllegalArgumentException](
      metadata.addCheckConstraint("NAME", "expression2"),
      "Constraint 'NAME' already exists. Please remove the old constraint first.\n" +
        "Old constraint: expression"
    )

    // stores constraint lower case in metadata.configuration
    metadata = Metadata.builder().build().addCheckConstraint("NAME", "expression")
    assert(metadata.getConfiguration.get("delta.constraints.name").contains("expression"))
  }

  test("removeCheckConstraint") {
    // remove a constraint
    val configuration = Map("delta.constraints.name" -> "expression").asJava
    var metadata = Metadata.builder().configuration(configuration).build()
      .removeCheckConstraint("name")
    assert(!metadata.getConfiguration.containsKey("delta.constraints.name"))

    // remove a non-existent constraint
    val e = intercept[IllegalArgumentException](
      metadata.removeCheckConstraint("name")
    ).getMessage
    assert(e.contains("Cannot drop nonexistent constraint 'name'"))

    // not-case sensitive
    metadata = Metadata.builder().configuration(configuration).build()
      .removeCheckConstraint("NAME")
    assert(!metadata.getConfiguration.containsKey("delta.constraints.name"))
  }

  test("addCheckConstraint/removeCheckConstraint + getConstraints") {
    // add a constraint
    var metadata = Metadata.builder().build().addCheckConstraint("name", "expression")
    assert(metadata.getConstraints.asScala == Seq(new Constraint("name", "expression")))

    // remove the constraint
    metadata = metadata.removeCheckConstraint("name")
    assert(Metadata.builder().build().getConstraints.isEmpty)
  }

  test("check constraints + protocol version checks") {
    val schema = new StructType(Array(new StructField("col1", new IntegerType(), true)))

    // fail correctly when too low a protocol version
    withTempDir { dir =>
      val log = DeltaLogImpl.forTable(new Configuration(), dir.getCanonicalPath, true)
      val txn = log.startTransactionWithInitialProtocolVersion(1, 2)
      val metadata = Metadata.builder().schema(schema).build()
        .addCheckConstraint("test", "col1 < 0")
      testException[RuntimeException](
        txn.commit(
          Iterable(metadata).asJava,
          new Operation(Operation.Name.MANUAL_UPDATE),
          "test-engine-info"
        ),
        "Feature checkConstraint requires at least writer version 3 but current " +
          "table protocol is (1,2)"
      )
    }

    // can commit and retrieve check constraint with sufficient protocol version
    withTempDir { dir =>
      val log = DeltaLogImpl.forTable(new Configuration(), dir.getCanonicalPath, true)
      val txn = log.startTransactionWithInitialProtocolVersion(1, 3)
      val metadata = Metadata.builder().schema(schema).build()
        .addCheckConstraint("test", "col1 < 0")
      txn.commit(
        Iterable(metadata).asJava,
        new Operation(Operation.Name.MANUAL_UPDATE),
        "test-engine-info"
      )
      assert(log.startTransaction().metadata().getConstraints.asScala ==
        Seq(new Constraint("test", "col1 < 0")))
    }
  }
}
