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

  private def testGetConstraints(configuration: Map[String, String],
      expectedConstraints: Seq[Constraint]): Unit = {
    val metadata = Metadata.builder().configuration(configuration.asJava).build()
    assert(metadata.getConstraints.asScala == expectedConstraints)
  }

  test("getConstraints") {
    // no constraints
    assert(Metadata.builder().build().getConstraints.isEmpty)

    // retrieve one check constraints
    testGetConstraints(
      Map(ConstraintImpl.getCheckConstraintKey("constraint1") -> "expression1"),
      Seq(ConstraintImpl("constraint1", "expression1"))
    )

    // retrieve two check constraints
    testGetConstraints(
      Map(
        ConstraintImpl.getCheckConstraintKey("constraint1") -> "expression1",
        ConstraintImpl.getCheckConstraintKey("constraint2") -> "expression2"
      ),
      Seq(ConstraintImpl("constraint1", "expression1"),
        ConstraintImpl("constraint2", "expression2"))
    )

    // check constraint key format
    testGetConstraints(
      Map(
        // should be retrieved, preserves expression case
        ConstraintImpl.getCheckConstraintKey("constraints") -> "EXPRESSION",
        ConstraintImpl.getCheckConstraintKey("delta.constraints") ->
          "expression0",
        ConstraintImpl.getCheckConstraintKey(ConstraintImpl.CHECK_CONSTRAINT_KEY_PREFIX) ->
          "expression1",
        // should not be retrieved since they don't have the "delta.constraints." prefix
        "constraint1" -> "expression1",
        "delta.constraint.constraint2" -> "expression2",
        "constraints.constraint3" -> "expression3",
        "DELTA.CONSTRAINTS.constraint4" -> "expression4",
        "deltaxconstraintsxname" -> "expression5"
      ),
      Seq(ConstraintImpl("constraints", "EXPRESSION"),
        ConstraintImpl("delta.constraints", "expression0"),
        ConstraintImpl(ConstraintImpl.CHECK_CONSTRAINT_KEY_PREFIX, "expression1"))
    )
  }

  test("addCheckConstraint") {
    // add a constraint
    var metadata = Metadata.builder().build().withCheckConstraint("name", "expression")
    assert(metadata.getConfiguration.get(ConstraintImpl.getCheckConstraintKey("name"))
      == "expression")

    // add an already existing constraint
    testException[IllegalArgumentException](
      metadata.withCheckConstraint("name", "expression2"),
      "Constraint 'name' already exists. Please remove the old constraint first.\n" +
        "Old constraint: expression"
    )

    // not-case sensitive
    testException[IllegalArgumentException](
      metadata.withCheckConstraint("NAME", "expression2"),
      "Constraint 'NAME' already exists. Please remove the old constraint first.\n" +
        "Old constraint: expression"
    )

    // stores constraint lower case in metadata.configuration
    metadata = Metadata.builder().build().withCheckConstraint("NAME", "expression")
    assert(metadata.getConfiguration.get(ConstraintImpl.CHECK_CONSTRAINT_KEY_PREFIX + "name")
      == "expression")

    // add constraint with name='delta.constraints.'
    metadata = Metadata.builder().build()
      .withCheckConstraint(ConstraintImpl.CHECK_CONSTRAINT_KEY_PREFIX, "expression")
    assert(metadata.getConfiguration
      .get(ConstraintImpl.getCheckConstraintKey(ConstraintImpl.CHECK_CONSTRAINT_KEY_PREFIX))
      == "expression")
  }

  test("removeCheckConstraint") {
    // remove a constraint
    val configuration = Map(ConstraintImpl.getCheckConstraintKey("name") -> "expression").asJava
    var metadata = Metadata.builder().configuration(configuration).build()
      .withoutCheckConstraint("name")
    assert(!metadata.getConfiguration.containsKey(ConstraintImpl.getCheckConstraintKey("name")))

    // remove a non-existent constraint
    val e = intercept[IllegalArgumentException](
      Metadata.builder().build().withoutCheckConstraint("name")
    ).getMessage
    assert(e.contains("Cannot drop nonexistent constraint 'name'"))

    // not-case sensitive
    metadata = Metadata.builder().configuration(configuration).build()
      .withoutCheckConstraint("NAME")
    assert(!metadata.getConfiguration.containsKey(
      ConstraintImpl.CHECK_CONSTRAINT_KEY_PREFIX + "name"))
    assert(!metadata.getConfiguration
      .containsKey(
        ConstraintImpl.CHECK_CONSTRAINT_KEY_PREFIX + "NAME"))

    // remove constraint with name='delta.constraints.'
    metadata = Metadata.builder()
      .configuration(
        Map(
          ConstraintImpl.getCheckConstraintKey(ConstraintImpl.CHECK_CONSTRAINT_KEY_PREFIX) ->
          "expression").asJava)
      .build()
      .withoutCheckConstraint(ConstraintImpl.CHECK_CONSTRAINT_KEY_PREFIX)
    assert(!metadata.getConfiguration.containsKey(
      ConstraintImpl.getCheckConstraintKey(ConstraintImpl.CHECK_CONSTRAINT_KEY_PREFIX)))
  }

  test("addCheckConstraint/removeCheckConstraint + getConstraints") {
    // add a constraint
    var metadata = Metadata.builder().build().withCheckConstraint("name", "expression")
    assert(metadata.getConstraints.asScala == Seq(ConstraintImpl("name", "expression")))

    // remove the constraint
    metadata = metadata.withoutCheckConstraint("name")
    assert(Metadata.builder().build().getConstraints.isEmpty)
  }

  test("check constraints: metadata-protocol compatibility checks") {
    val schema = new StructType(Array(new StructField("col1", new IntegerType(), true)))

    // cannot add a check constraint to a table with too low a protocol version
    withTempDir { dir =>
      val log = getDeltaLogWithMaxFeatureSupport(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      txn.asInstanceOf[OptimisticTransactionImpl].upgradeProtocolVersion(1, 2)
      val metadata = Metadata.builder().schema(schema).build()
        .withCheckConstraint("test", "col1 < 0")
      testException[RuntimeException](
        txn.commit(
          Iterable(metadata).asJava,
          new Operation(Operation.Name.MANUAL_UPDATE),
          "test-engine-info"
        ),
        "Feature checkConstraint requires at least minWriterVersion = 3"
      )
    }

    // can commit and retrieve check constraint for table with sufficient protocol version
    withTempDir { dir =>
      val log = getDeltaLogWithMaxFeatureSupport(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      txn.asInstanceOf[OptimisticTransactionImpl].upgradeProtocolVersion(1, 3)
      val metadata = Metadata.builder().schema(schema).build()
        .withCheckConstraint("test", "col1 < 0")
      txn.commit(
        Iterable(metadata).asJava,
        new Operation(Operation.Name.MANUAL_UPDATE),
        "test-engine-info"
      )
      assert(log.startTransaction().metadata().getConstraints.asScala ==
        Seq(ConstraintImpl("test", "col1 < 0")))
    }
  }
}
