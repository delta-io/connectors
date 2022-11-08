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
import io.delta.standalone.types.{ArrayType, FieldMetadata, IntegerType, MapType, StringType, StructField, StructType}

import io.delta.standalone.internal.actions.AddFile
import io.delta.standalone.internal.util.InvariantUtils
import io.delta.standalone.internal.util.TestUtils._

class DeltaConstraintsSuite extends FunSuite {

  private def testGetConstraints(
      configuration: Map[String, String] = Map.empty,
      schema: StructType = null,
      expectedConstraints: Seq[Constraint]): Unit = {

    val metadata = Metadata.builder().configuration(configuration.asJava).schema(schema).build()
    assert(expectedConstraints.toSet == metadata.getConstraints.asScala.toSet)
  }

  ///////////////////////////////////////////////////////////////////////////
  // CHECK constraints
  ///////////////////////////////////////////////////////////////////////////

  test("getConstraints with check constraints") {
    // no constraints
    assert(Metadata.builder().build().getConstraints.isEmpty)

    // retrieve one check constraints
    testGetConstraints(
      configuration = Map(ConstraintImpl.getCheckConstraintKey("constraint1") -> "expression1"),
      expectedConstraints = Seq(ConstraintImpl("constraint1", "expression1"))
    )

    // retrieve two check constraints
    testGetConstraints(
      configuration = Map(
        ConstraintImpl.getCheckConstraintKey("constraint1") -> "expression1",
        ConstraintImpl.getCheckConstraintKey("constraint2") -> "expression2"
      ),
      expectedConstraints = Seq(ConstraintImpl("constraint1", "expression1"),
        ConstraintImpl("constraint2", "expression2"))
    )

    // check constraint key format
    testGetConstraints(
      configuration = Map(
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
      expectedConstraints = Seq(ConstraintImpl("constraints", "EXPRESSION"),
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

  ///////////////////////////////////////////////////////////////////////////
  // column invariants
  ///////////////////////////////////////////////////////////////////////////

  private def fieldMetadataWithInvariant(expr: String): FieldMetadata = {
    FieldMetadata.builder()
      .putString(
        InvariantUtils.INVARIANTS_FIELD,
        InvariantUtils.PersistedExpression(expr).json)  // {"expression":{"expression":"$expr"}}
      .build()
  }

  test("getConstraints with column invariants") {

    // no column invariants
    testGetConstraints(
      schema = new StructType()
        .add("col1", new IntegerType())
        .add("col2", new IntegerType()),
      expectedConstraints = Seq.empty
    )

    // top-level column with a column invariant
    val structField1 = new StructField("col1", new IntegerType(), true,
      fieldMetadataWithInvariant("col1 < 3"))
    testGetConstraints(
      schema = new StructType().add(structField1),
      expectedConstraints = Seq(ConstraintImpl("EXPRESSION(col1 < 3)", "col1 < 3"))
    )

    // two top-level columns with column invariant
    val structField2 = new StructField("col2", new IntegerType(), true,
      fieldMetadataWithInvariant("col2 < 3"))
    testGetConstraints(
      schema = new StructType(Array(structField1, structField2)),
      expectedConstraints = Seq(ConstraintImpl("EXPRESSION(col1 < 3)", "col1 < 3"),
        ConstraintImpl("EXPRESSION(col2 < 3)", "col2 < 3"))
    )

    // nested column with a column invariant
    val nestedStructField = new StructField("col1", new IntegerType(), true,
      fieldMetadataWithInvariant("nested.col1 < 3"))
    testGetConstraints(
      schema = new StructType().add(
        new StructField(
          "nested",
          new StructType().add(nestedStructField)
        )),
      expectedConstraints = Seq(ConstraintImpl("EXPRESSION(nested.col1 < 3)", "nested.col1 < 3"))
    )

    // nested column + top-level column with column invariant
    testGetConstraints(
      schema = new StructType().add(
        new StructField(
          "nested",
          new StructType().add(nestedStructField),
          true,
          fieldMetadataWithInvariant("nested is not null")
        )),
      expectedConstraints = Seq(ConstraintImpl("EXPRESSION(nested.col1 < 3)", "nested.col1 < 3"),
        ConstraintImpl("EXPRESSION(nested is not null)", "nested is not null"))
    )

    // ignore constraints from Array<StructType> column
    val arrayStructField = new StructField(
      "array",
      new ArrayType(new StructType().add(structField1), true)
    )
    testGetConstraints(
      schema = new StructType().add(arrayStructField),
      expectedConstraints = Seq.empty
    )

    // ignore constraints from Map<StructType, StructType> column
    val mapStructField = new StructField(
      "map",
      new MapType(
        new StructType().add(structField1),
        new StructType().add(structField2),
        true
      )
    )
    testGetConstraints(
      schema = new StructType().add(mapStructField),
      expectedConstraints = Seq.empty
    )
  }

  test("misformatted invariant") {
    val expr = """{"expression":"col1 < 3"}"""
    val fieldMetadata = FieldMetadata.builder()
      .putString(InvariantUtils.INVARIANTS_FIELD, expr)
      .build()
    val schema = new StructType()
      .add(new StructField("col1", new IntegerType(), true, fieldMetadata))
    testException[IllegalStateException](
      Metadata.builder().schema(schema).build().getConstraints,
      s"Misformatted invariant: $expr"
    )
  }

  // TODO: unblock these tests once we allow writerVersion=1
  ignore("column-invariants: metadata-protocol compatibility checks") {
    val structField = new StructField(
      "col1",
      new IntegerType(),
      true,
      fieldMetadataWithInvariant("col1 > 3")
    )
    val schema = new StructType().add(structField)

    // cannot use column invariants with too low a protocol version
    withTempDir { dir =>
      val log = getDeltaLogWithMaxFeatureSupport(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      txn.asInstanceOf[OptimisticTransactionImpl].upgradeProtocolVersion(1, 1)
      val metadata = Metadata.builder().schema(schema).build()
      testException[RuntimeException](
        txn.commit(
          Iterable(metadata).asJava,
          new Operation(Operation.Name.MANUAL_UPDATE),
          "test-engine-info"
        ),
        "Feature columnInvariants requires at least writer version 2 but current " +
          "table protocol is (1,1)"
      )
    }

    // can use column invariants with writerVersion >=2
    withTempDir { dir =>
      val log = getDeltaLogWithMaxFeatureSupport(new Configuration(), dir.getCanonicalPath)
      val txn = log.startTransaction()
      txn.asInstanceOf[OptimisticTransactionImpl].upgradeProtocolVersion(1, 2)
      val metadata = Metadata.builder().schema(schema).build()
      txn.commit(
        Iterable(metadata).asJava,
        new Operation(Operation.Name.MANUAL_UPDATE),
        "test-engine-info"
      )
      assert(log.startTransaction().metadata().getConstraints.asScala ==
        Seq(ConstraintImpl("EXPRESSION(col1 > 3)", "col1 > 3")))
    }
  }

  test("cannot add or change a column invariant in the schema") {
    val structField = new StructField("col1", new IntegerType())
    val schemaWithNoInvariant = new StructType().add(structField)
    val schemaWithInvariant1 = new StructType().add(
      new StructField(structField.getName, structField.getDataType, structField.isNullable,
        fieldMetadataWithInvariant("col1 > 1"))
    )
    val schemaWithInvariant2 = new StructType().add(
      new StructField(structField.getName, structField.getDataType, structField.isNullable,
        fieldMetadataWithInvariant("col1 > 2"))
    )

    // These are formatted as (originalSchema, updatedSchema, shouldFail)
    // We will create a table with originalSchema in transaction 1, and in transaction 2 attempt to
    // change the schema to updatedSchema. shouldFail is whether we expect transaction 2 to fail
    Seq(
      (schemaWithNoInvariant, schemaWithInvariant1, true), // cannot add a column invariant
      (schemaWithInvariant1, schemaWithInvariant2, true), // cannot change a column invariant
      (schemaWithInvariant1, schemaWithNoInvariant, false) // we don't explicitly prevent removal
    ).foreach { case (originalSchema, updatedSchema, shouldFail) =>
      withTempDir { dir =>
        val log = getDeltaLogWithMaxFeatureSupport(new Configuration(), dir.getCanonicalPath)
        val txn1 = log.startTransaction()
        txn1.asInstanceOf[OptimisticTransactionImpl].upgradeProtocolVersion(1, 2)
        txn1.updateMetadata(Metadata.builder().schema(originalSchema).build())
        // This invariant does not apply when the table is empty or if the current commit is
        // removing all the files in the table
        val addFile = AddFile("path/to/file/test.parquet", Map(), 0, 0, true)
        txn1.commit(
          addFile :: Nil,
          new Operation(Operation.Name.MANUAL_UPDATE),
          "test-engine-info"
        )
        val txn2 = log.startTransaction()
        txn2.updateMetadata(txn2.metadata().copyBuilder().schema(updatedSchema).build())
        if (shouldFail) {
          testException[IllegalStateException](
            txn2.commit(Seq.empty, new Operation(Operation.Name.MANUAL_UPDATE), "test-engine-info"),
            "Detected incompatible schema change:"
          )
        } else {
          txn2.commit(Seq.empty, new Operation(Operation.Name.MANUAL_UPDATE), "test-engine-info")
        }
      }
    }
  }
}
