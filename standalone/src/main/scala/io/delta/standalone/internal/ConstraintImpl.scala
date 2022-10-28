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

import java.util.Locale

import scala.collection.JavaConverters._

import io.delta.standalone.Constraint
import io.delta.standalone.actions.Metadata

import io.delta.standalone.internal.util.InvariantUtils

/**
 * Scala implementation of Java interface [[Constraint]].
 */
private[internal] case class ConstraintImpl(name: String, expression: String) extends Constraint {

  override def getName: String = name

  override def getExpression: String = expression

  override def toString: String = s"$name ($expression)"

}

private[standalone] object ConstraintImpl {

  /**
   * Extracts constraints from the table properties. These include both CHECK constraints stored in
   * the [[Metadata#getConfiguration( )]] and column invariants stored in the
   * [[StructField#getMetadata( )]].
   */
  def getConstraints(metadata: Metadata): java.util.List[Constraint] = {
    (InvariantUtils.getFromSchema(metadata.getSchema)
      ++ getCheckConstraints(metadata.getConfiguration.asScala.toMap)).asJava
  }

  ///////////////////////////////////////////////////////////////////////////
  // Check constraint methods
  ///////////////////////////////////////////////////////////////////////////

  /**
   * The key-prefix for the check constraint key "delta.constraints.{constraintName}" in
   * [[Metadata#getConfiguration( )]]
   */
  val CHECK_CONSTRAINT_KEY_PREFIX = "delta.constraints.";

  /**
   * The [[Metadata#getConfiguration( )]] key for a check constraint with `name`. Keys are always
   * stored in the metadata in lowercase.
   */
  def getCheckConstraintKey(name: String): String = {
    CHECK_CONSTRAINT_KEY_PREFIX + name.toLowerCase(Locale.ROOT)
  }

  private[internal] def getCheckConstraints(configuration: Map[String, String]): Seq[Constraint] = {
    val prefixRegex = CHECK_CONSTRAINT_KEY_PREFIX.replace(".", "\\.")
    configuration
      .filterKeys(_.startsWith(CHECK_CONSTRAINT_KEY_PREFIX))
      .map { case (key, value) =>
        ConstraintImpl(key.replaceFirst(prefixRegex, ""), value)
      }.toSeq
  }
}
