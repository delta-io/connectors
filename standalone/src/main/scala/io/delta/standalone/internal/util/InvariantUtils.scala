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

package io.delta.standalone.internal.util

import scala.collection.JavaConverters._

import io.delta.standalone.Constraint
import io.delta.standalone.types.StructType

import io.delta.standalone.internal.ConstraintImpl
import io.delta.standalone.internal.exception.DeltaErrors

/**
 * Invariant utils for retrieving column invariants from the schema.
 */
private[internal] object InvariantUtils {
  sealed trait Rule {
    val name: String
  }

  sealed trait RulePersistedInMetadata {
    def wrap: PersistedRule
    def json: String = JsonUtils.toJson(wrap)
  }

  /** Rules that are persisted in the metadata field of a schema. */
  case class PersistedRule(expression: PersistedExpression = null) {
    def unwrap: RulePersistedInMetadata = {
      if (expression != null) {
        expression
      } else {
        null
      }
    }
  }

  /** Persisted companion of the ArbitraryExpression rule. */
  case class PersistedExpression(expression: String) extends RulePersistedInMetadata {
    override def wrap: PersistedRule = PersistedRule(expression = this)
  }

  /** Extract column invariants from the given schema */
  def getFromSchema(schema: StructType): Seq[Constraint] = {
    val columns = SchemaUtils.filterRecursively(schema, checkComplexTypes = false) { field =>
      field.getMetadata.contains(INVARIANTS_FIELD)
    }
    columns.map {
      case (_, field) =>
        val rule = field.getMetadata.get(INVARIANTS_FIELD).asInstanceOf[String]
        try {
          val invariant = Option(JsonUtils.mapper.readValue[PersistedRule](rule).unwrap) match {
            case Some(PersistedExpression(exprString)) =>
              exprString
            case _ =>
              throw DeltaErrors.misformattedInvariant(rule)
          }
          ConstraintImpl(s"EXPRESSION($invariant)", invariant)
        } catch {
          case e: com.fasterxml.jackson.databind.JsonMappingException =>
            throw DeltaErrors.misformattedInvariant(rule)
        }
    }
  }

  val INVARIANTS_FIELD = "delta.invariants"
}
