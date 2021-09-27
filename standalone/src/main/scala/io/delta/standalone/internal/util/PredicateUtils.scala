/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import io.delta.standalone.expressions.{And, Expression, Literal}

object PredicateUtils {

  /**
   * Partition the given condition into two optional conjunctive predicates M, D such that
   * condition = M AND D, where we define:
   * - M: conjunction of predicates that can be evaluated using metadata only.
   * - D: conjunction of other predicates.
   */
  def splitMetadataAndDataPredicates(
      condition: Expression,
      partitionColumns: Seq[String]): (Option[Expression], Option[Expression]) = {
    val (metadataPredicates, dataPredicates) =
      splitConjunctivePredicates(condition).partition(isPredicateMetadataOnly(_, partitionColumns))

    val metadataConjunction = if (metadataPredicates.isEmpty) {
      None
    } else {
      Some(metadataPredicates.reduceLeftOption(new And(_, _)).getOrElse(Literal.True))
    }

    val dataConjunction = if (dataPredicates.isEmpty) {
      None
    } else {
      Some(dataPredicates.reduceLeftOption(new And(_, _)).getOrElse(Literal.True))
    }

    (metadataConjunction, dataConjunction)
  }

  /**
   * Check if condition can be evaluated using only metadata (i.e. partition columns)
   */
  def isPredicateMetadataOnly(condition: Expression, partitionColumns: Seq[String]): Boolean = {
    // TODO: name equality resolver ?
    condition.references().asScala.forall(partitionColumns.contains(_))
  }

  private def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case a: And => splitConjunctivePredicates(a.left) ++ splitConjunctivePredicates(a.right)
      case other => other :: Nil
    }
  }

}
