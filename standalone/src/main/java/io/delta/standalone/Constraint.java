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

package io.delta.standalone;

import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructField;

/**
 * Represents a constraint defined on a Delta table which writers must verify before writing.
 * Constraints can come in one of two ways:
 * - A CHECK constraint which is stored in {@link Metadata#getConfiguration()}. CHECK constraints
 *   are stored as the key-value pair ("delta.constraints.{constraintName}", "{expression}")
 * - A column invariant which is stored in {@link StructField#getMetadata()}. See
 *   <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-invariants">Column Invariants</a>
 *   for more information.
 */
public interface Constraint {

    /**
     * @return the name of this constraint
     */
    String getName();

    /**
     * @return the expression to enforce of this constraint as a SQL string
     */
    String getExpression();
}
