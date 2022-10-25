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

import java.util.Objects;

import javax.annotation.Nonnull;

import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructField;

// TODO: if we ever plan to support not-null constraints as a java class should we name this
//  "CheckConstraint" even though we will also be presenting invariants in this form?

/**
 * Represents a constraint defined on a Delta table which writers must verify before writing.
 * Constraints can come in one of two ways:
 * - A CHECK constraint which is stored in {@link Metadata#getConfiguration()}
 * - A column invariant which is stored in {@link StructField#getMetadata()}
 */
public final class Constraint {

    public static final String CHECK_CONSTRAINT_KEY_PREFIX = "delta.constraints.";
    @Nonnull private final String name;
    @Nonnull private final String expression;

    public Constraint(
            @Nonnull String name,
            @Nonnull String expression) {
        this.name = name;
        this.expression = expression;
    }

    /**
     * @return the name of this constraint
     */
    @Nonnull
    public String getName() {
        return name;
    }

    /**
     * @return the expression to enforce of this constraint as a SQL string
     */
    @Nonnull
    public String getExpression() {
        return expression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Constraint constraint = (Constraint) o;
        return Objects.equals(name, constraint.name) &&
                Objects.equals(expression, constraint.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, expression);
    }
}
