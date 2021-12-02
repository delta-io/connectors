package io.delta.standalone.expressions;

import io.delta.standalone.internal.exception.DeltaErrors;

/**
 * Evaluates logical NOT {@code expr} for {@code new Not(expr)}.
 * <p>
 * Requires the child expression evaluates to a boolean upon evaluation.
 */
public class Not extends UnaryExpression implements Predicate {
    public Not(Expression child) {
        super(child);
    }

    @Override
    protected Object nullSafeEval(Object childResult) {
        if (!(childResult instanceof Boolean)) {
            throw DeltaErrors.illegalExpressionValueType(
                    "NOT",
                    "Boolean",
                    childResult.getClass().getName());
        }

        return !((boolean) childResult);
    }

    @Override
    public String toString() {
        return "(NOT " + child.toString() + ")";
    }
}
