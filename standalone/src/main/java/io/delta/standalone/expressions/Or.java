package io.delta.standalone.expressions;

import io.delta.standalone.internal.exception.DeltaErrors;

/**
 * Evaluates logical {@code expr1} OR {@code expr2} for {@code new Or(expr1, expr2)}.
 * <p>
 * Requires both left and right input expressions evaluate to booleans upon evaluation.
 */
public final class Or extends BinaryOperator implements Predicate {

    public Or(Expression left, Expression right) {
        super(left, right, "||");
    }

    @Override
    protected Object nullSafeEval(Object leftResult, Object rightResult) {
        if (!(leftResult instanceof Boolean) || !(rightResult instanceof Boolean)) {
            throw DeltaErrors.illegalExpressionValueType(
                    "OR",
                    "Boolean",
                    leftResult.getClass().getName(),
                    rightResult.getClass().getName());
        }

        return (boolean) leftResult || (boolean) rightResult;
    }
}
