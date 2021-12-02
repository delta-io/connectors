package io.delta.standalone.expressions;

import io.delta.standalone.internal.exception.DeltaErrors;

/**
 * Evaluates logical {@code expr1} AND {@code expr2} for {@code new And(expr1, expr2)}.
 * <p>
 * Requires both left and right input expressions evaluate to booleans upon evaluation.
 */
public final class And extends BinaryOperator implements Predicate {

    public And(Expression left, Expression right) {
        super(left, right, "&&");
    }

    @Override
    protected Object nullSafeEval(Object leftResult, Object rightResult) {
        if (!(leftResult instanceof Boolean) || !(rightResult instanceof Boolean)) {
            throw DeltaErrors.illegalExpressionValueType(
                    "AND",
                    "Boolean",
                    leftResult.getClass().getName(),
                    rightResult.getClass().getName());
        }

        return (boolean) leftResult && (boolean) rightResult;
    }
}
