package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.internal.exception.DeltaErrors;

/**
 * Evaluates logical {@code expr1} AndPartialResult {@code expr2} for
 * {@code new AndPartialResult(expr1, expr2)}.
 * <p>
 * Requires both left and right input expressions evaluate to booleans.
 */
public final class AndPartialResult extends BinaryOperator implements Predicate {

    public AndPartialResult(Expression left, Expression right) {
        super(left, right, "&&&");
        if (!(left.dataType() instanceof BooleanType) ||
                !(right.dataType() instanceof BooleanType)) {
            throw DeltaErrors.illegalExpressionValueType(
                    "ANDPartialResult",
                    "bool",
                    left.dataType().getTypeName(),
                    right.dataType().getTypeName());
        }
    }

    @Override
    public Object eval(RowRecord record) {
        Object leftResult = left.eval(record);
        Object rightResult = right.eval(record);

        if (null == rightResult && null == leftResult) {
            return null;
        } else if (null == rightResult) {
            return leftResult;
        } else if (null == leftResult) {
            return rightResult;
        } else {
            return nullSafeEval(leftResult, rightResult);
        }
    }

    @Override
    public Object nullSafeEval(Object leftResult, Object rightResult) {
        return (boolean) leftResult && (boolean) rightResult;
    }
}
