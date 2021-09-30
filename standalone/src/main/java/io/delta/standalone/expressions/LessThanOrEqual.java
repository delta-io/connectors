package io.delta.standalone.expressions;

/**
 * Usage: new LessThanOrEqual(expr1, expr2) - Returns true if `expr1` is less than or
 * equal to `expr2`, else false.
 */
public final class LessThanOrEqual extends BinaryComparison implements Predicate {
    public LessThanOrEqual(Expression left, Expression right) {
        super(left, right, "<=");
    }

    @Override
    public Object nullSafeEval(Object leftResult, Object rightResult) {
        return compare(leftResult, rightResult) <= 0;
    }
}
