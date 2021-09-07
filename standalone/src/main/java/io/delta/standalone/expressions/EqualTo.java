package io.delta.standalone.expressions;

/**
 * Usage: new EqualTo(expr1, expr2) - Returns true if `expr1` equals `expr2`, else false.
 */
public final class EqualTo extends BinaryComparison {
    public EqualTo(Expression left, Expression right) {
        super(left, right, "=");
    }

    @Override
    public Object nullSafeBoundEval(Object leftResult, Object rightResult) {
        return Util.compare(left.dataType(), leftResult, rightResult) == 0;
    }
}
