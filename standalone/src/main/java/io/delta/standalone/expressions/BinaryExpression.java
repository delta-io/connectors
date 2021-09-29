package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;

import java.util.Arrays;
import java.util.List;

/**
 * An expression with two inputs and one output. The output is by default evaluated to null
 * if any input is evaluated to null.
 */
public abstract class BinaryExpression implements Expression {
    public final Expression left;
    public final Expression right;

    public BinaryExpression(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public final Object eval(RowRecord record) {
        Object leftResult = left.eval(record);
        if (null == leftResult) return null;

        Object rightResult = right.eval(record);
        if (null == rightResult) return null;

        return nullSafeEval(leftResult, rightResult);
    }

    protected abstract Object nullSafeEval(Object leftResult, Object rightResult);

    @Override
    public List<Expression> children() {
        return Arrays.asList(left, right);
    }
}
