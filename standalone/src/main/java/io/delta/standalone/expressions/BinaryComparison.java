package io.delta.standalone.expressions;

import io.delta.standalone.internal.expressions.Util;

import java.util.Comparator;

/**
 * A [[BinaryOperator]] that compares the left and right [[Expression]]s and returns a boolean value.
 */
public abstract class BinaryComparison extends BinaryOperator implements Predicate {
    private final Comparator<Object> comparator;

    public BinaryComparison(Expression left, Expression right, String symbol) {
        super(left, right, symbol);

        // super asserted that left and right DataTypes were the same

        comparator = Util.createComparator(left.dataType());
    }

    protected int compare(Object leftResult, Object rightResult) {
        return comparator.compare(leftResult, rightResult);
    }
}
