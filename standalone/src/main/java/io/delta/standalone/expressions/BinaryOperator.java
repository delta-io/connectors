package io.delta.standalone.expressions;

/**
 * A [[BinaryExpression]] that is an operator, with two properties:
 *
 * 1. The string representation is "x symbol y", rather than "funcName(x, y)".
 * 2. Two inputs are expected to be of the same type. If the two inputs have different types, a
 *    RuntimeException will be thrown
 */
public abstract class BinaryOperator extends BinaryExpression {
    protected final String symbol;

    public BinaryOperator(Expression left, Expression right, String symbol) {
        super(left, right);
        this.symbol = symbol;
    }

    @Override
    public void verifyInputDataTypes() {
        if (!left.dataType().equals(right.dataType())) {
            throw new RuntimeException("BinaryOperator left and right DataTypes must be the same");
        }
    }

    @Override
    public String toString() {
        return "(" + left.toString() + " " + symbol + " " + right.toString() + ")";
    }
}
