package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.DataType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An expression in Delta Standalone.
 */
public interface Expression {

    /**
     * Returns the result of evaluating this expression on a given input RowRecord.
     */
    Object eval(RowRecord record);

    /**
     * Returns the [[DataType]] of the result of evaluating this expression.
     */
    DataType dataType();

    /**
     * Returns the String representation of this expression.
     */
    String toString();

    default Set<String> references() {
        Set<String> result = new HashSet<>();
        children().forEach(child -> result.addAll(child.references()));
        return result;
    }

    /**
     * Returns a List of the children of this node. Children should not change.
     */
    List<Expression> children();
}
