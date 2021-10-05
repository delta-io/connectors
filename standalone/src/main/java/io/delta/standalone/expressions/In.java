package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.internal.expressions.Util;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Usage: new In(expr, exprList) - Returns true if `expr` is equal to any in `exprList`, else false.
 */
public final class In implements Predicate {
    private final Expression value;
    private final List<? extends Expression> elems;
    private final CastingComparator<?> comparator;

    public In(Expression value, List<? extends Expression> elems) {
        if (null == value) {
            throw new IllegalArgumentException("'In' expression 'value' cannot be null");
        }
        if (null == elems) {
            throw new IllegalArgumentException("'In' expression 'elems' cannot be null");
        }
        if (elems.isEmpty()) {
            throw new IllegalArgumentException("'In' expression 'elems' cannot be empty");
        }

        boolean allSameDataType = elems.stream().allMatch(x -> x.dataType().equals(value.dataType()));

        if (!allSameDataType) {
            throw new IllegalArgumentException("In expression 'elems' and 'value' must all be of the same DataType");
        }

        this.value = value;
        this.elems = elems;
        this.comparator = Util.createCastingComparator(value.dataType());
        // TODO: this only allows certain dataTypes, do we want it to be broader than this?
    }

    @Override
    public Boolean eval(RowRecord record) {
        Object result = value.eval(record);
        if (null == result) {
            throw new IllegalArgumentException("'In' expression 'value.eval' result can't be null");
            // TODO: is this what we want? what if we want to check for if null is in it?
        }

        return elems.stream().anyMatch(setElem -> {
            Object setElemValue = setElem.eval(record);

            if (null == setElemValue) {
                throw new IllegalArgumentException("'In' expression 'elems(i).eval' result can't be null");
                // TODO: same thing here why exactly can't null be in the list?
            }

            return comparator.compare(result, setElemValue) == 0;
        });
    }

    @Override
    public String toString() {
        String elemsStr = elems.stream().map(Expression::toString).collect(Collectors.joining(", "));
        return value + " IN (" + elemsStr + ")";
    }

    @Override
    public List<Expression> children() {
        return Stream.concat(Stream.of(value), elems.stream()).collect(Collectors.toList());
    }
}
