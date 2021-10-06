package io.delta.standalone.expressions;

import java.util.Comparator;

public class CastingComparator<T extends Comparable<T>> implements Comparator<Object> {
    private final Comparator<T> comparator;

    public CastingComparator() {
        comparator = Comparator.naturalOrder();
    }

    @Override
    public int compare(Object a, Object b) {
        return comparator.compare((T) a, (T) b);
    }
}