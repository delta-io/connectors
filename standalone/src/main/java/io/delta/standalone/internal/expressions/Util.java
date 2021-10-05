package io.delta.standalone.internal.expressions;

import java.math.BigDecimal;
import java.util.Date;

import io.delta.standalone.expressions.CastingComparator;
import io.delta.standalone.types.*;

public final class Util {

    public static CastingComparator<?> createCastingComparator(DataType dataType) {
        if (dataType instanceof IntegerType) {
            return new CastingComparator<Integer>();
        }

        if (dataType instanceof BooleanType) {
            return new CastingComparator<Boolean>();
        }

        if (dataType instanceof FloatType) {
            return new CastingComparator<Float>();
        }

        if (dataType instanceof LongType) {
            return new CastingComparator<Long>();
        }

        if (dataType instanceof ByteType) {
            return new CastingComparator<Byte>();
        }

        if (dataType instanceof ShortType) {
            return new CastingComparator<Short>();
        }

        if (dataType instanceof DoubleType) {
            return new CastingComparator<Double>();
        }

        if (dataType instanceof DecimalType) {
            return new CastingComparator<BigDecimal>();
        }

        if (dataType instanceof TimestampType) {
            return new CastingComparator<Date>();
        }

        if (dataType instanceof DateType) {
            return new CastingComparator<Date>();
        }

        // TODO: should we be able to compare binary values? what about strings?

        // unsupported comparison types: ArrayType, StructType, MapType
        throw new IllegalArgumentException("Couldn't find matching comparator for DataType: " + dataType.toString());
    }
}
