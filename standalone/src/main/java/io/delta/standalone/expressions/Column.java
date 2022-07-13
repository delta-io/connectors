package io.delta.standalone.expressions;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.*;
import io.delta.standalone.internal.exception.DeltaErrors;

/**
 * A column whose row-value will be computed based on the data in a {@link RowRecord}.
 * <p>
 * It is recommended that you instantiate using an existing table schema {@link StructType}
 * with {@link StructType#column(String)}. For example, {@code mySchema.column(columnName)}.
 * <p>
 * Only supports primitive data types, see
 * <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types">Delta Transaction Log Protocol: Primitive Types</a>.
 */
public final class Column extends LeafExpression {
    private final String[] name;
    private final DataType dataType;
    private final RowRecordEvaluator evaluator;

    public Column(String[] name, DataType dataType) {
        // TODO extend the RowRecord interface to support column identity represent in array of
        //  strings.
        if (name.length < 1) {
            throw DeltaErrors.invalidColumnName(Arrays.toString(name));
        }

        this.name = name;
        this.dataType = dataType;

        if (dataType instanceof IntegerType) {
            evaluator = (record -> record.getInt(name[0]));
        } else if (dataType instanceof LongType) {
            evaluator = (record -> record.getLong(name[0]));
        } else if (dataType instanceof ByteType) {
            evaluator = (record -> record.getByte(name[0]));
        } else if (dataType instanceof ShortType) {
            evaluator = (record -> record.getShort(name[0]));
        } else if (dataType instanceof BooleanType) {
            evaluator = (record -> record.getBoolean(name[0]));
        } else if (dataType instanceof FloatType) {
            evaluator = (record -> record.getFloat(name[0]));
        } else if (dataType instanceof DoubleType) {
            evaluator = (record -> record.getDouble(name[0]));
        } else if (dataType instanceof StringType) {
            evaluator = (record -> record.getString(name[0]));
        } else if (dataType instanceof BinaryType) {
            evaluator = (record -> record.getBinary(name[0]));
        } else if (dataType instanceof DecimalType) {
            evaluator = (record -> record.getBigDecimal(name[0]));
        } else if (dataType instanceof TimestampType) {
            evaluator = (record -> record.getTimestamp(name[0]));
        } else if (dataType instanceof DateType) {
            evaluator = (record -> record.getDate(name[0]));
        } else {
            throw new UnsupportedOperationException("The data type of column " +
                    Arrays.toString(name) + " is " + dataType.getTypeName() +
                    ". This is not supported yet.");
        }
    }

    public Column(String name, DataType dataType) {
        this(new String[]{name}, dataType);
    }

    public String[] name() {
        return name;
    }

    @Override
    public Object eval(RowRecord record) {
        return record.isNullAt(name[0]) ? null : evaluator.nullSafeEval(record);
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public String toString() {
        return "Column(" + Arrays.toString(name) + ")";
    }

    @Override
    public Set<String> references() {
        return Collections.singleton(Arrays.toString(name));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Column column = (Column) o;
        return Arrays.equals(name, column.name) &&
                Objects.equals(dataType, column.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(name), dataType);
    }

    @FunctionalInterface
    private interface RowRecordEvaluator {
        Object nullSafeEval(RowRecord record);
    }
}
