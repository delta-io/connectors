package io.delta.standalone.expressions;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.*;

/**
 * A column whose row-value will be computed based on the data in a [[RowRecord]].
 *
 * Usage: new Column(columnName, columnDataType).
 *
 * It is recommended that you instantiate using a table schema (StructType).
 * e.g. schema.column(columnName)
 */
public final class Column extends LeafExpression {
    private final String name;
    private final DataType dataType;
    private final RowRecordEvaluator evaluator;

    public Column(String name, DataType dataType) {
        this.name = name;
        this.dataType = dataType;

        if (dataType instanceof IntegerType) {
            evaluator = (record -> record.getInt(name));
        } else if (dataType instanceof LongType) {
            evaluator = (record -> record.getLong(name));
        } else if (dataType instanceof ByteType) {
            evaluator = (record -> record.getByte(name));
        } else if (dataType instanceof ShortType) {
            evaluator = (record -> record.getShort(name));
        } else if (dataType instanceof BooleanType) {
            evaluator = (record -> record.getBoolean(name));
        } else if (dataType instanceof FloatType) {
            evaluator = (record -> record.getFloat(name));
        } else if (dataType instanceof DoubleType) {
            evaluator = (record -> record.getDouble(name));
        } else if (dataType instanceof StringType) {
            evaluator = (record -> record.getString(name));
        } else if (dataType instanceof BinaryType) {
            evaluator = (record -> record.getBinary(name));
        } else if (dataType instanceof DecimalType) {
            evaluator = (record -> record.getBigDecimal(name));
        } else if (dataType instanceof TimestampType) {
            evaluator = (record -> record.getTimestamp(name));
        } else if (dataType instanceof DateType) {
            evaluator = (record -> record.getDate(name));
        } else if (dataType instanceof ArrayType) {
            evaluator = (record -> record.getList(name));
        } else if (dataType instanceof MapType) {
            evaluator = (record -> record.getMap(name));
        } else if (dataType instanceof StructType) {
            evaluator = (record -> record.getRecord(name));
        } else {
            throw new IllegalArgumentException("Couldn't find matching rowRecord DataType for column: " + name);
        }
    }

    public String name() {
        return name;
    }

    @Override
    public Object eval(RowRecord record) {
        return record.isNullAt(name) ? null : evaluator.nullSafeEval(record);
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public String toString() {
        return "Column(" + name + ")";
    }

    @FunctionalInterface
    private interface RowRecordEvaluator {
        Object nullSafeEval(RowRecord record);
    }
}
