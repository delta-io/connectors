package io.delta.standalone.expressions;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.*;

public final class Literal extends LeafExpression {
    // TODO: why is one of these Boolean.TRUE and the other false?
    public static final Literal True = Literal.of(Boolean.TRUE, new BooleanType());
    public static final Literal False = Literal.of(false, new BooleanType());

    private final Object value;
    private final DataType dataType;

    public Literal(Object value, DataType dataType) {
        Literal.validateLiteralValue(value, dataType);

        this.value = value;
        this.dataType = dataType;
    }

    public Object value() {
        return value;
    }

    @Override
    public Object eval(RowRecord record) {
        return value;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    //TODO: thoroughly test this???
    private static void validateLiteralValue(Object value, DataType dataType) {
        if (value == null) return;
        if ((dataType instanceof BinaryType && value instanceof byte[]) ||
                (dataType instanceof BinaryType && value instanceof Byte[]) ||
                (dataType instanceof BooleanType && value instanceof Boolean) ||
                (dataType instanceof ByteType && value instanceof Byte) ||
                (dataType instanceof DateType && value instanceof Date) ||
                (dataType instanceof DecimalType && value instanceof BigDecimal) ||
                (dataType instanceof DoubleType && value instanceof Double) ||
                (dataType instanceof FloatType && value instanceof Float) ||
                (dataType instanceof IntegerType && value instanceof Integer) ||
                (dataType instanceof LongType && value instanceof Long) ||
                (dataType instanceof NullType && value == null) ||
                (dataType instanceof ShortType && value instanceof Short) ||
                (dataType instanceof StringType && value instanceof String) ||
                (dataType instanceof TimestampType && value instanceof Timestamp)) return;
        if (dataType instanceof ArrayType) {
            if (!(value instanceof List)) {
                throw new IllegalArgumentException(
                        "Invalid literal creation with DataType: ArrayType and Value: " +
                                value.toString());
            }
            try {
                DataType innerType = ((ArrayType) dataType).getElementType();
                ((List) value).forEach( (x) -> Literal.validateLiteralValue(x, innerType) );
                return;
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid literal creation with DataType: ArrayType and "
                        + "Value: " + value.toString() + "with incorrect inner-type: " +
                e.getMessage());
            }
        }
        if (dataType instanceof MapType) {
            if (!(value instanceof Map)) {
                throw new IllegalArgumentException(
                        "Invalid literal creation with DataType: MapType and Value: " +
                                value.toString());
            }
            try {
                DataType keyType = ((MapType) dataType).getKeyType();
                DataType valueType = ((MapType) dataType).getValueType();
                ((Map) value).keySet().forEach( (x) -> Literal.validateLiteralValue(x, keyType));
                ((Map) value).values().forEach( (x) -> Literal.validateLiteralValue(x, valueType));
                return;
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid literal creation with DataType: MapType and "
                        + "Value: " + value.toString() + "with incorrect inner-type: " +
                        e.getMessage());
            }
        }
        if (dataType instanceof StructType) {
            if (!(value instanceof RowRecord)) {
                throw new IllegalArgumentException(
                        "Invalid literal creation with DataType: StructType and Value: " +
                                value.toString());
            }
            if (!Objects.equals(((RowRecord) value).getSchema(), (StructType) dataType)) {
                throw new IllegalArgumentException(
                        "Invalid literal creation with DataType: StructType and Value: " +
                        value.toString() + " with mismatched schemas");
            }
            return;
        }
        throw new IllegalArgumentException(
                String.format("Invalid literal creation DataType: %s and Value: %s",
                        dataType.getSimpleString(),
                        value.toString()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Literal literal = (Literal) o;
        return Objects.equals(value, literal.value) &&
            Objects.equals(dataType, literal.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, dataType);
    }

    public static Literal of(Object value, DataType type) { return new Literal(value, type); }

    //todo: could also do of(Object value, IntegerType type) etc for each type?????? validate within
}
