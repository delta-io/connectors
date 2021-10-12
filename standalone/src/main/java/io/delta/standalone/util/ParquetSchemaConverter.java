/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.OriginalType.*;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import org.apache.parquet.schema.Type.Repetition.*;

import io.delta.standalone.types.*;

import java.util.Arrays;
import java.util.stream.Collectors;


public class ParquetSchemaConverter {

    enum OutputTimestampType {
        INT96,
        TIMESTAMP_MICROS,
        TIMESTAMP_MILLIS
    }

    static OutputTimestampType outputTimestampType;
    static boolean writeLegacyParquetFormat;

    // todo: where do we want OutputTimestampType and writeLegacyParquetFormat? construction or method argument

    // todo: what constructors or methods should we have in terms of default values?
    //  (writeLegacyParquetFormat, OuptutTimestampType, hadoop Conf, combos etc)

    public static MessageType convertToParquet(StructType schema) {
        return convert(schema, false, OutputTimestampType.INT96);
    }

    private static MessageType convert(StructType schema,
                                       boolean writeLegacyParquetFormat,
                                       OutputTimestampType outputTimestampType) {
        return Types.buildMessage().addFields(
                Arrays.stream(schema.getFields())
                        .map(field -> convertField(field))
                        .toArray(Type[]::new))
                // todo: ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME
                .named("todo schema name");
    }

    private static Type convertField(StructField field) {
        return convertField(field,
                field.isNullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED);
    }

    private static Type convertField(StructField field, Type.Repetition repetition) {
        // todo: ParquetSchemaConverter.checkFieldName

        if (field.getDataType() instanceof BooleanType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition)
                    .named(field.getName());
        } else if (field.getDataType() instanceof ByteType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                    .as(OriginalType.INT_8)
                    .named(field.getName());
        } else if (field.getDataType() instanceof ShortType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                    .as(OriginalType.INT_16)
                    .named(field.getName());
        } else if (field.getDataType() instanceof IntegerType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                    .named(field.getName());
        } else if (field.getDataType() instanceof LongType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
                    .named(field.getName());
        } else if (field.getDataType() instanceof FloatType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition)
                    .named(field.getName());
        } else if (field.getDataType() instanceof DoubleType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition)
                    .named(field.getName());
        } else if (field.getDataType() instanceof StringType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                    .as(OriginalType.UTF8)
                    .named(field.getName());
        } else if (field.getDataType() instanceof DateType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                    .as(OriginalType.DATE)
                    .named(field.getName());
        }
        // NOTE: Spark SQL can write timestamp values to Parquet using INT96, TIMESTAMP_MICROS or
        // TIMESTAMP_MILLIS. TIMESTAMP_MICROS is recommended but INT96 is the default to keep the
        // behavior same as before.
        //
        // As stated in PARQUET-323, Parquet `INT96` was originally introduced to represent nanosecond
        // timestamp in Impala for some historical reasons.  It's not recommended to be used for any
        // other types and will probably be deprecated in some future version of parquet-format spec.
        // That's the reason why parquet-format spec only defines `TIMESTAMP_MILLIS` and
        // `TIMESTAMP_MICROS` which are both logical types annotating `INT64`.
        //
        // Originally, Spark SQL uses the same nanosecond timestamp type as Impala and Hive.  Starting
        // from Spark 1.5.0, we resort to a timestamp type with microsecond precision so that we can
        // store a timestamp into a `Long`.  This design decision is subject to change though, for
        // example, we may resort to nanosecond precision in the future.
        // todo: review this
        else if (field.getDataType() instanceof TimestampType) {
            switch (outputTimestampType) {
                case INT96:
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, repetition)
                            .named(field.getName());
                case TIMESTAMP_MICROS:
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
                            .as(OriginalType.TIMESTAMP_MICROS)
                            .named(field.getName());
                case TIMESTAMP_MILLIS:
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
                            .as(OriginalType.TIMESTAMP_MILLIS)
                            .named(field.getName());
            }
        } else if (field.getDataType() instanceof BinaryType) {
            return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                    .named(field.getName());
        } else if (field.getDataType() instanceof DecimalType) {
            DecimalType dataType = (DecimalType) field.getDataType();
            if (writeLegacyParquetFormat) {
                // ======================
                // Decimals (legacy mode)
                // ======================

                // Spark 1.4.x and prior versions only support decimals with a maximum precision of 18 and
                // always store decimals in fixed-length byte arrays.  To keep compatibility with these older
                // versions, here we convert decimals with all precisions to `FIXED_LEN_BYTE_ARRAY` annotated
                // by `DECIMAL`.
                return Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
                        .as(OriginalType.DECIMAL)
                        .precision(dataType.getPrecision())
                        .scale(dataType.getScale())
                        // todo: Decimal.minBytesForPrecision(precision)
                        .length(0)
                        .named(field.getName());
            } else {
                // ========================
                // Decimals (standard mode)
                // ========================
                if (dataType.getPrecision() <= 9) {
                    // Decimal.MAX_INT_DIGITS = 9
                    // Uses INT32 for 1 <= precision <= 9
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                            .as(OriginalType.DECIMAL)
                            .precision(dataType.getPrecision())
                            .scale(dataType.getScale())
                            .named(field.getName());
                } else if (dataType.getPrecision() <= 18) {
                    // Decimal.MAX_LONG_DIGITS = 18
                    // Uses INT64 for 1 <= precision <= 18
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
                            .as(OriginalType.DECIMAL)
                            .precision(dataType.getPrecision())
                            .scale(dataType.getScale())
                            .named(field.getName());
                } else {
                    // Uses FIXED_LEN_BYTE_ARRAY for all other precisions
                    return Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
                            .as(OriginalType.DECIMAL)
                            .precision(dataType.getPrecision())
                            .scale(dataType.getScale())
                            // todo: Decimal.minBytesForPrecision(precision)
                            .length(0)
                            .named(field.getName());
                }
            }
        } else if (field.getDataType() instanceof ArrayType) {
            ArrayType dataType = (ArrayType) field.getDataType();
            if (writeLegacyParquetFormat) {
                // Spark 1.4.x and prior versions convert `ArrayType` with nullable elements into a 3-level
                // `LIST` structure.  This behavior is somewhat a hybrid of parquet-hive and parquet-avro
                // (1.6.0rc3): the 3-level structure is similar to parquet-hive while the 3rd level element
                // field name "array" is borrowed from parquet-avro.
                if (dataType.containsNull()) {
                    // <list-repetition> group <name> (LIST) {
                    //   optional group bag {
                    //     repeated <element-type> array;
                    //   }
                    // }
                    // todo: review this comments and code below

                    // This should not use `listOfElements` here because this new method checks if the
                    // element name is `element` in the `GroupType` and throws an exception if not.
                    // As mentioned above, Spark prior to 1.4.x writes `ArrayType` as `LIST` but with
                    // `array` as its element name as below. Therefore, we build manually
                    // the correct group type here via the builder. (See SPARK-16777)
                    return Types
                            .buildGroup(repetition).as(OriginalType.LIST)
                            .addFields(Types
                                    .buildGroup(Type.Repetition.REPEATED)
                                    // "array" is the name chosen by parquet-hive (1.7.0 and prior version)
                                    .addField(convertField(
                                            new StructField("array", dataType.getElementType(), true)))
                                    .named("bag"))
                            .named(field.getName());
                } else {
                    // Spark 1.4.x and prior versions convert ArrayType with non-nullable elements into a 2-level
                    // LIST structure.  This behavior mimics parquet-avro (1.6.0rc3).  Note that this case is
                    // covered by the backwards-compatibility rules implemented in `isElementType()`.
                    // todo: review this comments and code below

                    // <list-repetition> group <name> (LIST) {
                    //   repeated <element-type> element;
                    // }

                    // Here too, we should not use `listOfElements`. (See SPARK-16777)
                    return Types
                            .buildGroup(repetition).as(OriginalType.LIST)
                            // "array" is the name chosen by parquet-avro (1.7.0 and prior version)
                            .addField(convertField(
                                    new StructField("array", dataType.getElementType(), false),
                                    Type.Repetition.REPEATED))
                            .named(field.getName());
                }
            } else {
                // <list-repetition> group <name> (LIST) {
                //   repeated group list {
                //     <element-repetition> <element-type> element;
                //   }
                // }
               return Types
                        .buildGroup(repetition).as(OriginalType.LIST)
                       .addField(
                               Types.repeatedGroup()
                               .addField(convertField(
                                       new StructField("element", dataType.getElementType(), dataType.containsNull())))
                                       .named("list"))
                               .named(field.getName());
            }
        } else if (field.getDataType() instanceof MapType) {
            MapType dataType = (MapType) field.getDataType();
            if (writeLegacyParquetFormat) {
                // Spark 1.4.x and prior versions convert MapType into a 3-level group annotated by
                // MAP_KEY_VALUE.  This is covered by `convertGroupField(field: GroupType): DataType`.
                // <map-repetition> group <name> (MAP) {
                //   repeated group map (MAP_KEY_VALUE) {
                //     required <key-type> key;
                //     <value-repetition> <value-type> value;
                //   }
                // }
                return ConversionPatterns.mapType(
                        repetition,
                        field.getName(),
                        convertField(new StructField("key", dataType.getKeyType(), false)),
                        convertField(new StructField("value", dataType.getValueType(), dataType.valueContainsNull())));
            } else {
                // <map-repetition> group <name> (MAP) {
                //   repeated group key_value {
                //     required <key-type> key;
                //     <value-repetition> <value-type> value;
                //   }
                // }
                return Types
                        .buildGroup(repetition).as(OriginalType.MAP)
                        .addField(Types
                                .repeatedGroup()
                                .addField(convertField(new StructField("key", dataType.getKeyType(), false)))
                                .addField(convertField(new StructField("value", dataType.getValueType(), dataType.valueContainsNull())))
                                .named("key_value"))
                        .named(field.getName());
            }
        } else if (field.getDataType() instanceof StructType) {
            // todo
//            fields.foldLeft(Types.buildGroup(repetition)) { (builder, field) =>
//                builder.addField(convertField(field))
//            }.named(field.name)
        } else {
            // todo: analysis exception?
            throw new IllegalArgumentException("Unsupported data type " +
                    field.getDataType().getCatalogString());
        }
    }
}
