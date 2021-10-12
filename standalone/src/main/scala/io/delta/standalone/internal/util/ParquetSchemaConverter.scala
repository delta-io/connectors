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

package org.apache.spark.sql.execution.datasources.parquet

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema._
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * This converter class is used to convert Spark SQL [[StructType]] to Parquet [[MessageType]].
 *
 * @param writeLegacyParquetFormat Whether to use legacy Parquet format compatible with Spark 1.4
 *        and prior versions when converting a Catalyst [[StructType]] to a Parquet [[MessageType]].
 *        When set to false, use standard format defined in parquet-format spec.  This argument only
 *        affects Parquet write path.
 * @param outputTimestampType which parquet timestamp type to use when writing.
 */
class SparkToParquetSchemaConverter(
  writeLegacyParquetFormat: Boolean = SQLConf.PARQUET_WRITE_LEGACY_FORMAT.defaultValue.get,
  outputTimestampType: SQLConf.ParquetOutputTimestampType.Value =
  SQLConf.ParquetOutputTimestampType.INT96) {

  def this(conf: SQLConf) = this(
    writeLegacyParquetFormat = conf.writeLegacyParquetFormat,
    outputTimestampType = conf.parquetOutputTimestampType)

  def this(conf: Configuration) = this(
    writeLegacyParquetFormat = conf.get(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key).toBoolean,
    outputTimestampType = SQLConf.ParquetOutputTimestampType.withName(
      conf.get(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key)))

  /**
   * Converts a Spark SQL [[StructType]] to a Parquet [[MessageType]].
   */
  def convert(catalystSchema: StructType): MessageType = {
    Types
      .buildMessage()
      .addFields(catalystSchema.map(convertField): _*)
      .named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)
  }

  /**
   * Converts a Spark SQL [[StructField]] to a Parquet [[Type]].
   */
  def convertField(field: StructField): Type = {
    convertField(field, if (field.nullable) OPTIONAL else REQUIRED)
  }

  private def convertField(field: StructField, repetition: Type.Repetition): Type = {
    ParquetSchemaConverter.checkFieldName(field.name)

    field.dataType match {
      // ===================
      // Simple atomic types
      // ===================

      case BooleanType =>
        Types.primitive(BOOLEAN, repetition).named(field.name)

      case ByteType =>
        Types.primitive(INT32, repetition).as(INT_8).named(field.name)

      case ShortType =>
        Types.primitive(INT32, repetition).as(INT_16).named(field.name)

      case IntegerType =>
        Types.primitive(INT32, repetition).named(field.name)

      case LongType =>
        Types.primitive(INT64, repetition).named(field.name)

      case FloatType =>
        Types.primitive(FLOAT, repetition).named(field.name)

      case DoubleType =>
        Types.primitive(DOUBLE, repetition).named(field.name)

      case StringType =>
        Types.primitive(BINARY, repetition).as(UTF8).named(field.name)

      case DateType =>
        Types.primitive(INT32, repetition).as(DATE).named(field.name)

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
      case TimestampType =>
        outputTimestampType match {
          case SQLConf.ParquetOutputTimestampType.INT96 =>
            Types.primitive(INT96, repetition).named(field.name)
          case SQLConf.ParquetOutputTimestampType.TIMESTAMP_MICROS =>
            Types.primitive(INT64, repetition).as(TIMESTAMP_MICROS).named(field.name)
          case SQLConf.ParquetOutputTimestampType.TIMESTAMP_MILLIS =>
            Types.primitive(INT64, repetition).as(TIMESTAMP_MILLIS).named(field.name)
        }

      case BinaryType =>
        Types.primitive(BINARY, repetition).named(field.name)

      // ======================
      // Decimals (legacy mode)
      // ======================

      // Spark 1.4.x and prior versions only support decimals with a maximum precision of 18 and
      // always store decimals in fixed-length byte arrays.  To keep compatibility with these older
      // versions, here we convert decimals with all precisions to `FIXED_LEN_BYTE_ARRAY` annotated
      // by `DECIMAL`.
      case DecimalType.Fixed(precision, scale) if writeLegacyParquetFormat =>
        Types
          .primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .as(DECIMAL)
          .precision(precision)
          .scale(scale)
          .length(Decimal.minBytesForPrecision(precision))
          .named(field.name)

      // ========================
      // Decimals (standard mode)
      // ========================

      // Uses INT32 for 1 <= precision <= 9
      case DecimalType.Fixed(precision, scale)
        if precision <= Decimal.MAX_INT_DIGITS && !writeLegacyParquetFormat =>
        Types
          .primitive(INT32, repetition)
          .as(DECIMAL)
          .precision(precision)
          .scale(scale)
          .named(field.name)

      // Uses INT64 for 1 <= precision <= 18
      case DecimalType.Fixed(precision, scale)
        if precision <= Decimal.MAX_LONG_DIGITS && !writeLegacyParquetFormat =>
        Types
          .primitive(INT64, repetition)
          .as(DECIMAL)
          .precision(precision)
          .scale(scale)
          .named(field.name)

      // Uses FIXED_LEN_BYTE_ARRAY for all other precisions
      case DecimalType.Fixed(precision, scale) if !writeLegacyParquetFormat =>
        Types
          .primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .as(DECIMAL)
          .precision(precision)
          .scale(scale)
          .length(Decimal.minBytesForPrecision(precision))
          .named(field.name)

      // ===================================
      // ArrayType and MapType (legacy mode)
      // ===================================

      // Spark 1.4.x and prior versions convert `ArrayType` with nullable elements into a 3-level
      // `LIST` structure.  This behavior is somewhat a hybrid of parquet-hive and parquet-avro
      // (1.6.0rc3): the 3-level structure is similar to parquet-hive while the 3rd level element
      // field name "array" is borrowed from parquet-avro.
      case ArrayType(elementType, nullable @ true) if writeLegacyParquetFormat =>
        // <list-repetition> group <name> (LIST) {
        //   optional group bag {
        //     repeated <element-type> array;
        //   }
        // }

        // This should not use `listOfElements` here because this new method checks if the
        // element name is `element` in the `GroupType` and throws an exception if not.
        // As mentioned above, Spark prior to 1.4.x writes `ArrayType` as `LIST` but with
        // `array` as its element name as below. Therefore, we build manually
        // the correct group type here via the builder. (See SPARK-16777)
        Types
          .buildGroup(repetition).as(LIST)
          .addField(Types
            .buildGroup(REPEATED)
            // "array" is the name chosen by parquet-hive (1.7.0 and prior version)
            .addField(convertField(StructField("array", elementType, nullable)))
            .named("bag"))
          .named(field.name)

      // Spark 1.4.x and prior versions convert ArrayType with non-nullable elements into a 2-level
      // LIST structure.  This behavior mimics parquet-avro (1.6.0rc3).  Note that this case is
      // covered by the backwards-compatibility rules implemented in `isElementType()`.
      case ArrayType(elementType, nullable @ false) if writeLegacyParquetFormat =>
        // <list-repetition> group <name> (LIST) {
        //   repeated <element-type> element;
        // }

        // Here too, we should not use `listOfElements`. (See SPARK-16777)
        Types
          .buildGroup(repetition).as(LIST)
          // "array" is the name chosen by parquet-avro (1.7.0 and prior version)
          .addField(convertField(StructField("array", elementType, nullable), REPEATED))
          .named(field.name)

      // Spark 1.4.x and prior versions convert MapType into a 3-level group annotated by
      // MAP_KEY_VALUE.  This is covered by `convertGroupField(field: GroupType): DataType`.
      case MapType(keyType, valueType, valueContainsNull) if writeLegacyParquetFormat =>
        // <map-repetition> group <name> (MAP) {
        //   repeated group map (MAP_KEY_VALUE) {
        //     required <key-type> key;
        //     <value-repetition> <value-type> value;
        //   }
        // }
        ConversionPatterns.mapType(
          repetition,
          field.name,
          convertField(StructField("key", keyType, nullable = false)),
          convertField(StructField("value", valueType, valueContainsNull)))

      // =====================================
      // ArrayType and MapType (standard mode)
      // =====================================

      case ArrayType(elementType, containsNull) if !writeLegacyParquetFormat =>
        // <list-repetition> group <name> (LIST) {
        //   repeated group list {
        //     <element-repetition> <element-type> element;
        //   }
        // }
        Types
          .buildGroup(repetition).as(LIST)
          .addField(
            Types.repeatedGroup()
              .addField(convertField(StructField("element", elementType, containsNull)))
              .named("list"))
          .named(field.name)

      case MapType(keyType, valueType, valueContainsNull) =>
        // <map-repetition> group <name> (MAP) {
        //   repeated group key_value {
        //     required <key-type> key;
        //     <value-repetition> <value-type> value;
        //   }
        // }
        Types
          .buildGroup(repetition).as(MAP)
          .addField(
            Types
              .repeatedGroup()
              .addField(convertField(StructField("key", keyType, nullable = false)))
              .addField(convertField(StructField("value", valueType, valueContainsNull)))
              .named("key_value"))
          .named(field.name)

      // ===========
      // Other types
      // ===========

      case StructType(fields) =>
        fields.foldLeft(Types.buildGroup(repetition)) { (builder, field) =>
          builder.addField(convertField(field))
        }.named(field.name)

      case udt: UserDefinedType[_] =>
        convertField(field.copy(dataType = udt.sqlType))

      case _ =>
        throw new AnalysisException(s"Unsupported data type ${field.dataType.catalogString}")
    }
  }
}

private[sql] object ParquetSchemaConverter {
  val SPARK_PARQUET_SCHEMA_NAME = "spark_schema"

  val EMPTY_MESSAGE: MessageType =
    Types.buildMessage().named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)

  def checkFieldName(name: String): Unit = {
    // ,;{}()\n\t= and space are special characters in Parquet schema
    checkConversionRequirement(
      !name.matches(".*[ ,;{}()\n\t=].*"),
      s"""Attribute name "$name" contains invalid character(s) among " ,;{}()\\n\\t=".
         |Please use alias to rename it.
       """.stripMargin.split("\n").mkString(" ").trim)
  }

  def checkFieldNames(names: Seq[String]): Unit = {
    names.foreach(checkFieldName)
  }

  def checkConversionRequirement(f: => Boolean, message: String): Unit = {
    if (!f) {
      throw new AnalysisException(message)
    }
  }
}
