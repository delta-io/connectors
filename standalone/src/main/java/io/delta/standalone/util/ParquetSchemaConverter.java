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

import scala.Enumeration.Value;

import org.apache.parquet.schema.MessageType;

import io.delta.standalone.internal.util.SparkToParquetSchemaConverter;
import io.delta.standalone.types.StructType;

// todo: implementation alternative = public interface SparkToParquetSchemaConverter and then extend
//  it in scala
//  implications: have to expose convert differently with defaults (multiple method signatures?)

// todo: docs
public class ParquetSchemaConverter {

    public static MessageType sparkToParquet(StructType schema) {
        return new SparkToParquetSchemaConverter().convert(schema);
    }

    public static MessageType sparkToParquet(StructType schema, Boolean writeLegacyParquetFormat) {
        return new SparkToParquetSchemaConverter(writeLegacyParquetFormat).convert(schema);
    }

    public static MessageType sparkToParquet(StructType schema, Value outputTimestampType) {
        return new SparkToParquetSchemaConverter(outputTimestampType).convert(schema);
    }

    public static MessageType sparkToParquet(
            StructType schema,
            Boolean writeLegacyParquetFormat,
            Value outputTimestampType) {
        return new SparkToParquetSchemaConverter(writeLegacyParquetFormat, outputTimestampType)
                .convert(schema);
    }
}