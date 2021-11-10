/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.delta.sink;

import java.util.Arrays;

import org.apache.flink.table.types.logical.*;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

public class TestSchemaConverter {

    @Test
    public void testConvertFlinkSchemaToIcebergSchema() {
        // GIVEN
        RowType flinkRowType = new RowType(
            Arrays.asList(
                new RowType.RowField("f1", new FloatType()),
                new RowType.RowField("f2", new IntType()),
                new RowType.RowField("f3", new VarCharType()),
                new RowType.RowField("f4", new DoubleType()),
                new RowType.RowField("f5", new MapType(new VarCharType(), new IntType())),
                new RowType.RowField("f6", new ArrayType(new TinyIntType())),
                new RowType.RowField("f7", new ArrayType(new VarCharType())),
                new RowType.RowField("f8", new VarCharType()),
                new RowType.RowField("f9", new BooleanType()),
                new RowType.RowField("f10", new TinyIntType()),
                new RowType.RowField("f11", new SmallIntType()),
                new RowType.RowField("f12", new BigIntType()),
                new RowType.RowField("f13", new BinaryType()),
                new RowType.RowField("f14", new VarBinaryType()),
                new RowType.RowField("f15", new TimestampType()),
                new RowType.RowField("f16", new DateType()),
                new RowType.RowField("f17", new CharType()),
                new RowType.RowField("f18", new DecimalType()),
                new RowType.RowField("f19", new DecimalType(2)),
                new RowType.RowField("f21", new DecimalType(2, 2)),
                new RowType.RowField("f22", new DecimalType(38, 2)),
                new RowType.RowField("f23", new DecimalType(10, 1)),
                new RowType.RowField("nested_field", new RowType(Arrays.asList(
                    new RowType.RowField("f01", new VarCharType()),
                    new RowType.RowField("f02", new IntType())
                )))
            ));

        // WHEN
        StructType deltaStructType = new SchemaConverter().toDeltaFormat(flinkRowType);

        // THEN
        StructType expectedDeltaStructType = new StructType(
            new StructField[]{
                new StructField("f1", new io.delta.standalone.types.FloatType()),
                new StructField("f2", new io.delta.standalone.types.IntegerType()),
                new StructField("f3", new io.delta.standalone.types.StringType()),
                new StructField("f4", new io.delta.standalone.types.DoubleType()),
                new StructField("f5", new io.delta.standalone.types.MapType(
                    new io.delta.standalone.types.StringType(),
                    new io.delta.standalone.types.IntegerType(),
                    true
                )),
                new StructField("f6", new io.delta.standalone.types.ArrayType(
                    new io.delta.standalone.types.ByteType(), true)),
                new StructField("f7", new io.delta.standalone.types.ArrayType(
                    new io.delta.standalone.types.StringType(), true)),
                new StructField("f8", new io.delta.standalone.types.StringType()),
                new StructField("f9", new io.delta.standalone.types.BooleanType()),
                new StructField("f10", new io.delta.standalone.types.ByteType()),
                new StructField("f11", new io.delta.standalone.types.ShortType()),
                new StructField("f12", new io.delta.standalone.types.LongType()),
                new StructField("f13", new io.delta.standalone.types.BinaryType()),
                new StructField("f14", new io.delta.standalone.types.BinaryType()),
                new StructField("f15", new io.delta.standalone.types.TimestampType()),
                new StructField("f16", new io.delta.standalone.types.DateType()),
                new StructField("f17", new io.delta.standalone.types.StringType()),
                new StructField("f18", new io.delta.standalone.types.DecimalType(10, 0)),
                new StructField("f19", new io.delta.standalone.types.DecimalType(2, 0)),
                new StructField("f21", new io.delta.standalone.types.DecimalType(2, 2)),
                new StructField("f22", new io.delta.standalone.types.DecimalType(38, 2)),
                new StructField("f23", new io.delta.standalone.types.DecimalType(10, 1)),
                new StructField("nested_field", new StructType(new StructField[]{
                    new StructField("f01", new io.delta.standalone.types.StringType()),
                    new StructField("f02", new io.delta.standalone.types.IntegerType()),
                }))

            });

        assertEquals(expectedDeltaStructType, deltaStructType);
    }

    @Test
    public void testMapType() {
        // TODO add extensive tests for MapType
    }

    @Test
    public void testStructType() {
        // TODO add extensive tests for StructType
    }
}
