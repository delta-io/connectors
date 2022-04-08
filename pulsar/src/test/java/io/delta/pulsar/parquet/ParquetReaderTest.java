/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.delta.pulsar.parquet;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.Type;
import org.testng.annotations.Test;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;




/**
 * The ParquetReaderUtilsTest test ParquetReaderUtils.
 */
@Slf4j
public class ParquetReaderTest {
    final StructType deltaSchema = new StructType()
        .add("year", new IntegerType())
        .add("month", new IntegerType())
        .add("day", new IntegerType())
        .add("sale_id", new StringType())
        .add("customer", new StringType())
        .add("total_cost", new FloatType());

    @Test
    public void testFetchAadParseParquet() throws IOException {
        Configuration configuration = new Configuration();
        String path = "src/test/java/resources/external/sales/"
            + "part-00000-29bf7ab2-30a1-473b-8e0c-2970c40059dc-c000.snappy.parquet";

        DeltaParquetReader.Parquet parquet =
            DeltaParquetReader.getTotalParquetData(path, configuration);
        try {
            Set<String> fieldsInParquetSchema = new HashSet<>();
            for (Type type : parquet.getMessageType().getFields()) {
                assertNotNull(deltaSchema.get(type.getName()));
                fieldsInParquetSchema.add(type.getName());
            }

            for (StructField field : deltaSchema.getFields()) {
                assertTrue(fieldsInParquetSchema.contains(field.getName()));
            }
        } catch (IllegalArgumentException e) {
            fail();
        }

        for (int index = 0; index < 10; index++) {
            SimpleGroup group = parquet.getData().get(index);
            assertEquals(2000, group.getLong(0, 0));
            assertEquals(1, group.getLong(1, 0));
            assertEquals(index + 1, group.getLong(2, 0));
        }
    }

    @Test
    public void testParquetGetRowNum() throws IOException {
        Configuration configuration = new Configuration();
        String path = "src/test/java/resources/external/sales/"
            + "part-00000-29bf7ab2-30a1-473b-8e0c-2970c40059dc-c000.snappy.parquet";
        long totalRowNumber = 28 * 12 * 21;
        assertEquals(totalRowNumber, DeltaParquetReader.getRowNum(path, configuration));
    }

    @Test
    public void testReadBatch() throws IOException {
        DeltaParquetReader parquetReader = new DeltaParquetReader();
        Configuration configuration = new Configuration();
        String path = "src/test/java/resources/external/sales/"
            + "part-00000-29bf7ab2-30a1-473b-8e0c-2970c40059dc-c000.snappy.parquet";
        int rowCnt = 5;

        parquetReader.open(path, configuration);

        DeltaParquetReader.Parquet parquet = parquetReader.readBatch(rowCnt);
        try {
            for (Type type : parquet.getMessageType().getFields()) {
                assertNotNull(deltaSchema.get(type.getName()));
            }
        } catch (IllegalArgumentException e) {
            fail();
        }

        assertEquals(rowCnt, parquet.getData().size());

        for (int index = 0; index < rowCnt; index++) {
            SimpleGroup group = parquet.getData().get(index);
            assertEquals(2000, group.getLong(0, 0));
            assertEquals(1, group.getLong(1, 0));
            assertEquals(index + 1, group.getLong(2, 0));
        }
    }
}
