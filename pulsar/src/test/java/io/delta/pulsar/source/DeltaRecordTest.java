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
package io.delta.pulsar.source;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static io.delta.pulsar.source.DeltaRecord.OP_ADD_RECORD;
import static io.delta.pulsar.source.DeltaRecord.OP_FIELD;
import static io.delta.pulsar.source.DeltaRecord.PARTITION_VALUE_FIELD;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

/**
 * DeltaRecord test.
 */
public class DeltaRecordTest {
    String path = "src/test/java/resources/external/sales";
    DeltaReader deltaReader;
    SourceContextForTest sourceContext;
    ExecutorService executorService;
    final StructType deltaSchema = new StructType()
        .add("year", new LongType())
        .add("month", new LongType())
        .add("day", new LongType())
        .add("sale_id", new StringType())
        .add("customer", new StringType())
        .add("total_cost", new DoubleType());

    @BeforeClass
    public void setup() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("startSnapshotVersion", 1);
        map.put("fetchHistoryData", true);
        map.put("tablePath", path);
        map.put("fileSystemType", "filesystem");
        map.put("parquetParseParallelism", 3);
        map.put("maxReadBytesSizeOneRound", 1024 * 1024);
        map.put("maxReadRowCountOneRound", 1000);
        map.put("checkpointInterval", 30);

        DeltaSourceConfig config = DeltaSourceConfig.load(map);
        config.validate();
        deltaReader = new DeltaReader(config, 5);
        deltaReader.setFilter(readCursor -> true);
        executorService =
            Executors.newSingleThreadExecutor(new DefaultThreadFactory("test"));

        sourceContext = new SourceContextForTest();
    }

    @Test
    public void testConvertToPulsarSchema() {
        try {
            GenericSchema<GenericRecord> pulsarSchema =
                DeltaRecord.convertToPulsarSchema(deltaSchema);
            Set<String> fieldsInPulsarSchema = new HashSet<>();
            for (Field field : pulsarSchema.getFields()) {
                assertNotNull(deltaSchema.get(field.getName()));
                fieldsInPulsarSchema.add(field.getName());
            }

            for (StructField field : deltaSchema.getFields()) {
                assertTrue(fieldsInPulsarSchema.contains(field.getName()));
            }
        } catch (IOException e) {
            fail();
        }
    }

    @Test
    public void testGetGenericRecord() {
        List<DeltaReader.ReadCursor> readCursorList = deltaReader.getDeltaActionFromSnapShotVersion(
            0, 1024 * 1024, false, sourceContext);
        List<DeltaReader.RowRecordData> queue;
        AtomicInteger readStatus = new AtomicInteger(0);
        DeltaReader.ReadCursor readCursor = readCursorList.get(1);
        try {
            queue = deltaReader.readParquetFileAsync(readCursor, executorService, readStatus).get();
            long totalRowNumber = 28 * 12 * 21;
            assertEquals(1, readStatus.get());
            assertEquals(totalRowNumber, queue.size());

            DeltaReader.RowRecordData rowRecordData;

            int cnt = 0;
            GenericSchema<GenericRecord> pulsarSchema =
                DeltaRecord.convertToPulsarSchema(deltaSchema);
            // check the first 10 items in the queue
            while (!queue.isEmpty() && cnt < 10) {
                rowRecordData = queue.get(cnt);
                GenericRecord record =
                    DeltaRecord.getGenericRecord(deltaSchema, pulsarSchema, rowRecordData);
                assertEquals(2000, (long) record.getField("year"));
                assertEquals(1, (long) record.getField("month"));
                assertEquals(cnt + 1, (long) record.getField("day"));

                record = DeltaRecord.getGenericRecord(null,
                    pulsarSchema, rowRecordData);
                assertEquals(2000, (long) record.getField("year"));
                assertEquals(1, (long) record.getField("month"));
                assertEquals(cnt + 1, (long) record.getField("day"));

                cnt++;
            }
        } catch (ExecutionException | InterruptedException | IOException e) {
            fail();
        }
    }

    @Test
    public void testDeltaRecordGeneration() {
        List<DeltaReader.ReadCursor> readCursorList = deltaReader.getDeltaActionFromSnapShotVersion(
            0, 1024 * 1024, false, sourceContext);
        List<DeltaReader.RowRecordData> queue;
        AtomicInteger readStatus = new AtomicInteger(0);
        DeltaReader.ReadCursor readCursor = readCursorList.get(1);

        try {
            queue = deltaReader.readParquetFileAsync(readCursor, executorService, readStatus).get();
            long totalRowNumber = 28 * 12 * 21;
            assertEquals(1, readStatus.get());
            assertEquals(totalRowNumber, queue.size());
            DeltaReader.RowRecordData rowRecordData;

            int cnt = 0;
            String topic = "delta_test_v1";
            DeltaReader.topicPartitionNum = 10;
            GenericSchema<GenericRecord> pulsarSchema =
                DeltaRecord.convertToPulsarSchema(deltaSchema);
            AtomicInteger processingException = new AtomicInteger(0);
            while (!queue.isEmpty() && cnt < 10) {
                rowRecordData = queue.get(cnt);
                DeltaRecord deltaRecord = new DeltaRecord(rowRecordData, topic,
                    deltaSchema, null, processingException);
                assertEquals(deltaSchema, DeltaRecord.deltaSchema);
                assertEquals(pulsarSchema.getSchemaInfo().getSchemaDefinition(),
                    DeltaRecord.pulsarSchema.getSchemaInfo().getSchemaDefinition());
                assertEquals(topic, deltaRecord.getTopic());

                // validate record
                GenericRecord record = deltaRecord.getValue();
                assertEquals(2000, (long) record.getField("year"));
                assertEquals(1, (long) record.getField("month"));
                assertEquals(cnt + 1, (long) record.getField("day"));

                assertEquals(0, deltaRecord.getPartition());
                assertEquals(cnt, (long) deltaRecord.getRecordSequence().get());

                // check properties
                assertEquals(OP_ADD_RECORD, deltaRecord.getProperties().get(OP_FIELD));
                assertTrue(StringUtils.isBlank(
                    deltaRecord.getProperties().get(PARTITION_VALUE_FIELD)));

                cnt++;
            }
        } catch (ExecutionException | InterruptedException | IOException e) {
            fail();
        }
    }
}
