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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import io.delta.pulsar.common.Murmur32Hash;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.Type;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static io.delta.pulsar.DeltaLakeConnectorStats.PREPARE_READ_FILES_COUNT;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

/**
 * DeltaRead test.
 */
public class DeltaReaderTest {
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
    public void testGetDeltaSnapshot() throws Exception {
        Snapshot snapshot = deltaReader.getSnapShot(0);
        assertEquals(0, snapshot.getVersion());
        assertEquals(1, snapshot.getAllFiles().size());
        assertEquals(deltaSchema, snapshot.getMetadata().getSchema());

        // test invalid snapshot version
        DeltaReader.VersionResponse versionResponse = deltaReader.getAndValidateSnapShotVersion(-1);
        assertEquals(0, versionResponse.version.longValue());
        assertFalse(versionResponse.isOutOfRange);

        // test out of range snapshot version
        versionResponse = deltaReader.getAndValidateSnapShotVersion(1);
        assertEquals(0, versionResponse.version.longValue());
        assertFalse(versionResponse.isOutOfRange);

        // test get snapshot by timestamp
        long current = System.currentTimeMillis();
        versionResponse = deltaReader.getSnapShotVersionFromTimeStamp(current);
        assertEquals(0, versionResponse.version.longValue());
        assertFalse(versionResponse.isOutOfRange);
    }

    @Test
    public void testGetDeltaActionWithFullSnapshot() {
        List<DeltaReader.ReadCursor> readCursorList = deltaReader.getDeltaActionFromSnapShotVersion(
            0, 1024 * 1024, true, sourceContext);
        DeltaReader.ReadCursor readCursor = readCursorList.get(0);
        assertEquals(1, readCursorList.size());
        assertEquals(0, readCursor.getVersion());
        assertTrue(readCursor.isFullSnapShot());
        assertEquals(0, readCursor.getChangeIndex());
        assertEquals(-1, readCursor.getRowNum());
        assertFalse(readCursor.isEndOfFile());
        assertFalse(readCursor.isEndOfVersion());
        assertTrue(StringUtils.isBlank(readCursor.getPartitionValue()));
        assertTrue(readCursor.getAct() instanceof AddFile);
    }

    @Test
    public void testGetDeltaActionWithIncrementSnapshot() {
        List<DeltaReader.ReadCursor> readCursorList = deltaReader.getDeltaActionFromSnapShotVersion(
            0, 1024 * 1024, false, sourceContext);
        assertEquals(2, readCursorList.size());
        assertTrue(sourceContext.getMetrics().get(PREPARE_READ_FILES_COUNT).equals(2.0));

        DeltaReader.ReadCursor readCursor = readCursorList.get(0);
        assertEquals(0, readCursor.getVersion());
        assertFalse(readCursor.isFullSnapShot());
        assertEquals(2, readCursor.getChangeIndex());
        assertEquals(-1, readCursor.getRowNum());
        assertFalse(readCursor.isEndOfFile());
        assertFalse(readCursor.isEndOfVersion());
        assertTrue(StringUtils.isBlank(readCursor.getPartitionValue()));
        assertTrue(readCursor.getAct() instanceof Metadata);

        readCursor = readCursorList.get(1);
        assertEquals(0, readCursor.getVersion());
        assertFalse(readCursor.isFullSnapShot());
        assertEquals(3, readCursor.getChangeIndex());
        assertEquals(-1, readCursor.getRowNum());
        assertFalse(readCursor.isEndOfFile());
        assertFalse(readCursor.isEndOfVersion());
        assertTrue(StringUtils.isBlank(readCursor.getPartitionValue()));
        assertTrue(readCursor.getAct() instanceof AddFile);
    }

    @Test
    public void testGetMaxConcurrency() {
        List<DeltaReader.ReadCursor> readCursorList = deltaReader.getDeltaActionFromSnapShotVersion(
            0, 1024 * 1024, false, sourceContext);
        assertEquals(1, deltaReader.getMaxConcurrency(readCursorList, 0));
    }

    @Test
    public void testPartitionValueString() {
        Map<String, String> partition = new HashMap<>();
        partition.put("year", "2021");
        partition.put("month", "3");
        partition.put("day", "23");
        String partitionValue = DeltaReader.partitionValueToString(partition);
        String value = "day=23,month=3,year=2021";
        assertEquals(value, partitionValue);

        partitionValue = DeltaReader.partitionValueToString(new HashMap<>());
        assertTrue(StringUtils.isBlank(partitionValue));
    }

    @Test
    public void testGetPartitionIdByDeltaPartitionValue() {
        String key = "day=23,month=3,year=2021";

        int topicPartitionNum = 10;
        int partition = Murmur32Hash.getInstance()
            .makeHash(key.getBytes(StandardCharsets.UTF_8)) % topicPartitionNum;
        assertEquals(partition,
            DeltaReader.getPartitionIdByDeltaPartitionValue(key, topicPartitionNum));
        assertEquals(0,
            DeltaReader.getPartitionIdByDeltaPartitionValue(key, 0));

        key = "";
        assertEquals(0,
            DeltaReader.getPartitionIdByDeltaPartitionValue(key, topicPartitionNum));
        key = null;
        assertEquals(0,
            DeltaReader.getPartitionIdByDeltaPartitionValue(key, topicPartitionNum));
    }

    @Test
    public void testReadParquetFiles() {
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

            // check the first 10 items in the queue
            DeltaReader.RowRecordData rowRecordData;
            DeltaReader.ReadCursor readCursor1;
            SimpleGroup simpleGroup;
            List<Type> parquetSchema;

            int cnt = 0;
            while (!queue.isEmpty() && cnt < 10) {
                rowRecordData = queue.get(cnt);
                readCursor1 = rowRecordData.getNextCursor();
                simpleGroup = rowRecordData.getSimpleGroup();
                parquetSchema = rowRecordData.getParquetSchema();

                // check read cursor
                assertTrue(readCursor1.getAct() instanceof AddFile);
                assertEquals(0, readCursor1.getVersion());
                assertFalse(readCursor1.isFullSnapShot);
                assertEquals(3, readCursor1.getChangeIndex());
                assertEquals(cnt, readCursor1.getRowNum());
                assertFalse(readCursor1.isEndOfFile());
                assertFalse(readCursor1.isEndOfVersion());
                assertTrue(StringUtils.isBlank(readCursor1.getPartitionValue()));

                // check columns of this row
                assertEquals(2000, simpleGroup.getLong(0, 0));
                assertEquals(1, simpleGroup.getLong(1, 0));
                assertEquals(cnt + 1, simpleGroup.getLong(2, 0));

                // check parquet schema
                try {
                    Set<String> fieldsInParquetSchema = new HashSet<>();
                    for (Type type : parquetSchema) {
                        assertNotNull(deltaSchema.get(type.getName()));
                        fieldsInParquetSchema.add(type.getName());
                    }

                    for (StructField field : deltaSchema.getFields()) {
                        assertTrue(fieldsInParquetSchema.contains(field.getName()));
                    }
                } catch (IllegalArgumentException e) {
                    fail();
                }

                cnt++;
            }

        } catch (ExecutionException | InterruptedException e) {
            fail();
        }


    }
}
