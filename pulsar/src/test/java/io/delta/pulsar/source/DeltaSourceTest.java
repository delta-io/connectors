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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.delta.pulsar.DeltaSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.functions.api.Record;
import org.mockito.Mockito;
import static io.delta.pulsar.source.DeltaRecord.OP_ADD_RECORD;
import static io.delta.pulsar.source.DeltaRecord.OP_FIELD;
import static io.delta.pulsar.source.DeltaRecord.PARTITION_VALUE_FIELD;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import io.delta.standalone.DeltaScan;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.expressions.Expression;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructType;

/**
 * DeltaLake source connector test.
 *
 */
@Slf4j
public class DeltaSourceTest {
    final StructType deltaSchema = new StructType()
        .add("year", new LongType())
        .add("month", new LongType())
        .add("day", new LongType())
        .add("sale_id", new StringType())
        .add("customer", new StringType())
        .add("total_cost", new DoubleType());

    Snapshot snapshot = new Snapshot() {
        Metadata metadata = new Metadata.Builder().schema(deltaSchema).build();
        @Override
        public DeltaScan scan() {
            return null;
        }

        @Override
        public DeltaScan scan(Expression expression) {
            return null;
        }

        @Override
        public List<AddFile> getAllFiles() {
            return null;
        }

        @Override
        public Metadata getMetadata() {
            return metadata;
        }

        @Override
        public long getVersion() {
            return 0;
        }

        @Override
        public CloseableIterator<RowRecord> open() {
            return null;
        }
    };

    //@Test
    public void integrationTest() throws Exception {
        String path = "src/test/java/resources/external/sales";
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("fetchHistoryData", true);
        configMap.put("tablePath", path);
        configMap.put("fileSystemType", "filesystem");
        configMap.put("parquetParseParallelism", 3);
        configMap.put("maxReadBytesSizeOneRound", 1024 * 1024);
        configMap.put("maxReadRowCountOneRound", 1000);
        configMap.put("checkpointInterval", 0);

        String outputTopic = "persistent://public/default/delta_test_v1";
        GenericSchema<GenericRecord> pulsarSchema = DeltaRecord.convertToPulsarSchema(deltaSchema);
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        List<String> topics = new ArrayList<>();
        topics.add(outputTopic);
        future.complete(topics);

        SourceContextForTest sourceContextForTest = spy(new SourceContextForTest());
        sourceContextForTest.setTopic(outputTopic);
        sourceContextForTest.setInstanceId(0);
        sourceContextForTest.setNumInstances(1);

        Mockito.doReturn(mock(PulsarClientImpl.class)).when(sourceContextForTest).getPulsarClient();
        DeltaSource deltaLakeSourceConnector = new DeltaSource();
        PulsarClient pulsarClient = sourceContextForTest.getPulsarClient();
        Mockito.doReturn(future).when(pulsarClient).getPartitionsForTopic(any());
        deltaLakeSourceConnector.open(configMap, sourceContextForTest);

        try {
            for (int year = 2000; year < 2021; ++year) {
                for (int month = 1; month < 13; ++month) {
                    for (int day = 1; day < 29; ++day) {
                        Record<GenericRecord> record = deltaLakeSourceConnector.read();
                        GenericRecord genericRecord = record.getValue();
                        assertEquals(year, (long) genericRecord.getField("year"));
                        assertEquals(month, (long) genericRecord.getField("month"));
                        assertEquals(day, (long) genericRecord.getField("day"));
                        Map<String, String> properties = record.getProperties();
                        assertEquals(OP_ADD_RECORD, properties.get(OP_FIELD));
                        assertTrue(StringUtils.isBlank(properties.get(PARTITION_VALUE_FIELD)));
                        assertEquals(outputTopic, record.getDestinationTopic().get());
                        assertEquals(pulsarSchema.getSchemaInfo().getSchemaDefinition(),
                            record.getSchema().getSchemaInfo().getSchemaDefinition());
                    }
                }
            }
        } catch (Exception e) {
            fail();
        }
    }
}
