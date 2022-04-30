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

package io.delta.pulsar;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.pulsar.source.DeltaCheckpoint;
import io.delta.pulsar.source.DeltaReader;
import io.delta.pulsar.source.DeltaReaderThread;
import io.delta.pulsar.source.DeltaRecord;
import io.delta.pulsar.source.DeltaSourceConfig;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

import io.delta.standalone.types.StructType;


/**
 * A source connector that read data from delta lake through delta standalone reader.
 */
@Getter(AccessLevel.PACKAGE)
@Slf4j
public class DeltaSource implements Source<GenericRecord> {
    public static final Integer MIN_CHECKPOINT_KEY = -1;

    private DeltaSourceConfig config;
    private SourceContext sourceContext;
    private String outputTopic;
    private int topicPartitionNum;

    private ExecutorService fetchRecordExecutor;
    private ExecutorService parquetParseExecutor;
    private ScheduledExecutorService snapshotExecutor;

    private DeltaReader reader;
    // delta lake schema, when delta lake schema changes, this schema will change
    private StructType deltaSchema;
    private GenericSchema<GenericRecord> pulsarSchema;
    private DeltaCheckpoint minCheckpoint;
    private LinkedBlockingQueue<DeltaRecord> queue;

    // metrics
    private final AtomicInteger processingException = new AtomicInteger(0);
    private long recordCnt = 0;
    private static long checkpointId = 0;


    @Override
    public void open(Map<String, Object> configMap, SourceContext sourceContext) throws Exception {
        if (config != null) {
            log.error("The connector is already running, exit!");
            throw new IllegalStateException("The connector is already running, exit!");
        }

        this.sourceContext = sourceContext;
        outputTopic = sourceContext.getOutputTopic();
        topicPartitionNum =
            sourceContext.getPulsarClient().getPartitionsForTopic(outputTopic).get().size();

        // load the configuration and validate it
        config = DeltaSourceConfig.load(configMap);
        log.info("Delta Lake connector config: {}", config);
        config.validate();

        queue = new LinkedBlockingQueue<>(config.getQueueSize());
        reader = new DeltaReader(config, topicPartitionNum);

        // TODO check restore configuration, from config or from checkpoint
        Triple<Map<Integer, DeltaCheckpoint>, StructType, GenericSchema<GenericRecord>>
            checkpointTriple = restoreCheckpoint(sourceContext, config, topicPartitionNum, reader);

        Map<Integer, DeltaCheckpoint> checkpointMap = null;
        if (checkpointTriple != null) {
            checkpointMap = checkpointTriple.getLeft();
            deltaSchema = checkpointTriple.getMiddle();
            pulsarSchema = checkpointTriple.getRight();
        }

        if (checkpointMap == null || checkpointMap.isEmpty()) {
            log.info("instanceId: {} source connector do nothing, without any partition assigned",
                    sourceContext.getInstanceId());
            return;
        }
        minCheckpoint = checkpointMap.get(MIN_CHECKPOINT_KEY);

        // read from delta lake CDC
        reader.setFilter(initDeltaReadFilter(checkpointMap, topicPartitionNum));
        reader.setStartCheckpoint(minCheckpoint);

        // init executors
        snapshotExecutor =
            Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("snapshot-io"));
        parquetParseExecutor = Executors.newFixedThreadPool(config.getParquetParseThreads(),
            new DefaultThreadFactory("parquet-parse-io"));
        fetchRecordExecutor = Executors.newSingleThreadExecutor(
            new DefaultThreadFactory("fetch-record-io"));

        // start process records
        process();
    }

    private void process() {
        fetchRecordExecutor.execute(new DeltaReaderThread(reader, parquetParseExecutor,
            config, queue, deltaSchema, pulsarSchema, outputTopic,
            processingException, minCheckpoint, sourceContext));

        if (config.getCheckpointInterval() <= 0) {
            log.info("Due to checkpointInterval: {}, disable checkpoint.",
                config.getCheckpointInterval());
            return;
        }

        // TODO checkpoint support version
        snapshotExecutor.scheduleAtFixedRate(() -> {
            Map<Integer, DeltaCheckpoint> currentCheckpoint = DeltaRecord.currentSnapshot();
            Long startCheckpoint = System.currentTimeMillis();
            ObjectMapper mapper = ObjectMapperFactory.getThreadLocal();
            currentCheckpoint.forEach((key, value) -> {
                try {
                    Long start = System.currentTimeMillis();
                    sourceContext.putState(DeltaCheckpoint.getStatekey(key),
                        ByteBuffer.wrap(mapper.writeValueAsBytes(value)));
                    Long current = System.currentTimeMillis();
                    log.info("Do checkpoint complete @ {}, cost:{} ms for partition: {}, "
                            + "checkpoint: {}", current, current - start, key, value);
                } catch (Exception e) {
                    log.error("Failed to do checkpoint for key: {}, value: {} ", key, value, e);
                }
            });
            Long endCheckpoint = System.currentTimeMillis();

            log.info("Checkpoint {} complete @ {}, cost: {} ms",
                checkpointId++, endCheckpoint, endCheckpoint - startCheckpoint);
        }, config.getCheckpointInterval(), config.getCheckpointInterval(), TimeUnit.SECONDS);

    }

    @Override
    public Record<GenericRecord> read() throws Exception {
        if (this.processingException.get() > 0) {
            log.error("processing encounter exception will stop reading record "
                + "and connector will exit");
            throw new Exception("processing exception in processing delta record");
        }
        return  queue.take();
    }

    @Override
    public void close() {
        log.info("Closing source connector");

        if (fetchRecordExecutor != null) {
            fetchRecordExecutor.shutdown();
        }

        if (parquetParseExecutor != null) {
            parquetParseExecutor.shutdown();
        }

        if (snapshotExecutor != null) {
            snapshotExecutor.shutdown();
        }
    }

    private static GenericSchema<GenericRecord> handleGetDeltaSchemaFailed(
        SourceContext sourceContext) throws Exception {
        Class<?> classType = sourceContext.getClass();
        Field adminField = classType.getDeclaredField("pulsarAdmin");
        adminField.setAccessible(true);
        PulsarAdmin admin = (PulsarAdmin) adminField.get(sourceContext);
        GenericSchema<GenericRecord> schema =
            Schema.generic(admin.schemas().getSchemaInfo(sourceContext.getOutputTopic()));
        log.info("Get latest schema from pulsar, pulsarSchema: {}", schema);
        return schema;
    }

    /**
     * get checkpoint position from pulsar function stateStore.
     * @return if this instance not own any partition, will return empty,
     * else return the checkpoint map.
     */
    public static Triple<Map<Integer, DeltaCheckpoint>, StructType, GenericSchema<GenericRecord>>
        restoreCheckpoint(SourceContext sourceContext,
                      DeltaSourceConfig config,
                      int topicPartitionNum,
                      DeltaReader reader)
        throws Exception {
        int instanceId = sourceContext.getInstanceId();
        int numInstance = sourceContext.getNumInstances();
        DeltaCheckpoint minCheckpoint = null;
        Map<Integer, DeltaCheckpoint> checkpointMap = new HashMap<>();
        StructType deltaSchema = null;
        GenericSchema<GenericRecord> pulsarSchema = null;

        List<Integer> partitions = IntStream.range(0, topicPartitionNum)
            .filter(id -> id % numInstance == instanceId)
            .boxed()
            .collect(Collectors.toList());

        if (partitions.isEmpty()) {
            return null;
        }

        // for each partition, get checkpoint from pulsar source context state store.
        for (int partitionId : partitions) {
            ByteBuffer byteBuffer =
                sourceContext.getState(DeltaCheckpoint.getStatekey(partitionId));
            if (byteBuffer == null) {   // doesn't fond the partition checkpoint from state store
                continue;
            }

            String jsonString = StandardCharsets.UTF_8.decode(byteBuffer).toString();
            ObjectMapper mapper = ObjectMapperFactory.getThreadLocal();
            DeltaCheckpoint checkpoint;
            try {
                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                checkpoint = mapper.readValue(jsonString, DeltaCheckpoint.class);
            } catch (IOException e) {
                log.error("Parse the checkpoint for partition {} failed, jsonString: {} ",
                    partitionId, jsonString, e);
                throw new IOException("Parse checkpoint failed");
            }

            checkpointMap.put(partitionId, checkpoint);
            if (minCheckpoint == null || minCheckpoint.compareTo(checkpoint) > 0) {
                minCheckpoint = checkpoint;
            }
        }

        if (checkpointMap.isEmpty()) { // doesn't found any checkpoints from state store.
            DeltaReader.VersionResponse versionResponse = null;
            if (config.getStartSnapshotVersion() != null) {
                versionResponse =
                    reader.getAndValidateSnapShotVersion(config.getStartSnapshotVersion());
            } else if (config.getStartTimestamp() != null) {
                versionResponse =
                    reader.getSnapShotVersionFromTimeStamp(config.getStartTimestamp());
            }

            long startVersion = versionResponse != null
                ? versionResponse.getVersion() : DeltaSourceConfig.LATEST;
            DeltaCheckpoint.StateType copyMode = DeltaCheckpoint.StateType.INCREMENTAL_COPY;
            if (versionResponse != null
                && !versionResponse.getIsOutOfRange()
                && config.getFetchHistoryData()) {
                copyMode = DeltaCheckpoint.StateType.FULL_COPY;
            }
            // create minCheckpoint according to given start version
            minCheckpoint = new DeltaCheckpoint(copyMode, startVersion);
            checkpointMap.put(MIN_CHECKPOINT_KEY, minCheckpoint);

            // get deltaSchema, if failed, get pulsar schema.
            try {
                deltaSchema = reader.getSnapShot(startVersion).getMetadata().getSchema();
            } catch (Exception e) {
                log.error("getSchema from delta lake table snapshot {} failed, ", startVersion, e);
                pulsarSchema = handleGetDeltaSchemaFailed(sourceContext);
            }

            //use minCheckpoint to init each partition's checkpoint map.
            for (int id : partitions) {
                log.info("checkpointMap not including partition: {}, will start from version: {}",
                    id, startVersion);
                checkpointMap.put(id, minCheckpoint);
            }
        } else if (minCheckpoint != null) { // if restored checkpoint from state store.
            // validate the minCheckpoint snapshot version
            long startVersion =
                    reader.getAndValidateSnapShotVersion(minCheckpoint.getSnapShotVersion())
                        .getVersion();
            if (startVersion > minCheckpoint.getSnapShotVersion()) {
                log.error("checkpoint version: {} not exist, current version {}",
                        minCheckpoint.getSnapShotVersion(), startVersion);
                throw new IOException("last checkpoint version not exist, "
                    + "need to handle this manually");
            }

            checkpointMap.put(MIN_CHECKPOINT_KEY, minCheckpoint);

            // get delta schema from delta lake
            try {
                deltaSchema = reader.getSnapShot(startVersion).getMetadata().getSchema();
            } catch (Exception e) {
                log.error("getSchema from snapshot {} failed, ", startVersion, e);
                pulsarSchema = handleGetDeltaSchemaFailed(sourceContext);
            }

            for (int id : partitions) {
                if (!checkpointMap.containsKey(id)) {
                    log.warn("checkpointMap not including partition {}, "
                            + "will use default checkpoint {}", id, minCheckpoint);
                    checkpointMap.put(id, minCheckpoint);
                }
            }
        }

        return Triple.of(checkpointMap, deltaSchema, pulsarSchema);
    }

    public static Function<DeltaReader.ReadCursor, Boolean>
                            initDeltaReadFilter(Map<Integer, DeltaCheckpoint> checkpointMap,
                            int topicPartitionNum) {
        return (readCursor) -> {
            if (readCursor == null) {
                log.info("readCursor is null, return true");
                return true;
            }
            int slot =
                DeltaReader.getPartitionIdByDeltaPartitionValue(readCursor.getPartitionValue(),
                topicPartitionNum);
            DeltaCheckpoint s = checkpointMap.get(slot);
            if (s == null) {
                // no checkpoint before means there are no record before, no need to filter
                log.info("checkpoint for partition {} {} is missing", checkpointMap, slot);
                return true;
            }

            DeltaCheckpoint newCheckPoint = null;
            if (readCursor.isFullSnapShot()) {
                newCheckPoint = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY,
                    readCursor.getVersion());
            } else {
                newCheckPoint = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY,
                    readCursor.getVersion());
            }
            newCheckPoint.setMetadataChangeFileIndex(readCursor.getChangeIndex());
            newCheckPoint.setRowNum(readCursor.getRowNum());

            if (newCheckPoint.getRowNum() >= 0 && newCheckPoint.compareTo(s) >= 0) {
                //row filter
                return true;
            }

            return newCheckPoint.getRowNum() < 0 && newCheckPoint.compareVersionAndIndex(s) >= 0;
            //action filter
        };
    }

}
