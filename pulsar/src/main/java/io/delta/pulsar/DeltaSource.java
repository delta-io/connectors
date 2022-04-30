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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import io.delta.pulsar.source.DeltaRecord;
import io.delta.pulsar.source.DeltaSourceConfig;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;



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

    // delta lake schema, when delta lake schema changes, this schema will change
    private LinkedBlockingQueue<DeltaRecord> queue;

    // metrics
    private final AtomicInteger processingException = new AtomicInteger(0);
    private static long checkpointId = 0;


    @Override
    public void open(Map<String, Object> configMap, SourceContext sourceContext) throws Exception {
        if (config != null) {
            log.error("The connector is already running, exit!");
            throw new IllegalStateException("The connector is already running, exit!");
        }

        this.sourceContext = sourceContext;
        this.outputTopic = sourceContext.getOutputTopic();
        this.topicPartitionNum =
            sourceContext.getPulsarClient().getPartitionsForTopic(outputTopic).get().size();

        // load the configuration and validate it
        this.config = DeltaSourceConfig.load(configMap);
        log.info("Delta Lake connector config: {}", config);
        config.validate();

        queue = new LinkedBlockingQueue<>(config.getQueueSize());

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
        if (config.getCheckpointInterval() <= 0) {
            log.info("Due to checkpointInterval: {}, disable checkpoint.",
                config.getCheckpointInterval());
            return;
        }
        // TODO Add checkpoint thread
    }

    @Override
    public Record<GenericRecord> read() throws Exception {
        if (this.processingException.get() > 0) {
            log.error("processing encounter exception will stop reading record "
                + "and connector will exit");
            throw new Exception("processing exception in processing delta record");
        }
        return queue.take();
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
}
