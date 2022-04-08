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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;
import io.delta.pulsar.DeltaSource;
import io.delta.pulsar.parquet.DeltaParquetReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.io.core.SourceContext;
import static io.delta.pulsar.DeltaLakeConnectorStats.CURRENT_READ_DELTA_TABLE_VERSION;
import static io.delta.pulsar.DeltaLakeConnectorStats.FETCH_ADN_PARSE_FILE_LATENCY;
import static io.delta.pulsar.DeltaLakeConnectorStats.GET_DELTA_ACTION_LATENCY;
import static io.delta.pulsar.DeltaLakeConnectorStats.MAX_READ_FILES_CONCURRENCY;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.CommitInfo;
import io.delta.standalone.actions.FileAction;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.RemoveFile;
import io.delta.standalone.types.StructType;

/**
 * The delta reader thread class for {@link DeltaSource}.
 *
 */
@Slf4j
public class DeltaReaderThread extends Thread {
    private volatile boolean running;
    private final ExecutorService parquetParseExecutor;
    private final AtomicInteger processingException;
    private final DeltaReader reader;
    private final DeltaSourceConfig config;
    private final LinkedBlockingQueue<DeltaRecord> queue;
    private StructType deltaSchema;
    private final GenericSchema<GenericRecord> pulsarSchema;
    private final String topic;
    private final DeltaCheckpoint checkpoint;
    // used for expose user defined metrics
    private final SourceContext sourceContext;

    public DeltaReaderThread(DeltaReader reader,
                             ExecutorService executor,
                             DeltaSourceConfig config,
                             LinkedBlockingQueue<DeltaRecord> queue,
                             StructType deltaSchema,
                             GenericSchema<GenericRecord> pulsarSchema,
                             String topic,
                             AtomicInteger processingException,
                             DeltaCheckpoint checkpoint,
                             SourceContext sourceContext) {
        this.reader = reader;
        this.config = config;
        this.queue = queue;
        this.deltaSchema = deltaSchema;
        this.running = false;
        this.parquetParseExecutor = executor;
        this.processingException = processingException;
        this.checkpoint = checkpoint;
        this.pulsarSchema = pulsarSchema;
        this.topic = topic;
        this.sourceContext = sourceContext;
    }

    @Override
    public void run() {
        long startVersion;
        long nextVersion = checkpoint.getSnapShotVersion();
        CompletableFuture<List<DeltaReader.ReadCursor>> actionListFuture = null;
        List<DeltaReader.ReadCursor> actionList;
        boolean isFullCopyPeriod = checkpoint.isFullCopy();

        running = true;
        long preReadActionTimestamp = System.currentTimeMillis();

        while (running) {
            try {
                startVersion = nextVersion;
                sourceContext.recordMetric(CURRENT_READ_DELTA_TABLE_VERSION, startVersion);

                if (log.isDebugEnabled()) {
                    log.debug("Begin to read version {} ", startVersion);
                }

                if (actionListFuture == null) {
                    preReadActionTimestamp = System.currentTimeMillis();
                    actionList = reader.getDeltaActionFromSnapShotVersion(
                        startVersion, config.maxReadBytesSizeOneRound,
                        isFullCopyPeriod, sourceContext);
                } else {
                    actionList = actionListFuture.get();
                }
                sourceContext.recordMetric(GET_DELTA_ACTION_LATENCY,
                    System.currentTimeMillis() - preReadActionTimestamp);
                actionListFuture = null;

                if (actionList == null || actionList.isEmpty()) {
                    if (startVersion == checkpoint.getSnapShotVersion()
                        && checkpoint.getMetadataChangeFileIndex() >= 0
                        && checkpoint.getRowNum() >= 0) {
                        nextVersion = startVersion + 1;
                        log.info("read end of the delta version {}, will go to next version {}",
                                startVersion, nextVersion);
                    } else {
                        log.debug("Read from version: {} not find any delta actions,"
                                + " wait to get actions next round",
                                startVersion);
                        try {
                            Thread.sleep(10_000);
                        } catch (InterruptedException e) {
                            //
                        }
                    }
                    continue;
                }

                // get the last action snapshot version as the nextVersion
                nextVersion = Iterables.getLast(actionList).getVersion() + 1;
                if (isFullCopyPeriod) {
                    log.info("Change from fullCopy mode to incrCopy mode, incrCopy version {}",
                        nextVersion);
                    isFullCopyPeriod = false;
                }
                // pre read next batch records
                preReadActionTimestamp = System.currentTimeMillis();
                actionListFuture = reader.getDeltaActionFromSnapShotVersionAsync(nextVersion,
                        config.maxReadBytesSizeOneRound, isFullCopyPeriod, sourceContext);

                int base = 0;
                while (base < actionList.size()) {
                    int concurrency = reader.getMaxConcurrency(actionList, base);
                    sourceContext.recordMetric(MAX_READ_FILES_CONCURRENCY, concurrency);

                    if (log.isDebugEnabled()) {
                        log.debug("totalActionSize: {}, concurrency: {}, base: {}",
                            actionList.size(), concurrency, base);
                    }

                    long startFetchAndParse = System.currentTimeMillis();
                    if (concurrency > 1) { // read multiple files
                        // https://stackoverflow.com/questions/19348248/waiting-on-a-list-of-future
                        CompletionService<List<DeltaReader.RowRecordData>> completionService =
                            new ExecutorCompletionService<>(parquetParseExecutor);
                        int taskCnt = 0;
                        for (int i = 0; i < concurrency; ++i) {
                            DeltaReader.ReadCursor readCursor = actionList.get(base + i);
                            if (readCursor.act instanceof Metadata) {
                                deltaSchema = ((Metadata) readCursor.act).getSchema();
                                continue;
                            }
                            completionService.submit(
                                new ReadTotalParquetFileTask(config, readCursor, reader));
                            taskCnt++;
                        }

                        int receivedTasks = 0;
                        boolean errors = false;
                        long totalSize = 0;
                        while (receivedTasks < taskCnt && !errors) {
                            Future<List<DeltaReader.RowRecordData>> recordFuture =
                                completionService.take();
                            try {
                                List<DeltaReader.RowRecordData> recordData = recordFuture.get();
                                receivedTasks++;
                                totalSize += recordData.size();
                                recordData.forEach(this::enqueue);
                            } catch (Exception e) {
                                log.error("Failed to get records from parquet file", e);
                                errors = true;
                            }
                        }

                        if (errors) {
                            // TODO deal with exception
                        }

                        if (log.isDebugEnabled()) {
                            log.debug("Parse all parquet files cost: {} ms, "
                                    + "total record: {} total files: {}",
                                System.currentTimeMillis() - startFetchAndParse,
                                totalSize, receivedTasks);
                        }
                        sourceContext.recordMetric(FETCH_ADN_PARSE_FILE_LATENCY,
                            System.currentTimeMillis() - startFetchAndParse);
                    } else {
                        AtomicInteger readStatus = new AtomicInteger(0);
                        if (actionList.get(base).act instanceof CommitInfo) {
                            base = base + concurrency;
                            continue;
                        }
                        List<DeltaReader.RowRecordData> recordData =
                            reader.readParquetFileAsync(actionList.get(base),
                                parquetParseExecutor, readStatus).get();
                        recordData.forEach(this::enqueue);

                        if (readStatus.get() < 0) {
                            log.error("readPartParquetFileAsync encounter exception, "
                                + "will skip read");
                            throw new IOException("readPartParquetFileAsync failed");
                            // TODO define exception
                        }
                        sourceContext.recordMetric(FETCH_ADN_PARSE_FILE_LATENCY,
                            System.currentTimeMillis() - startFetchAndParse);
                    }
                    base = base + concurrency;
                }

            } catch (Exception e) { // TODO format exceptions
                log.error("read data from delta lake error, will mark processingException", e);
                close();
                this.processingException.incrementAndGet();
            }
        }
    }

    static class ReadTotalParquetFileTask implements Callable<List<DeltaReader.RowRecordData>> {
        private final DeltaSourceConfig config;
        private final DeltaReader.ReadCursor cursor;
        private final Configuration configuration;
        private final DeltaReader reader;

        ReadTotalParquetFileTask(DeltaSourceConfig config,
                                 DeltaReader.ReadCursor cursor,
                                 DeltaReader reader) {
            this.config = config;
            this.cursor = cursor;
            this.configuration = reader.getConf();
            this.reader = reader;
        }

        @Override
        public List<DeltaReader.RowRecordData> call() throws Exception {
            Action action = cursor.act;
            List<DeltaReader.RowRecordData> rowRecordDataList = new ArrayList<>();
            if (action instanceof AddFile || action instanceof RemoveFile) {
                String path = config.tablePath + "/" + ((FileAction) action).getPath();
                long start = System.currentTimeMillis();
                DeltaParquetReader.Parquet parquet;
                try {
                    parquet = DeltaParquetReader.getTotalParquetData(path, configuration);
                } catch (IOException e) {
                    throw new IOException(e);
                }

                if (log.isDebugEnabled()) {
                    log.debug("Read and parse parquet file {} const {} ms",
                        path, System.currentTimeMillis() - start);
                }

                for (int i = 0; i < parquet.getData().size(); ++i) {
                    DeltaReader.ReadCursor readCursor;
                    try {
                        readCursor = (DeltaReader.ReadCursor) cursor.clone();
                    } catch (CloneNotSupportedException e) {
                        throw new IOException(e);
                    }
                    readCursor.rowNum = i;
                    if (i == parquet.getData().size() - 1) {
                        readCursor.endOfFile = true;
                    }

                    if (reader.getFilter().apply(readCursor)) {
                        rowRecordDataList.add(new DeltaReader.RowRecordData(readCursor,
                            parquet.getData().get(i), parquet.getSchema()));
                    }
                }
            } else if (action instanceof CommitInfo) {
                rowRecordDataList.add(new DeltaReader.RowRecordData(cursor, null));
            }

            return rowRecordDataList;
        }

    }

    public void enqueue(DeltaReader.RowRecordData rowRecordData) {
        try {
            if (deltaSchema != null) {
                queue.put(new DeltaRecord(rowRecordData, topic, deltaSchema, null,
                    processingException));
            } else if (pulsarSchema != null) {
                queue.put(new DeltaRecord(rowRecordData, topic, null, pulsarSchema,
                    processingException));
            } else {
                // TODO
            }
        } catch (IOException ex) {
            log.error("delta message enqueue failed for ", ex);
            processingException.incrementAndGet();
        } catch (InterruptedException interruptedException) {
            log.error("delta message enqueue interrupted", interruptedException);
            processingException.incrementAndGet();
        } catch (NullPointerException npe) { // will not happen
            log.error("delta message enqueue failed, ", npe);
            processingException.incrementAndGet();
        }
    }

    public void close() {
        running = false;
    }
}
