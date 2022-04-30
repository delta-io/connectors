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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.pulsar.common.Murmur32Hash;
import io.delta.pulsar.parquet.DeltaParquetReader;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.Type;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.io.core.SourceContext;
import static io.delta.pulsar.DeltaLakeConnectorStats.FILTERED_FILES;
import static io.delta.pulsar.DeltaLakeConnectorStats.PREPARE_READ_FILES_BYTES_SIZE;
import static io.delta.pulsar.DeltaLakeConnectorStats.PREPARE_READ_FILES_COUNT;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.CommitInfo;
import io.delta.standalone.actions.FileAction;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.RemoveFile;
import io.delta.standalone.exceptions.DeltaStandaloneException;


/**
 * The delta reader for {@link DeltaReaderThread}.
 */
@Data
@Slf4j
public class DeltaReader {
    private DeltaCheckpoint startCheckpoint;
    private DeltaLog deltaLog;
    private Function<ReadCursor, Boolean> filter;
    protected static int topicPartitionNum;
    private DeltaSourceConfig config;
    private Configuration conf;
    protected static long filteredCnt = 0;


    public static int getPartitionIdByDeltaPartitionValue(String partitionValue,
                                                          int topicPartitionNum) {
        if (partitionValue == null) {
            partitionValue = "";
        }
        if (topicPartitionNum == 0) {
            return 0;
        }
        return Murmur32Hash.getInstance().makeHash(partitionValue.getBytes(StandardCharsets.UTF_8))
                % topicPartitionNum;
    }

    /**
     * The ReadCursor is used to describe the read position.
     */
    @Data
    public static class ReadCursor implements Cloneable {
        Action act;
        long version;
        boolean isFullSnapShot;
        long changeIndex;
        long rowNum;
        boolean endOfFile;
        boolean endOfVersion;
        String partitionValue;

        public ReadCursor(Action act, long version, boolean isFullSnapShot,
                          long changeIndex, String partitionValue) {
            this.act = act;
            this.version = version;
            this.isFullSnapShot = isFullSnapShot;
            this.changeIndex = changeIndex;
            this.partitionValue = partitionValue;
            this.rowNum = -1;
        }

        @Override
        protected Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        public static ObjectMapper jsonMapper() {
            return ObjectMapperFactory.getThreadLocal();
        }

        @Override
        public String toString() {
            try {
                return jsonMapper().writeValueAsString(this);
            } catch (JsonProcessingException e) {
                log.error("Failed to write DeltaLakeConnectorConfig ", e);
                return "";
            }
        }
    }

    /**
     * The RowRecordData is used to contain the data and next read position.
     */
    @Data
    public static class RowRecordData {
        ReadCursor nextCursor;
        SimpleGroup simpleGroup;
        List<Type> parquetSchema;

        public RowRecordData(ReadCursor nextCursor, SimpleGroup simpleGroup) {
            this.nextCursor = nextCursor;
            this.simpleGroup = simpleGroup;
        }

        public RowRecordData(ReadCursor nextCursor, SimpleGroup simpleGroup,
                             List<Type> parquetSchema) {
            this.nextCursor = nextCursor;
            this.simpleGroup = simpleGroup;
            this.parquetSchema = parquetSchema;
        }
    }

    public DeltaReader(DeltaSourceConfig config, int topicPartitionNum)
        throws Exception {
        this.config = config;
        DeltaReader.topicPartitionNum = topicPartitionNum;
        open();
    }

    /**
     * The VersionResponse is used to contain version and outOfRange flag.
     */
    @Data
    public static class VersionResponse {
        Long version;
        Boolean isOutOfRange;

        public VersionResponse(Long version, Boolean isOutOfRange) {
            this.version = version;
            this.isOutOfRange = isOutOfRange;
        }
    }

    public VersionResponse getSnapShotVersionFromTimeStamp(long timeStamp) {
        try {
            Snapshot snapshot = deltaLog.getSnapshotForTimestampAsOf(timeStamp);
            return new VersionResponse(snapshot.getVersion(), false);
        } catch (IllegalArgumentException | DeltaStandaloneException e) {
            log.warn("timestamp  {} is not exist is delta lake, will use the latest version ",
                timeStamp, e);
            return updateAndGetSnapshotVersion();
        }
    }

    public VersionResponse getAndValidateSnapShotVersion(long snapShotVersion) {
        try {
            if (snapShotVersion == -1) {
                log.warn("snapShotVersion {} is not exist is delta lake, "
                        + "will use the latest version.",
                    snapShotVersion);
                return updateAndGetSnapshotVersion();
            }
            return new
                VersionResponse(deltaLog.getSnapshotForVersionAsOf(snapShotVersion).getVersion(),
                false);
        } catch (IllegalArgumentException | DeltaStandaloneException e) {
            log.error("snapShotVersion {} is not exist is delta lake, will use the latest version",
                snapShotVersion);
            return updateAndGetSnapshotVersion();
        }
    }

    private VersionResponse updateAndGetSnapshotVersion() {
        try {
            Snapshot snapshot = deltaLog.update();
            long version = snapshot.getVersion();
            log.info("Use the latest version: {}", version);
            return new VersionResponse(version, false);
        } catch (Exception e) {
            log.warn("get latest snapshot version failed, use 0 instead ", e);
            return new VersionResponse(0L, true);
        }
    }

    public Snapshot getSnapShot(long snapShotVersion) {
        return deltaLog.getSnapshotForVersionAsOf(snapShotVersion);
    }

    public CompletableFuture<List<ReadCursor>> getDeltaActionFromSnapShotVersionAsync(
        long startVersion, long maxBytesSize, boolean isFullSnapshot, SourceContext sourceContext) {
        CompletableFuture<List<ReadCursor>> promise = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                List<ReadCursor> readCursorList =
                        getDeltaActionFromSnapShotVersion(startVersion, maxBytesSize,
                            isFullSnapshot, sourceContext);
                promise.complete(readCursorList);
            } catch (IllegalArgumentException | IllegalStateException e) {
                promise.completeExceptionally(e);
            }
        });
        return promise;
    }

    public List<ReadCursor> getDeltaActionFromSnapShotVersion(long startVersion,
                                                              long maxBytesSize,
                                                              boolean isFullSnapshot,
                                                              SourceContext sourceContext) {
        List<ReadCursor> actionList = new LinkedList<>();
        long totalReadBytes = 0;
        if (isFullSnapshot) {
            Snapshot snapshot = deltaLog.getSnapshotForVersionAsOf(startVersion);
            List<AddFile> addFiles = snapshot.getAllFiles();
            log.info("getFullSnapshot allAddFile: {}, startVersion: {}",
                addFiles.size(), startVersion);
            for (int i = 0; i < addFiles.size(); i++) {
                AddFile addFile = addFiles.get(i);
                String partitionValue = partitionValueToString(addFile.getPartitionValues());
                ReadCursor cursor = new ReadCursor(addFile, startVersion,
                    true, i, partitionValue);
                if (filter.apply(cursor)) {
                    actionList.add(cursor);
                    totalReadBytes += addFile.getSize();
                } else {
                    sourceContext.recordMetric(FILTERED_FILES, ++filteredCnt);
                }
            }
        } else {
            long prevVersion = startVersion;
            long start = System.currentTimeMillis();
            Iterator<VersionLog> vlogs = deltaLog.getChanges(startVersion, false);
            while (vlogs.hasNext()) {
                VersionLog v = vlogs.next();
                if (v.getVersion() > prevVersion && totalReadBytes >= maxBytesSize) {
                    long end = System.currentTimeMillis();
                    log.debug("get changes: {}, cost: {}ms, total: {} actions",
                            v.getVersion(), end - start, actionList.size());
                    break;
                }
                prevVersion = v.getVersion();
                List<Action> actions = v.getActions();
                for (int i = 0; i < actions.size(); i++) {
                    Action action = actions.get(i);
                    if (action instanceof AddFile) {
                        AddFile addFile = (AddFile) action;
                        String partitionValue =
                            partitionValueToString(addFile.getPartitionValues());
                        ReadCursor cursor = new ReadCursor(addFile, v.getVersion(),
                            false, i, partitionValue);
                        Boolean matchFlag = filter.apply(cursor);
                        log.info("getChanges version: {}, index: {}, "
                                        + "actionListSize: {}, addFile: {}, dataChange: {}, "
                                + "partitionValue: {}, "
                                        + "isMatch: {}, modifiedTime: {}, fileSize: {}",
                                v.getVersion(), i, actions.size(), addFile.getPath(),
                                addFile.isDataChange(),
                                partitionValue,
                                matchFlag,
                                addFile.getModificationTime(),
                                addFile.getSize());
                        if (matchFlag) {
                            actionList.add(cursor);
                            totalReadBytes += addFile.getSize();
                        } else {
                            sourceContext.recordMetric(FILTERED_FILES, ++filteredCnt);
                        }
                    } else if (action instanceof CommitInfo) {
                        CommitInfo info = (CommitInfo) action;
                        log.debug("getChanges skip commitInfo version: {}, "
                                + "index: {}, Operation: {}, "
                                        + "operationParam: {}, modifiedTime: {}, "
                                + "operationMetrics: {}",
                                startVersion, i, info.getOperation(),
                                info.getOperationParameters(),
                                info.getTimestamp(),
                                info.getOperationMetrics());
                    } else if (action instanceof RemoveFile) {
                        RemoveFile removeFile = (RemoveFile) action;
                        String partitionValue =
                            partitionValueToString(removeFile.getPartitionValues());
                        ReadCursor cursor = new ReadCursor(removeFile, v.getVersion(),
                            false, i, partitionValue);
                        Boolean matchFlag = filter.apply(cursor);
                        log.debug("getChanges version: {}, index: {}, "
                                + "removeFile: {}, dataChange: {}, "
                                + "partitionValue: {}, isMatch: {} deletionTime: {}, fileSize: {}",
                                v.getVersion(), i, removeFile.getPath(),
                                removeFile.isDataChange(),
                                partitionValue, matchFlag,
                                removeFile.getDeletionTimestamp(),
                                removeFile.getSize());
                        if (matchFlag) {
                            actionList.add(cursor);
                            if (removeFile.getSize().isPresent()) {
                                totalReadBytes += removeFile.getSize().get();
                            }
                        } else {
                            sourceContext.recordMetric(FILTERED_FILES, ++filteredCnt);
                        }
                    } else if (action instanceof Metadata){
                        Metadata meta = (Metadata) action;
                        log.info("getChanges version: {}, index: {}, "
                                + "metadataChange schema: {}, partitionColum: {},"
                                + " format: {}, createTime: {}",
                            v.getVersion(), i, meta.getSchema().getTreeString(),
                            meta.getPartitionColumns(),
                            meta.getFormat(),
                            meta.getCreatedTime());
                        ReadCursor cursor = new ReadCursor(meta, v.getVersion(),
                            false, i, "");
                        actionList.add(cursor);
                    } else {
                        log.info("getChanges skip unknown change type: {}", action);
                    }
                }
            }
        }

        // expose metrics
        sourceContext.recordMetric(PREPARE_READ_FILES_COUNT, actionList.size());
        sourceContext.recordMetric(PREPARE_READ_FILES_BYTES_SIZE, totalReadBytes);

        return actionList;
    }

    public int getMaxConcurrency(List<ReadCursor> actionList, int baseIndex) {
        long currentRecordsNum = 0;
        long currentReadBytesSize = 0;
        int maxConcurrency = 0;
        for (int i = baseIndex; i < actionList.size()
            && currentRecordsNum < config.maxReadRowCountOneRound
            && currentReadBytesSize < config.maxReadBytesSizeOneRound; i++) {
            Action act = actionList.get(i).act;
            if (act instanceof AddFile || act instanceof RemoveFile) {
                String filePath = config.tablePath + "/" + ((FileAction) act).getPath();
                try {
                    // calculate records number
                    currentRecordsNum += DeltaParquetReader.getRowNum(filePath, conf);

                    // calculate read bytes size
                    if (act instanceof AddFile) {
                        currentReadBytesSize += ((AddFile) act).getSize();
                    } else {
                        RemoveFile removeFile = (RemoveFile) act;
                        if (removeFile.getSize().isPresent()) {
                            currentReadBytesSize += removeFile.getSize().get();
                        }
                    }

                    if (currentRecordsNum < config.maxReadRowCountOneRound
                        || currentReadBytesSize < config.getMaxReadBytesSizeOneRound()
                        || maxConcurrency == 0) {
                        maxConcurrency++;
                    }
                } catch (IOException e) {
                    log.error("Failed to get file records info. ", e);
                    return -1;
                }
            }
        }
        return maxConcurrency;
    }


    public CompletableFuture<List<RowRecordData>> readParquetFileAsync(ReadCursor startCursor,
                                         ExecutorService executorService,
                                         AtomicInteger readStatus) {
        return CompletableFuture.supplyAsync(()-> {
            List<RowRecordData> recordData = new ArrayList<>();
            Action action = startCursor.act;
            if (action instanceof AddFile || action instanceof RemoveFile) {
                String filePath = config.tablePath + "/" + ((FileAction) action).getPath();
                DeltaParquetReader reader = new DeltaParquetReader();
                try {
                    reader.open(filePath, conf);
                } catch (IOException e) {
                    log.error("Open parquet file {} failed ", filePath, e);
                    readStatus.set(-1);
                    return recordData;
                }

                int rowNumInParquetFile = 0;
                try {
                    DeltaParquetReader.Parquet parquet;
                    while ((parquet = reader.readBatch(config.maxReadRowCountOneRound)) != null) {
                        for (int i = 0; i < parquet.getData().size(); i++) {
                            ReadCursor cursor;
                            try {
                                cursor = (ReadCursor) startCursor.clone();
                            } catch (CloneNotSupportedException e) {
                                return recordData;
                            }
                            cursor.rowNum = rowNumInParquetFile++;
                            if (i == parquet.getData().size() - 1) {
                                cursor.endOfFile = true;
                            }
                            if (filter.apply(cursor)) {
                                recordData.add(new RowRecordData(cursor, parquet.getData().get(i),
                                    parquet.getSchema()));
                            }
                        }
                    }
                    readStatus.set(1);
                } catch (IOException e) {
                    log.error("readPartParquetFileAsync readBatch encounter exception,", e);
                    readStatus.set(-1);
                } finally {
                    try {
                        reader.close();
                    } catch (IOException e2) {
                        log.error("readPartParquetFileAsync close encounter exception,", e2);
                        readStatus.set(-1);
                    }
                }
            } else if (action instanceof CommitInfo) {
                recordData.add(new RowRecordData(startCursor, null));
            }

            return recordData;
        }, executorService);
    }

    public static String partitionValueToString(Map<String, String> partitionValue) {
        TreeMap<String, String> treemap = new TreeMap<>(partitionValue);
        StringBuilder builder = new StringBuilder();
        treemap.forEach((key, value) -> {
            builder.append(key)
                .append("=")
                .append(value)
                .append(",");
        }); // TODO  how to parse the string

        if (builder.length() > 0) {
            builder.deleteCharAt(builder.length() - 1);
        }
        return builder.toString();
    }

    private void open() throws Exception {
        conf = new Configuration();
        deltaLog = DeltaLog.forTable(conf, this.config.tablePath);
    }
}
