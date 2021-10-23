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

package org.apache.flink.connector.delta.sink.committer;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.SetTransaction;
import io.delta.standalone.types.StructType;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.connector.delta.sink.SchemaConverter;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaPendingFile;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DeltaGlobalCommitter implements GlobalCommitter<DeltaCommittable, DeltaGlobalCommittable> {

    private final String engineInfo = "flink.1.12"; // TODO parametrize

    private final Configuration conf;

    private final Path basePath;

    private final RowType rowType;

    private final boolean canOverwriteSchema;

    public DeltaGlobalCommitter(Configuration conf,
                                Path basePath,
                                RowType rowType,
                                boolean canOverwriteSchema) {
        this.conf = conf;
        this.basePath = basePath;
        this.rowType = rowType;
        this.canOverwriteSchema = canOverwriteSchema;
    }


    @Override
    public List<DeltaGlobalCommittable> filterRecoveredCommittables(List<DeltaGlobalCommittable> globalCommittables) {
        return globalCommittables;
    }

    @Override
    public DeltaGlobalCommittable combine(List<DeltaCommittable> committables) {
        return new DeltaGlobalCommittable(committables);
    }

    private String resolveAppId(List<DeltaGlobalCommittable> globalCommittables) {
        for (DeltaGlobalCommittable globalCommittable : globalCommittables) {
            for (DeltaCommittable deltaCommittable : globalCommittable.getDeltaCommittables()) {
                return deltaCommittable.getAppId();
            }
        }
        return null; // to do throw custom exception here
    }

    @Override
    public List<DeltaGlobalCommittable> commit(List<DeltaGlobalCommittable> globalCommittables) {
        if (!globalCommittables.isEmpty()) {
            String appId = resolveAppId(globalCommittables);
            Map<Long, List<Action>> addFileActionsPerCheckpoint = prepareAddFileActionsPerCheckpoint(globalCommittables);
            DeltaLog deltaLog = DeltaLog.forTable(conf, basePath.getPath());

            for (long checkpointId : addFileActionsPerCheckpoint.keySet().stream().sorted().collect(Collectors.toList())) {
                OptimisticTransaction transaction = deltaLog.startTransaction();
                long lastCommittedVersion = transaction.txnVersion(appId);
                if (checkpointId > lastCommittedVersion) {
                    handleMetadataUpdate(transaction);
                    List<Action> actions = prepareAllActions(appId, checkpointId, addFileActionsPerCheckpoint);
                    Operation operation = prepareDeltaLogOperation(globalCommittables, addFileActionsPerCheckpoint.get(checkpointId).size());

                    transaction.commit(actions, operation, this.engineInfo);
                }
            }

        }
        return Collections.emptyList();
    }

    private void handleMetadataUpdate(OptimisticTransaction transaction) {
        Metadata metadataAction = prepareMetadata();
        if (!metadataAction.getSchema().toJson().equals(transaction.metadata().getSchema().toJson())) {
            if (canOverwriteSchema) {
                transaction.updateMetadata(metadataAction);
            } else {
                throw new RuntimeException();
            }
        }
        // schemas are matching so do nothing. We do not need to include the metadata in the commit actions
    }

    private List<Action> prepareAllActions(String appId,
                                           long checkpointId,
                                           Map<Long, List<Action>> addFileActionsPerCheckpoint) {
        List<Action> actions = new ArrayList<>();

        SetTransaction setTransaction = new SetTransaction(appId, checkpointId, Optional.of(System.currentTimeMillis()));
        actions.add(setTransaction);

        List<Action> addFileActions = addFileActionsPerCheckpoint.get(checkpointId);
        actions.addAll(addFileActions);
        return actions;
    }

    private Metadata prepareMetadata() {
        StructType dataStreamSchema = new SchemaConverter().toDeltaFormat(rowType);
        return new Metadata.Builder().schema(dataStreamSchema).build();
    }

    private Operation prepareDeltaLogOperation(List<DeltaGlobalCommittable> globalCommittables,
                                               int numAddedFiles) {
        Map<String, String> operationMetrics = prepareOperationMetrics(globalCommittables, numAddedFiles);
        Map<String, Object> operationParameters = Collections.emptyMap(); // TODO
        return new Operation(Operation.Name.STREAMING_UPDATE, operationParameters, operationMetrics);
    }

    private Map<Long, List<Action>> prepareAddFileActionsPerCheckpoint(List<DeltaGlobalCommittable> globalCommittables) {
        Map<Long, List<Action>> addFilesPerCheckpoint = new HashMap<>();
        for (DeltaGlobalCommittable globalCommittable : globalCommittables) {
            for (DeltaCommittable deltaCommittable : globalCommittable.getDeltaCommittables()) {
                if (deltaCommittable.hasDeltaPendingFile()) {
                    DeltaPendingFile deltaPendingFile = deltaCommittable.getDeltaPendingFile();

                    if (!addFilesPerCheckpoint.containsKey(deltaCommittable.getCheckpointId())) {
                        addFilesPerCheckpoint.put(deltaCommittable.getCheckpointId(), new ArrayList<>());
                    }

                    AddFile action = convertDeltaPendingFileToAddFileAction(deltaPendingFile);

                    addFilesPerCheckpoint.get(deltaCommittable.getCheckpointId()).add(action);
                }
            }
        }
        return addFilesPerCheckpoint;
    }

    private AddFile convertDeltaPendingFileToAddFileAction(DeltaPendingFile deltaPendingFile) {
        Map<String, String> partitionValues = Collections.emptyMap(); // TODO
        long modificationTime = deltaPendingFile.getLastUpdateTime();
        return new AddFile(deltaPendingFile.getFileName(), partitionValues, deltaPendingFile.getFileSize(), modificationTime, true, null, null);

    }

    private Map<String, String> prepareOperationMetrics(List<DeltaGlobalCommittable> globalCommittables,
                                                        int numAddedFiles) {
        long cumulatedRecordCount = 0;
        long cumulatedSize = 0;
        for (DeltaGlobalCommittable globalCommittable : globalCommittables) {
            for (DeltaCommittable deltaCommittable : globalCommittable.getDeltaCommittables()) {
                if (deltaCommittable.hasDeltaPendingFile()) {
                    DeltaPendingFile deltaPendingFile = deltaCommittable.getDeltaPendingFile();
                    cumulatedRecordCount += deltaPendingFile.getRecordCount();
                    cumulatedSize += deltaPendingFile.getFileSize();
                }
            }
        }

        Map<String, String> operationMetrics = new HashMap<>();
        operationMetrics.put("numRemovedFiles", "0"); // TODO
        operationMetrics.put("numAddedFiles", String.valueOf(numAddedFiles));
        operationMetrics.put("numOutputRows", String.valueOf(cumulatedRecordCount));
        operationMetrics.put("numOutputBytes", String.valueOf(cumulatedSize));

        return operationMetrics;
    }

    @Override
    public void endOfInput() throws IOException {

    }

    @Override
    public void close() throws Exception {

    }



}
