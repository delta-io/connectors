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

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link GlobalCommitter} implementation for {@link org.apache.flink.connector.delta.sink.DeltaSink}.
 * <p>
 * It commits written files to the DeltaLog and provides exactly once semantics by guaranteeing idempotence
 * behaviour of the commit phase. It means that when given the same set of {@link DeltaCommittable} objects
 * (that contain metadata about written files along with unique identifier of the given Flink's job and checkpoint id)
 * it will never commit them multiple times. Such behaviour is achieved by constructing transactional id using
 * mentioned app identifier and checkpointId.
 */
public class DeltaGlobalCommitter implements GlobalCommitter<DeltaCommittable, DeltaGlobalCommittable> {

    private static final String APPEND_MODE = "Append";
    private static final String ENGINE_INFO = "flink.1.12"; // TODO parametrize

    /**
     * Hadoop configuration that is passed to {@link DeltaLog} instance when creating it
     */
    private final Configuration conf;

    /**
     * Root path of the DeltaTable
     */
    private final Path basePath;

    /**
     * RowType object from which the Delta's {@link StructType} will be deducted
     */
    private final RowType rowType;

    /**
     * Indicator whether the committer should try to commit unmatching schema
     */
    private final boolean canTryUpdateSchema;

    public DeltaGlobalCommitter(Configuration conf,
                                Path basePath,
                                RowType rowType,
                                boolean canTryUpdateSchema) {
        this.conf = conf;
        this.basePath = basePath;
        this.rowType = rowType;
        this.canTryUpdateSchema = canTryUpdateSchema;
    }


    @Override
    public List<DeltaGlobalCommittable> filterRecoveredCommittables(List<DeltaGlobalCommittable> globalCommittables) {
        return globalCommittables;
    }

    @Override
    public DeltaGlobalCommittable combine(List<DeltaCommittable> committables) {
        return new DeltaGlobalCommittable(committables);
    }

    /**
     * Resolves appId param from the first committable object. It does not matter which object as all committables carry the same appId value.
     * It's ok to return null value here as it would mean that there are no committables (aka no stream events were received) for given checkpoint.
     *
     * @param globalCommittables list of global committables objects
     * @return unique app identifier for given Flink job
     */
    @Nullable
    private String resolveAppId(List<DeltaGlobalCommittable> globalCommittables) {
        for (DeltaGlobalCommittable globalCommittable : globalCommittables) {
            for (DeltaCommittable deltaCommittable : globalCommittable.getDeltaCommittables()) {
                return deltaCommittable.getAppId();
            }
        }
        return null;
    }

    /**
     * Commits already written files to the DeltaLake table using unique identifier for the given Flink job (appId)
     * and checkpointId delivered with every committable object. Those ids together construct transactionId that
     * will be used for verification whether given set of files has already been committed to the Delta table.
     *
     * <p>During commit preparation phase:
     *
     * <ol>
     *   <li>first appId is resolved from any of the provided committables.
     *       If no appId is resolved then it means that no committables were provided and no commit is performed.
     *       Such situations may happen when e.g. there were no stream events received within given checkpoint interval,
     *   <li>If appId is successfully resolved then the provided set of committables needs to be flatten (as one {@link DeltaGlobalCommittable}
     *       contains a list of {@link DeltaCommittable}), mapped to {@link AddFile} objects and then grouped by checkpointId.
     *       The grouping part is necessary as committer object may receive committables from different checkpoint intervals,
     *   <li>we process each of the resolved checkpointId incrementally,
     *   <li>during processing each of the checkpointId and their committables we first query the DeltaLog for last checkpointed version
     *       for given appId. Here checkpointed version equals checkpointId. We proceed with the transaction only if current checkpointId is greater
     *       than last checkpointed version.
     *   <li>if above condition is met the we handle the metadata for data in given stream by comparing the stream's schema with current table snapshot's
     *       schema. We proceed with the transaction only when the schemas are matching or when it was explicitly configured during creation of the sink
     *       that we can try to update the schema.
     *   <li>if above validation passes then we prepare the final set of {@link Action} objects to be committed along with transaction's metadata and mandatory
     *       parameters,
     *   <li>we try to commit the prepared transaction
     *   <li>if the commit fails then we fail the application as well. If it succeeds then we proceed with the next checkpointId (if any).
     * </ol>
     * <p>
     *
     * @param globalCommittables list of combined committables objects.
     * @return always empty collection as we do not want any retry behaviour
     */
    @Override
    public List<DeltaGlobalCommittable> commit(List<DeltaGlobalCommittable> globalCommittables) {
        String appId = resolveAppId(globalCommittables);
        if (appId != null) { // means there are committable objects present
            Map<Long, List<AddFile>> addFileActionsPerCheckpoint = prepareAddFileActionsPerCheckpoint(globalCommittables);
            DeltaLog deltaLog = DeltaLog.forTable(conf, basePath.getPath());

            for (long checkpointId : addFileActionsPerCheckpoint.keySet().stream().sorted().collect(Collectors.toList())) {
                OptimisticTransaction transaction = deltaLog.startTransaction();
                long lastCommittedVersion = transaction.txnVersion(appId);
                if (checkpointId > lastCommittedVersion) {
                    List<String> partitionColumns = resolvePartitionColumnsForCurrentCheckpoint(checkpointId, addFileActionsPerCheckpoint);
                    handleMetadataUpdate(deltaLog.snapshot().getVersion(), transaction, partitionColumns);
                    List<Action> actions = prepareActionsForTransaction(appId, checkpointId, addFileActionsPerCheckpoint.get(checkpointId));
                    Operation operation = prepareDeltaLogOperation(
                            globalCommittables,
                            addFileActionsPerCheckpoint.get(checkpointId).size(),
                            partitionColumns
                    );

                    transaction.commit(actions, operation, ENGINE_INFO);
                }
            }
        }
        return Collections.emptyList();
    }

    private List<String> resolvePartitionColumnsForCurrentCheckpoint(long checkpointId, Map<Long, List<AddFile>> addFileActionsPerCheckpoint) {
        // at this point we have already assured that all the AddFile actions per current checkpoint have the same partition columns
        // so in order to get partition columns for current checkpoint we only need to reach the first one in the list
        Map<String, String> currentPartitionValues = addFileActionsPerCheckpoint.get(checkpointId).get(0).getPartitionValues();
        return new ArrayList<>(currentPartitionValues.keySet());
    }

    /**
     * Resolves whether to add the {@link Metadata} object to the transaction.
     *
     * <p>During this process:
     * <ol>
     *   <li>first we prepare metadata {@link Action} object using provided {@link RowType} (and converting it to {@link StructType}) and partition values,
     *   <li>then we compare the schema from above metadata with the current table's schema,
     *   <li>resolved metadata object is added to the transaction only when it's the first commit to the given Delta table or when the schemas are not matching
     *       but the sink was provided with option {@link DeltaGlobalCommitter#canTryUpdateSchema} set to true (the commit may still fail though if the Delta Standalone
     *       Writer will determine that the schemas are not compatible),
     *   <li>if the schemas are not matching and {@link DeltaGlobalCommitter#canTryUpdateSchema} was set to false then we throw an exception
     *   <li>if the schemas are matching then we do nothing and let the transaction proceed
     * </ol>
     * <p>
     *
     * @param currentTableVersion current version of the table's snapshot
     * @param transaction         DeltaLog's transaction object
     * @param partitionColumns    list of partitions for the current data stream
     */
    private void handleMetadataUpdate(long currentTableVersion,
                                      OptimisticTransaction transaction,
                                      List<String> partitionColumns) {
        Metadata metadataAction = prepareMetadata(partitionColumns);
        boolean schemasAreMatching = metadataAction.getSchema().toJson().equals(transaction.metadata().getSchema().toJson());
        if ((currentTableVersion == -1) || (!schemasAreMatching && canTryUpdateSchema)) {
            transaction.updateMetadata(metadataAction);
        } else if (!schemasAreMatching) {
            throw new RuntimeException(
                    "DataStream's schema is different from current table's schema. \n" +
                            "provided: " + metadataAction.getSchema().toPrettyJson() + "\n" +
                            "is different from: " + transaction.metadata().getSchema().toPrettyJson());
        }
    }

    /**
     * Constructs the final set of actions that will be committed with given transaction
     *
     * @param appId          unique identifier of the application
     * @param checkpointId   current checkpointId
     * @param addFileActions resolved list of {@link AddFile} actions for given checkpoint interval
     * @return list of {@link Action} objects that will be committed with current DeltaLog transaction
     */
    private List<Action> prepareActionsForTransaction(String appId,
                                                      long checkpointId,
                                                      List<AddFile> addFileActions) {
        List<Action> actions = new ArrayList<>();
        SetTransaction setTransaction = new SetTransaction(appId, checkpointId, Optional.of(System.currentTimeMillis()));
        actions.add(setTransaction);
        actions.addAll(addFileActions);
        return actions;
    }

    private Metadata prepareMetadata(List<String> partitionValues) {
        StructType dataStreamSchema = new SchemaConverter().toDeltaFormat(rowType);
        return new Metadata.Builder().partitionColumns(partitionValues).schema(dataStreamSchema).build();
    }

    private Operation prepareDeltaLogOperation(List<DeltaGlobalCommittable> globalCommittables,
                                               int numAddedFiles,
                                               List<String> partitionColumns) {
        Map<String, String> operationMetrics = prepareOperationMetrics(globalCommittables, numAddedFiles);
        Map<String, Object> operationParameters = new HashMap<String, Object>() {{
            put("mode", APPEND_MODE);
            put("partitionBy", partitionColumns);
        }};
        return new Operation(Operation.Name.STREAMING_UPDATE, operationParameters, operationMetrics);
    }

    /**
     * Prepares the set of {@link AddFile} actions grouped per checkpointId.
     * During this process we not only map the single committables to {@link AddFile} actions and group them by checkpointId
     * but also flatten collection of {@link DeltaGlobalCommittable} objects (each containing its own collection of
     * {@link DeltaCommittable}). Additionally, during the iteration process we also validate whether the committables for
     * the same checkpoint interval have the same set of partition columns and throw a {@link RuntimeException} when this condition
     * is not met.
     *
     * @param globalCommittables list of combined @link DeltaGlobalCommittable} objects
     * @return {@link AddFile} actions grouped by the checkpoint interval in the form of {@link java.util.Map}. It is guaranteed
     * that all actions per have the same partition columns within given checkpoint interval.
     */
    private Map<Long, List<AddFile>> prepareAddFileActionsPerCheckpoint(List<DeltaGlobalCommittable> globalCommittables) {
        Map<Long, List<AddFile>> addFilesPerCheckpoint = new HashMap<>();
        Map<Long, Set<String>> partitionColumnsMetadataPerCheckpoint = new HashMap<>();

        for (DeltaGlobalCommittable globalCommittable : globalCommittables) {
            for (DeltaCommittable deltaCommittable : globalCommittable.getDeltaCommittables()) {
                DeltaPendingFile deltaPendingFile = deltaCommittable.getDeltaPendingFile();

                if (!addFilesPerCheckpoint.containsKey(deltaCommittable.getCheckpointId())) {
                    addFilesPerCheckpoint.put(deltaCommittable.getCheckpointId(), new ArrayList<>());
                }

                AddFile action = convertDeltaPendingFileToAddFileAction(deltaPendingFile);
                addFilesPerCheckpoint.get(deltaCommittable.getCheckpointId()).add(action);

                LinkedHashMap<String, String> currentPartitionSpec = deltaPendingFile.getPartitionSpec();
                if (!partitionColumnsMetadataPerCheckpoint.containsKey(deltaCommittable.getCheckpointId())) {
                    partitionColumnsMetadataPerCheckpoint.put(deltaCommittable.getCheckpointId(), currentPartitionSpec.keySet());
                }

                boolean isPartitionColumnsMetadataRetained = compareKeysOfLinkedSets(currentPartitionSpec.keySet(), partitionColumnsMetadataPerCheckpoint.get(deltaCommittable.getCheckpointId()));
                if (!isPartitionColumnsMetadataRetained) {
                    throw new RuntimeException(
                            "Partition columns cannot be different for files within the same checkpointId. " +
                                    "checkpointId=" + deltaCommittable.getCheckpointId() + " " +
                                    "Partition spec " + deltaPendingFile.getPartitionSpec() + " does not comply with partition columns from previous files: " +
                                    partitionColumnsMetadataPerCheckpoint.get(deltaCommittable.getCheckpointId())
                    );
                }
            }
        }
        return addFilesPerCheckpoint;
    }

    private boolean compareKeysOfLinkedSets(Set<String> first, Set<String> second) {
        Iterator<String> firstIterator = first.iterator();
        Iterator<String> secondIterator = second.iterator();
        while (firstIterator.hasNext() && secondIterator.hasNext()) {
            if (!firstIterator.next().equals(secondIterator.next())) {
                return false;
            }
        }
        return true;
    }


    private AddFile convertDeltaPendingFileToAddFileAction(DeltaPendingFile deltaPendingFile) {
        Map<String, String> partitionValues = deltaPendingFile.getPartitionSpec();
        long modificationTime = deltaPendingFile.getLastUpdateTime();
        return new AddFile(deltaPendingFile.getFileName(), partitionValues, deltaPendingFile.getFileSize(), modificationTime, true, null, null);

    }

    private Map<String, String> prepareOperationMetrics(List<DeltaGlobalCommittable> globalCommittables,
                                                        int numAddedFiles) {
        long cumulatedRecordCount = 0;
        long cumulatedSize = 0;
        for (DeltaGlobalCommittable globalCommittable : globalCommittables) {
            for (DeltaCommittable deltaCommittable : globalCommittable.getDeltaCommittables()) {
                DeltaPendingFile deltaPendingFile = deltaCommittable.getDeltaPendingFile();
                cumulatedRecordCount += deltaPendingFile.getRecordCount();
                cumulatedSize += deltaPendingFile.getFileSize();
            }
        }

        Map<String, String> operationMetrics = new HashMap<>();
        operationMetrics.put("numRemovedFiles", "0"); // will be supported when for different operation modes
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
