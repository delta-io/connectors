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
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaPendingFile;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeltaGlobalCommitter implements GlobalCommitter<DeltaCommittable, DeltaGlobalCommittable> {

    private final String engineInfo = "flink.1.12"; // TODO parametrize

    private final Configuration conf;

    private final Path basePath;

    public DeltaGlobalCommitter(Configuration conf,
                                Path basePath) {
        this.conf = conf;
        this.basePath = basePath;
    }


    @Override
    public List<DeltaGlobalCommittable> filterRecoveredCommittables(List<DeltaGlobalCommittable> globalCommittables) throws IOException {
        return globalCommittables;
    }

    @Override
    public DeltaGlobalCommittable combine(List<DeltaCommittable> committables) throws IOException {
        List<DeltaPendingFile> pendingFiles = new ArrayList<>();
        for (DeltaCommittable committable : committables) {
            if (committable.hasDeltaPendingFile()) {
                assert committable.getDeltaPendingFile() != null;
                assert committable.getDeltaPendingFile().getFileName() != null;
                pendingFiles.add(committable.getDeltaPendingFile());
            }
        }
        return new DeltaGlobalCommittable(pendingFiles);
    }

    @Override
    public List<DeltaGlobalCommittable> commit(List<DeltaGlobalCommittable> globalCommittables) throws IOException {
        if (!globalCommittables.isEmpty()) {
            List<Action> addFileActions = prepareAddFileActions(globalCommittables);

            Operation operation = prepareDeltaLogOperation(globalCommittables, addFileActions.size());
            DeltaLog deltaLog = DeltaLog.forTable(conf, basePath.getPath());

            Metadata metadata = deltaLog.snapshot().getMetadata();

            OptimisticTransaction transaction = deltaLog.startTransaction();

            transaction.commit(addFileActions, operation, this.engineInfo);

        }
        return Collections.emptyList();
    }

    private Operation prepareDeltaLogOperation(List<DeltaGlobalCommittable> globalCommittables,
                                               int numAddedFiles) {
        Map<String, String> operationMetrics = prepareOperationMetrics(globalCommittables, numAddedFiles);
        Map<String, Object> operationParameters = Collections.emptyMap(); // TODO
        return new Operation(Operation.Name.STREAMING_UPDATE, operationParameters, operationMetrics);
    }

    private List<Action> prepareAddFileActions(List<DeltaGlobalCommittable> globalCommittables) {
        List<Action> actions = new ArrayList<>();
        for (DeltaGlobalCommittable globalCommittable : globalCommittables) {
            for (DeltaPendingFile deltaPendingFile : globalCommittable.getPendingFiles()) {
                Map<String, String> partitionValues = Collections.emptyMap(); // TODO
                long modificationTime = System.currentTimeMillis(); // TODO
                Action action = new AddFile(deltaPendingFile.getFileName(), partitionValues, deltaPendingFile.getFileSize(), modificationTime, true, null, null);
                actions.add(action);
            }
        }
        return actions;
    }

    private Map<String, String> prepareOperationMetrics(List<DeltaGlobalCommittable> globalCommittables,
                                                        int numAddedFiles) {
        long cumulatedRecordCount = 0;
        long cumulatedSize = 0;
        for (DeltaGlobalCommittable globalCommittable : globalCommittables) {
            for (DeltaPendingFile deltaPendingFile : globalCommittable.getPendingFiles()) {
                cumulatedRecordCount += deltaPendingFile.getRecordCount();
                cumulatedSize += deltaPendingFile.getFileSize();
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

    public Configuration getConf() {
        return conf;
    }

    public Path getBasePath() {
        return basePath;
    }


}
