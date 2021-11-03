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

package org.apache.flink.connector.delta.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkPartWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaInProgressPart;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaPendingFile;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.InProgressFileRecoverable;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;


@Internal
class DeltaWriterBucket<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaWriterBucket.class);

    private final String bucketId;

    private final Path bucketPath;

    private final DeltaBulkBucketWriter<IN, String> bucketWriter;

    private final CheckpointRollingPolicy<IN, String> rollingPolicy;

    private final OutputFileConfig outputFileConfig;

    private final String uniqueId;

    private final List<DeltaPendingFile> pendingFiles = new ArrayList<>();

    private long partCounter;

    private long inProgressPartRecordCount;

    @Nullable
    private InProgressFileRecoverable inProgressFileToCleanup;

    @Nullable
    private DeltaInProgressPart<IN> deltaInProgressPart;

    private final LinkedHashMap<String, String> partitionSpec;

    /**
     * Constructor to create a new empty bucket.
     */
    private DeltaWriterBucket(
            String bucketId,
            Path bucketPath,
            DeltaBulkBucketWriter<IN, String> bucketWriter,
            CheckpointRollingPolicy<IN, String> rollingPolicy,
            OutputFileConfig outputFileConfig) throws IOException {
        this.bucketId = checkNotNull(bucketId);
        this.bucketPath = checkNotNull(bucketPath);
        this.bucketWriter = checkNotNull(bucketWriter);
        this.rollingPolicy = checkNotNull(rollingPolicy);
        this.outputFileConfig = checkNotNull(outputFileConfig);

        this.partitionSpec = PartitionPathUtils.extractPartitionSpecFromPath(this.bucketPath);
        this.uniqueId = UUID.randomUUID().toString();
        this.partCounter = 0;
        this.inProgressPartRecordCount = 0;
    }

    /**
     * Constructor to restore a bucket from checkpointed state.
     */
    private DeltaWriterBucket(
            DeltaBulkBucketWriter<IN, String> partFileFactory,
            CheckpointRollingPolicy<IN, String> rollingPolicy,
            DeltaWriterBucketState bucketState,
            OutputFileConfig outputFileConfig)
            throws IOException {

        this(
                bucketState.getBucketId(),
                bucketState.getBucketPath(),
                partFileFactory,
                rollingPolicy,
                outputFileConfig);

        restoreInProgressFile(bucketState);

    }

    private void restoreInProgressFile(DeltaWriterBucketState state) throws IOException {
        if (!state.hasInProgressFileRecoverable()) {
            return;
        }

        // we try to resume the previous in-progress file
        InProgressFileRecoverable inProgressFileRecoverable =
                state.getInProgressFileRecoverable();

        if (bucketWriter.getProperties().supportsResume()) {
            DeltaBulkPartWriter<IN, String> inProgressPart = (DeltaBulkPartWriter<IN, String>)
                    bucketWriter.resumeInProgressFileFrom(
                            bucketId,
                            inProgressFileRecoverable,
                            state.getInProgressFileCreationTime());
            deltaInProgressPart = new DeltaInProgressPart<>(
                    state.getInProgressPartFileName(),
                    inProgressPart
            );
        } else {
            DeltaPendingFile deltaPendingFile = new DeltaPendingFile(
                    partitionSpec,
                    state.getInProgressPartFileName(),
                    inProgressFileRecoverable,
                    state.getRecordCount(),
                    state.getInProgressPartFileSize(),
                    state.getLastUpdateTime()
            );
            pendingFiles.add(deltaPendingFile);
        }
    }

    public String getBucketId() {
        return bucketId;
    }

    public Path getBucketPath() {
        return bucketPath;
    }

    public long getPartCounter() {
        return partCounter;
    }

    public boolean isActive() {
        return deltaInProgressPart != null || inProgressFileToCleanup != null || pendingFiles.size() > 0;
    }

    void merge(final DeltaWriterBucket<IN> bucket) throws IOException {
        checkNotNull(bucket);
        checkState(Objects.equals(bucket.bucketPath, bucketPath));

        bucket.closePartFile();
        pendingFiles.addAll(bucket.pendingFiles);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Merging buckets for bucket id={}", bucketId);
        }
    }

    void write(IN element, long currentTime) throws IOException {
        if (deltaInProgressPart == null || rollingPolicy.shouldRollOnEvent(deltaInProgressPart.getInProgressPart(), element)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Opening new part file for bucket id={} due to element {}.",
                        bucketId,
                        element);
            }
            deltaInProgressPart = rollPartFile(currentTime);
        }

        deltaInProgressPart.getInProgressPart().write(element, currentTime);
        ++inProgressPartRecordCount;
    }


    List<DeltaCommittable> prepareCommit(boolean flush,
                                         String appId,
                                         long checkpointId) throws IOException {
        if (deltaInProgressPart != null) {
            if (rollingPolicy.shouldRollOnCheckpoint(deltaInProgressPart.getInProgressPart()) || flush) {
                if (LOG.isDebugEnabled())
                    LOG.debug("Closing in-progress part file for bucket id={} on checkpoint.", bucketId);

                closePartFile();
            } else {
                throw new RuntimeException(
                        "Unexpected behaviour. Bulk format writers should always roll part files on checkpoint. " +
                                "To resolve this issue verify behaviour of your rolling policy.");
            }
        }


        List<DeltaCommittable> committables = new ArrayList<>();
        pendingFiles.forEach(pendingFile -> committables.add(new DeltaCommittable(pendingFile, appId, checkpointId)));
        pendingFiles.clear();
        if (inProgressFileToCleanup != null) {
            committables.add(new DeltaCommittable(inProgressFileToCleanup, appId, checkpointId));
            inProgressFileToCleanup = null;
        }

        return committables;
    }

    DeltaWriterBucketState snapshotState(String appId) throws IOException {
        InProgressFileRecoverable inProgressFileRecoverable = null;
        long inProgressFileCreationTime = Long.MAX_VALUE;
        String inProgressPartFileName = null;
        long recordCount = 0;
        long inProgressPartFileSize = -1L;
        long lastUpdateTime = -1L;

        if (deltaInProgressPart != null) {
            InProgressFileWriter<IN, String> inProgressPart = deltaInProgressPart.getInProgressPart();
            inProgressFileRecoverable = inProgressPart.persist();
            inProgressFileCreationTime = inProgressPart.getCreationTime();
            inProgressPartFileName = deltaInProgressPart.getFileName();
            recordCount = this.inProgressPartRecordCount;
            inProgressPartFileSize = inProgressPart.getSize();
            lastUpdateTime = inProgressPart.getLastUpdateTime();

            inProgressFileToCleanup = inProgressFileRecoverable;
        }

        return new DeltaWriterBucketState(
                bucketId,
                bucketPath,
                inProgressFileCreationTime,
                inProgressFileRecoverable,
                inProgressPartFileName,
                recordCount,
                inProgressPartFileSize,
                lastUpdateTime,
                appId
        );
    }

    void onProcessingTime(long timestamp) throws IOException {
        if (deltaInProgressPart != null
                && rollingPolicy.shouldRollOnProcessingTime(deltaInProgressPart.getInProgressPart(), timestamp)) {
            InProgressFileWriter<IN, String> inProgressPart = deltaInProgressPart.getInProgressPart();
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Bucket {} closing in-progress part file for part file id={} due to processing time rolling policy "
                                + "(in-progress file created @ {}, last updated @ {} and current time is {}).",
                        bucketId,
                        uniqueId,
                        inProgressPart.getCreationTime(),
                        inProgressPart.getLastUpdateTime(),
                        timestamp);
            }

            closePartFile();
        }
    }

    private DeltaInProgressPart<IN> rollPartFile(long currentTime) throws IOException {
        closePartFile();

        final Path partFilePath = assembleNewPartPath();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Opening new part file \"{}\" for bucket id={}.",
                    partFilePath.getName(),
                    bucketId);
        }

        DeltaBulkPartWriter<IN, String> fileWriter = (DeltaBulkPartWriter<IN, String>) bucketWriter.openNewInProgressFile(bucketId, partFilePath, currentTime);

        LOG.debug(
                "Successfully opened new part file \"{}\" for bucket id={}.",
                partFilePath.getName(),
                bucketId);


        return new DeltaInProgressPart<IN>(partFilePath.getName(), fileWriter);
    }

    /**
     * Constructor a new PartPath and increment the partCounter.
     */
    private Path assembleNewPartPath() {
        long currentPartCounter = partCounter++;
        return new Path(
                bucketPath,
                outputFileConfig.getPartPrefix()
                        + '-'
                        + uniqueId
                        + '-'
                        + currentPartCounter
                        + outputFileConfig.getPartSuffix());
    }

    private void closePartFile() throws IOException {
        if (deltaInProgressPart != null) {
            // we need to close the writer explicitly before calling closeForCommit() in order to get
            // the actual file size
            deltaInProgressPart.getInProgressPart().closeWriter();
            long fileSize = deltaInProgressPart.getInProgressPart().getSize();
            InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable =
                    deltaInProgressPart.getInProgressPart().closeForCommit();

            DeltaPendingFile pendingFile = new DeltaPendingFile(
                    partitionSpec,
                    deltaInProgressPart.getFileName(),
                    pendingFileRecoverable,
                    this.inProgressPartRecordCount,
                    fileSize,
                    deltaInProgressPart.getInProgressPart().getLastUpdateTime()
            );
            pendingFiles.add(pendingFile);
            deltaInProgressPart = null;
            inProgressPartRecordCount = 0;
        }
    }


    void disposePartFile() {
        if (deltaInProgressPart != null) {
            deltaInProgressPart.getInProgressPart().dispose();

        }
    }

    public String getUniqueId() {
        return uniqueId;
    }

    @Nullable
    DeltaInProgressPart<IN> getDeltaInProgressPart() {
        return deltaInProgressPart;
    }

    public List<DeltaPendingFile> getPendingFiles() {
        return pendingFiles;
    }

    // --------------------------- Static Factory Methods -----------------------------

    static <IN> DeltaWriterBucket<IN> getNew(
            final String bucketId,
            final Path bucketPath,
            final DeltaBulkBucketWriter<IN, String> bucketWriter,
            final CheckpointRollingPolicy<IN, String> rollingPolicy,
            final OutputFileConfig outputFileConfig) throws IOException {
        return new DeltaWriterBucket<IN>(
                bucketId, bucketPath, bucketWriter, rollingPolicy, outputFileConfig);
    }


    static <IN> DeltaWriterBucket<IN> restore(
            final DeltaBulkBucketWriter<IN, String> bucketWriter,
            final CheckpointRollingPolicy<IN, String> rollingPolicy,
            final DeltaWriterBucketState bucketState,
            final OutputFileConfig outputFileConfig)
            throws IOException {
        return new DeltaWriterBucket<IN>(bucketWriter, rollingPolicy, bucketState, outputFileConfig);
    }

    public long getInProgressPartRecordCount() {
        return inProgressPartRecordCount;
    }

}
