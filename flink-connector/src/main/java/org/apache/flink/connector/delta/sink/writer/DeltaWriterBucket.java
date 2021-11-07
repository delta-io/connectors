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

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;


/**
 * Internal implementation for writing the actual events to the underlying files in the correct buckets
 * / partitions.
 *
 * <p>
 * In reference to the Flink's {@link org.apache.flink.api.connector.sink.Sink} topology
 * one of its main components is {@link org.apache.flink.api.connector.sink.SinkWriter}
 * which in case of DeltaSink is implemented as {@link DeltaWriter}. However, to comply
 * with DeltaLake's support for partitioning tables a new component was added in the form
 * of {@link DeltaWriterBucket} that is responsible for handling writes to only one of the
 * buckets (aka partitions). Such bucket writers are managed by {@link DeltaWriter}
 * which works as a proxy between higher order frameworks commands (write, prepareCommit etc.)
 * and actual writes' implementation in {@link DeltaWriterBucket}. Thanks to this solution
 * events within one {@link DeltaWriter} operator received during particular checkpoint interval
 * are always grouped and flushed to the currently opened in-progress file.
 * <p>
 * The implementation was sourced from the {@link org.apache.flink.connector.file.sink.FileSink}
 * that utilizes same concept and implements
 * {@link org.apache.flink.connector.file.sink.writer.FileWriter} with its FileWriterBucket
 * implementation.
 * All differences between DeltaSink's and FileSink's writer buckets are explained in particular
 * method's below.
 *
 * @param <IN> The type of input elements.
 */
@Internal
class DeltaWriterBucket<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaWriterBucket.class);

    private final String bucketId;

    private final Path bucketPath;

    private final OutputFileConfig outputFileConfig;

    private final String uniqueId;

    private final DeltaBulkBucketWriter<IN, String> bucketWriter;

    private final CheckpointRollingPolicy<IN, String> rollingPolicy;

    private final List<DeltaPendingFile> pendingFiles = new ArrayList<>();

    private final LinkedHashMap<String, String> partitionSpec;

    private long partCounter;

    private long inProgressPartRecordCount;

    @Nullable
    private DeltaInProgressPart<IN> deltaInProgressPart;

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
    }

    /**
     * @implNote This method behaves in the similar way as
     * org.apache.flink.connector.file.sink.writer.FileWriterBucket#prepareCommit
     * except that:
     * <ol>
     *   <li>it uses custom {@link DeltaInProgressPart} implementation
     *   <li>it adds transactional identifier for current checkpoint interval (appId + checkpointId)
     *       to the committables
     *   <li>it does not handle any in progress files to cleanup as it's supposed to always roll
     *       part files on checkpoint which is also the default behaviour for bulk formats in
     *       {@link org.apache.flink.connector.file.sink.FileSink} as well. The reason why its needed
     *       for FileSink is that it also provides support for row wise formats which is not required
     *       in case of DeltaSink.
     * </ol>
     */
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

        return committables;
    }

    /**
     * This method is responsible for snapshotting state of the bucket writer. The writer's
     * state snapshot can be further used to recover from failure or from manual Flink's app
     * snapshot.
     * <p>
     * Since the writer is supposed to always roll part files on checkpoint then there is not
     * much state to snapshot and recover from except bucket metadata (id and path) and also
     * unique identifier for the application that the writer is part of.
     *
     * @param appId unique identifier of the Flink app that needs to be retained within all app
     *              restarts
     * @return snapshot of the current bucket writer's state
     */
    DeltaWriterBucketState snapshotState(String appId) {
        return new DeltaWriterBucketState(
                bucketId,
                bucketPath,
                appId
        );
    }

    /**
     * Method responsible for "closing" previous in-progress file and "opening" new one to be
     * written to.
     *
     * @param currentTime current processing time
     * @return new in progress part instance representing part file that the writer will start
     * write data to
     * @throws IOException
     * @implNote This method behaves in the similar way as
     * org.apache.flink.connector.file.sink.writer.FileWriterBucket#rollPartFile
     * except that it uses custom implementation to represent the in-progress part file.
     * See {@link DeltaInProgressPart} for details.
     */
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
     * Method responsible for "closing" currently opened in-progress file and appending new
     * {@link DeltaPendingFile} instance to {@link this#pendingFiles}. Those pending files
     * during commit will become critical part of committables information passed to both
     * types of committers.
     *
     * @throws IOException
     * @implNote This method behaves in the similar way as
     * org.apache.flink.connector.file.sink.writer.FileWriterBucket#closePartFile
     * however it adds some implementation details.
     * <ol>
     *   <li>it uses custom {@link DeltaInProgressPart} implementation in order to be able to explicitly
     *       close the internal file writer what allows to get the actual file size. It is necessary
     *       as original implementation of {@link InProgressFileWriter} used by
     *       {@link org.apache.flink.connector.file.sink.FileSink} does not provide us with correct
     *       file size because for bulk formats it shows the file size before flushing the internal buffer,
     *   <li>it enriches the {@link DeltaPendingFile} with closed file's metadata
     *   <li>it resets the counter for currently opened part file
     * </ol>
     */
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

    ///////////////////////////////////////////////////////////////////////////
    // FileSink-specific methods
    ///////////////////////////////////////////////////////////////////////////

    /**
     * @implNote This method behaves in the same way as
     * org.apache.flink.connector.file.sink.writer.FileWriterBucket#write
     * except that it uses custom {@link DeltaInProgressPart} implementation and also
     * counts the events written to the currently opened part file.
     */
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

    void merge(final DeltaWriterBucket<IN> bucket) throws IOException {
        checkNotNull(bucket);
        checkState(Objects.equals(bucket.bucketPath, bucketPath));

        bucket.closePartFile();
        pendingFiles.addAll(bucket.pendingFiles);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Merging buckets for bucket id={}", bucketId);
        }
    }

    public boolean isActive() {
        return deltaInProgressPart != null || pendingFiles.size() > 0;
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

    void disposePartFile() {
        if (deltaInProgressPart != null) {
            deltaInProgressPart.getInProgressPart().dispose();
        }
    }

    // --------------------------- Static Factory -----------------------------

    public static class DeltaWriterBucketFactory {
        static <IN> DeltaWriterBucket<IN> getNewBucket(
                final String bucketId,
                final Path bucketPath,
                final DeltaBulkBucketWriter<IN, String> bucketWriter,
                final CheckpointRollingPolicy<IN, String> rollingPolicy,
                final OutputFileConfig outputFileConfig) throws IOException {
            return new DeltaWriterBucket<IN>(
                    bucketId, bucketPath, bucketWriter, rollingPolicy, outputFileConfig);
        }

        static <IN> DeltaWriterBucket<IN> restoreBucket(
                final DeltaBulkBucketWriter<IN, String> bucketWriter,
                final CheckpointRollingPolicy<IN, String> rollingPolicy,
                final DeltaWriterBucketState bucketState,
                final OutputFileConfig outputFileConfig)
                throws IOException {
            return new DeltaWriterBucket<IN>(bucketWriter, rollingPolicy, bucketState, outputFileConfig);
        }
    }

}
