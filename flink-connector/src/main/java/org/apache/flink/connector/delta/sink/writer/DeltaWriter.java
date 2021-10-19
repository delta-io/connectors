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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.file.sink.writer.FileWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;


/**
 * A {@link SinkWriter} implementation for {@link org.apache.flink.connector.delta.sink.DeltaSink}.
 *
 * <p>It writes data to and manages the different active {@link DeltaWriterBucket buckets} in the
 * {@link org.apache.flink.connector.delta.sink.DeltaSink}.
 * <p>
 * Most of the logic for this class was sourced from {@link FileWriter}
 *
 * @param <IN> The type of input elements.
 */
@Internal
public class DeltaWriter<IN>
        implements SinkWriter<IN, DeltaCommittable, DeltaWriterBucketState>,
        Sink.ProcessingTimeService.ProcessingTimeCallback {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaWriter.class);

    // ------------------------ configuration fields --------------------------

    private final Path basePath;

    private final DeltaWriterBucketFactory<IN> bucketFactory;

    private final BucketAssigner<IN, String> bucketAssigner;

    private final DeltaBulkBucketWriter<IN, String> bucketWriter;

    private final RollingPolicy<IN, String> rollingPolicy;

    private final Sink.ProcessingTimeService processingTimeService;

    private final long bucketCheckInterval;

    // --------------------------- runtime fields -----------------------------

    private final BucketerContext bucketerContext;

    private final Map<String, DeltaWriterBucket<IN>> activeBuckets;

    private final OutputFileConfig outputFileConfig;

    private final String appId;

    private long nextCheckpointId;

    public DeltaWriter(
            final Path basePath,
            final BucketAssigner<IN, String> bucketAssigner,
            final DeltaWriterBucketFactory<IN> bucketFactory,
            final DeltaBulkBucketWriter<IN, String> bucketWriter,
            final RollingPolicy<IN, String> rollingPolicy,
            final OutputFileConfig outputFileConfig,
            final Sink.ProcessingTimeService processingTimeService,
            final long bucketCheckInterval,
            final String appId,
            final long nextCheckpointId) {
        this.basePath = checkNotNull(basePath);
        this.bucketAssigner = checkNotNull(bucketAssigner);
        this.bucketFactory = checkNotNull(bucketFactory);
        this.bucketWriter = checkNotNull(bucketWriter);
        this.rollingPolicy = checkNotNull(rollingPolicy);

        this.outputFileConfig = checkNotNull(outputFileConfig);

        this.activeBuckets = new HashMap<>();
        this.bucketerContext = new BucketerContext();

        this.processingTimeService = checkNotNull(processingTimeService);
        checkArgument(
                bucketCheckInterval > 0,
                "Bucket checking interval for processing time should be positive.");
        this.bucketCheckInterval = bucketCheckInterval;
        this.appId = appId;
        this.nextCheckpointId = nextCheckpointId;
    }

    /**
     * Initializes the state after recovery from a failure.
     *
     * <p>During this process:
     *
     * <ol>
     *   <li>we set the initial value for part counter to the maximum value used before across all
     *       tasks and buckets. This guarantees that we do not overwrite valid data,
     *   <li>we resume writing to the previous in-progress file of each bucket, and
     *   <li>if we receive multiple states for the same bucket, we merge them.
     * </ol>
     *
     * @param bucketStates the state holding recovered state about active buckets.
     * @throws IOException if anything goes wrong during retrieving the state or
     *                     restoring/committing of any in-progress/pending part files
     */
    public void initializeState(List<DeltaWriterBucketState> bucketStates) throws IOException {
        checkNotNull(bucketStates, "The retrieved state was null.");

        for (DeltaWriterBucketState state : bucketStates) {
            String bucketId = state.getBucketId();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Restoring: {}", state);
            }

            DeltaWriterBucket<IN> restoredBucket =
                    bucketFactory.restoreBucket(bucketWriter, rollingPolicy, state, outputFileConfig);

            updateActiveBucketId(bucketId, restoredBucket);
        }

        registerNextBucketInspectionTimer();
    }

    private void updateActiveBucketId(String bucketId, DeltaWriterBucket<IN> restoredBucket)
            throws IOException {
        final DeltaWriterBucket<IN> bucket = activeBuckets.get(bucketId);
        if (bucket != null) {
            bucket.merge(restoredBucket);
        } else {
            activeBuckets.put(bucketId, restoredBucket);
        }
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        // setting the values in the bucketer context
        bucketerContext.update(
                context.timestamp(),
                context.currentWatermark(),
                processingTimeService.getCurrentProcessingTime());

        final String bucketId = bucketAssigner.getBucketId(element, bucketerContext);
        final DeltaWriterBucket<IN> bucket = getOrCreateBucketForBucketId(bucketId);
        bucket.write(element, processingTimeService.getCurrentProcessingTime());
    }

    @Override
    public List<DeltaCommittable> prepareCommit(boolean flush) throws IOException {
        List<DeltaCommittable> committables = new ArrayList<>();

        // Every time before we prepare commit, we first check and remove the inactive
        // buckets. Checking the activeness right before pre-committing avoid re-creating
        // the bucket every time if the bucket use OnCheckpointingRollingPolicy.
        Iterator<Map.Entry<String, DeltaWriterBucket<IN>>> activeBucketIt =
                activeBuckets.entrySet().iterator();
        while (activeBucketIt.hasNext()) {
            Map.Entry<String, DeltaWriterBucket<IN>> entry = activeBucketIt.next();
            if (!entry.getValue().isActive()) {
                activeBucketIt.remove();
            } else {
                committables.addAll(entry.getValue().prepareCommit(flush, appId, nextCheckpointId));
            }
        }

        incrementNextCheckpointId();
        return committables;
    }

    private void incrementNextCheckpointId() {
        nextCheckpointId += 1;
    }

    @Override
    public List<DeltaWriterBucketState> snapshotState() throws IOException {
        checkState(bucketWriter != null, "sink has not been initialized");

        List<DeltaWriterBucketState> state = new ArrayList<>();
        for (DeltaWriterBucket<IN> bucket : activeBuckets.values()) {
            state.add(bucket.snapshotState(appId));
        }

        return state;
    }

    private DeltaWriterBucket<IN> getOrCreateBucketForBucketId(String bucketId) throws IOException {
        DeltaWriterBucket<IN> bucket = activeBuckets.get(bucketId);
        if (bucket == null) {
            final Path bucketPath = assembleBucketPath(bucketId);
            bucket =
                    bucketFactory.getNewBucket(bucketId, bucketPath, bucketWriter, rollingPolicy, outputFileConfig);
            activeBuckets.put(bucketId, bucket);
        }
        return bucket;
    }

    @Override
    public void close() {
        if (activeBuckets != null) {
            activeBuckets.values().forEach(DeltaWriterBucket::disposePartFile);
        }
    }

    private Path assembleBucketPath(String bucketId) {
        if ("".equals(bucketId)) {
            return basePath;
        }
        return new Path(basePath, bucketId);
    }

    @Override
    public void onProcessingTime(long time) throws IOException {
        for (DeltaWriterBucket<IN> bucket : activeBuckets.values()) {
            bucket.onProcessingTime(time);
        }

        registerNextBucketInspectionTimer();
    }

    private void registerNextBucketInspectionTimer() {
        final long nextInspectionTime =
                processingTimeService.getCurrentProcessingTime() + bucketCheckInterval;
        processingTimeService.registerProcessingTimer(nextInspectionTime, this);
    }

    /**
     * The {@link BucketAssigner.Context} exposed to the {@link BucketAssigner#getBucketId(Object,
     * BucketAssigner.Context)} whenever a new incoming element arrives.
     */
    private static final class BucketerContext implements BucketAssigner.Context {

        @Nullable
        private Long elementTimestamp;

        private long currentWatermark;

        private long currentProcessingTime;

        private BucketerContext() {
            this.elementTimestamp = null;
            this.currentWatermark = Long.MIN_VALUE;
            this.currentProcessingTime = Long.MIN_VALUE;
        }

        void update(@Nullable Long elementTimestamp, long watermark, long currentProcessingTime) {
            this.elementTimestamp = elementTimestamp;
            this.currentWatermark = watermark;
            this.currentProcessingTime = currentProcessingTime;
        }

        @Override
        public long currentProcessingTime() {
            return currentProcessingTime;
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        @Nullable
        public Long timestamp() {
            return elementTimestamp;
        }
    }

    // --------------------------- Testing Methods -----------------------------

    @VisibleForTesting
    Map<String, DeltaWriterBucket<IN>> getActiveBuckets() {
        return activeBuckets;
    }
}
