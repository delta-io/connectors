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

package org.apache.flink.connector.delta.sink;


import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittableSerializer;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittableSerializer;
import org.apache.flink.connector.delta.sink.committer.DeltaCommitter;
import org.apache.flink.connector.delta.sink.committer.DeltaGlobalCommitter;
import org.apache.flink.connector.delta.sink.writer.DefaultDeltaWriterBucketFactory;
import org.apache.flink.connector.delta.sink.writer.DeltaWriter;
import org.apache.flink.connector.delta.sink.writer.DeltaWriterBucketFactory;
import org.apache.flink.connector.delta.sink.writer.DeltaWriterBucketState;
import org.apache.flink.connector.delta.sink.writer.DeltaWriterBucketStateSerializer;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;


public class DeltaSinkBuilder<IN>
        implements Serializable {

    private static final long serialVersionUID = 7493169281036370228L;

    protected static final long DEFAULT_BUCKET_CHECK_INTERVAL = 60L * 1000L;

    private final Path tableBasePath;

    private long bucketCheckInterval;

    private final ParquetWriterFactory<IN> writerFactory;

    private final DeltaWriterBucketFactory<IN> bucketFactory;

    private BucketAssigner<IN, String> bucketAssigner;

    private CheckpointRollingPolicy<IN, String> rollingPolicy;

    private OutputFileConfig outputFileConfig;

    private final SerializableConfiguration serializableConfiguration;

    private final RowType rowType;

    private final String appId;

    private boolean canOverwriteSchema;

    private static String generateNewAppId() {
        return UUID.randomUUID().toString();
    }

    protected DeltaSinkBuilder(
            Path basePath,
            Configuration conf,
            ParquetWriterFactory<IN> writerFactory,
            BucketAssigner<IN, String> assigner,
            RowType rowType) {
        this(
                basePath,
                conf,
                DEFAULT_BUCKET_CHECK_INTERVAL,
                writerFactory,
                assigner,
                OnCheckpointRollingPolicy.build(),
                new DefaultDeltaWriterBucketFactory<>(),
                OutputFileConfig.builder().withPartSuffix(".snappy.parquet").build(),
                rowType,
                generateNewAppId(),
                false
        );
    }

    protected DeltaSinkBuilder(
            Path basePath,
            Configuration conf,
            long bucketCheckInterval,
            ParquetWriterFactory<IN> writerFactory,
            BucketAssigner<IN, String> assigner,
            CheckpointRollingPolicy<IN, String> policy,
            DeltaWriterBucketFactory<IN> bucketFactory,
            OutputFileConfig outputFileConfig,
            RowType rowType,
            String appId,
            boolean canOverwriteSchema) {
        this.tableBasePath = checkNotNull(basePath);
        this.serializableConfiguration = new SerializableConfiguration(checkNotNull(conf));
        this.bucketCheckInterval = bucketCheckInterval;
        this.writerFactory = writerFactory;
        this.bucketAssigner = checkNotNull(assigner);
        this.rollingPolicy = checkNotNull(policy);
        this.bucketFactory = checkNotNull(bucketFactory);
        this.outputFileConfig = checkNotNull(outputFileConfig);
        this.rowType = rowType;
        this.appId = appId;
        this.canOverwriteSchema = canOverwriteSchema;
    }

    public DeltaSinkBuilder<IN> withBucketCheckInterval(final long interval) {
        this.bucketCheckInterval = interval;
        return this;
    }

    public DeltaSinkBuilder<IN> withBucketAssigner(BucketAssigner<IN, String> assigner) {
        this.bucketAssigner = checkNotNull(assigner);
        return this;
    }

    public DeltaSinkBuilder<IN> withRollingPolicy(CheckpointRollingPolicy<IN, String> rollingPolicy) {
        this.rollingPolicy = checkNotNull(rollingPolicy);
        return this;
    }

    public DeltaSinkBuilder<IN> withOutputFileConfig(final OutputFileConfig outputFileConfig) {
        this.outputFileConfig = outputFileConfig;
        return this;
    }

    public DeltaSinkBuilder<IN> withNewBucketAssigner(
            BucketAssigner<IN, String> assigner) {
        checkState(
                bucketFactory.getClass() == DefaultDeltaWriterBucketFactory.class,
                "newBuilderWithBucketAssigner() cannot be called "
                        + "after specifying a customized bucket factory");
        return new DeltaSinkBuilder<>(
                tableBasePath,
                serializableConfiguration.conf(),
                bucketCheckInterval,
                writerFactory,
                checkNotNull(assigner),
                rollingPolicy,
                bucketFactory,
                outputFileConfig,
                rowType,
                appId,
                canOverwriteSchema);
    }

    public DeltaSinkBuilder<IN> withRowType(RowType rowType) {
        return new DeltaSinkBuilder<>(
                tableBasePath,
                serializableConfiguration.conf(),
                bucketCheckInterval,
                writerFactory,
                bucketAssigner,
                rollingPolicy,
                bucketFactory,
                outputFileConfig,
                rowType,
                appId,
                canOverwriteSchema);
    }

    public DeltaSinkBuilder<IN> withAppId(final String appId) {
        return new DeltaSinkBuilder<>(
                tableBasePath,
                serializableConfiguration.conf(),
                bucketCheckInterval,
                writerFactory,
                bucketAssigner,
                rollingPolicy,
                bucketFactory,
                outputFileConfig,
                rowType,
                appId,
                canOverwriteSchema);
    }

    public DeltaSinkBuilder<IN> withCanOverwriteSchema(final boolean canOverwriteSchema) {
        this.canOverwriteSchema = canOverwriteSchema;
        return this;
    }

    /**
     * Creates the actual sink.
     */
    public DeltaSink<IN> build() {
        return new DeltaSink<IN>(this);
    }

    DeltaWriter<IN> createWriter(Sink.InitContext context,
                                 String appId,
                                 long nextCheckpointId) throws IOException {
        return new DeltaWriter<IN>(
                tableBasePath,
                bucketAssigner,
                bucketFactory,
                createBucketWriter(),
                rollingPolicy,
                outputFileConfig,
                context.getProcessingTimeService(),
                bucketCheckInterval,
                appId,
                nextCheckpointId);
    }

    DeltaCommitter createCommitter() throws IOException {
        return new DeltaCommitter(createBucketWriter());
    }

    DeltaGlobalCommitter createGlobalCommitter() throws IOException {
        return new DeltaGlobalCommitter(serializableConfiguration.conf(), tableBasePath, rowType, canOverwriteSchema);
    }

    SimpleVersionedSerializer<DeltaWriterBucketState> getWriterStateSerializer()
            throws IOException {
        BucketWriter<IN, String> bucketWriter = createBucketWriter();

        return new DeltaWriterBucketStateSerializer(
                bucketWriter.getProperties().getInProgressFileRecoverableSerializer()
        );
    }

    SimpleVersionedSerializer<DeltaCommittable> getCommittableSerializer()
            throws IOException {
        BucketWriter<IN, String> bucketWriter = createBucketWriter();

        return new DeltaCommittableSerializer(
                bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
                bucketWriter.getProperties().getInProgressFileRecoverableSerializer());
    }

    SimpleVersionedSerializer<DeltaGlobalCommittable> getGlobalCommittableSerializer()
            throws IOException {
        BucketWriter<IN, String> bucketWriter = createBucketWriter();

        return new DeltaGlobalCommittableSerializer(bucketWriter.getProperties().getPendingFileRecoverableSerializer());
    }

    private DeltaBulkBucketWriter<IN, String> createBucketWriter() throws IOException {
        return new DeltaBulkBucketWriter<>(
                FileSystem.get(tableBasePath.toUri()).createRecoverableWriter(), writerFactory);
    }

    RowType getRowType() {
        return rowType;
    }

    Path getTableBasePath() {
        return tableBasePath;
    }

    SerializableConfiguration getSerializableConfiguration() {
        return serializableConfiguration;
    }

    String getAppId() {
        return appId;
    }

    public boolean isCanOverwriteSchema() {
        return canOverwriteSchema;
    }
}


