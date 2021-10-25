package org.apache.flink.connector.delta.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;

import java.io.IOException;


@Internal
public class DefaultDeltaWriterBucketFactory<IN> implements DeltaWriterBucketFactory<IN> {

    @Override
    public DeltaWriterBucket<IN> getNewBucket(
            String bucketId,
            Path bucketPath,
            DeltaBulkBucketWriter<IN, String> bucketWriter,
            CheckpointRollingPolicy<IN, String> rollingPolicy,
            OutputFileConfig outputFileConfig)
            throws IOException {
        return DeltaWriterBucket.getNew(bucketId, bucketPath, bucketWriter, rollingPolicy, outputFileConfig);
    }

    @Override
    public DeltaWriterBucket<IN> restoreBucket(
            DeltaBulkBucketWriter<IN, String> bucketWriter,
            CheckpointRollingPolicy<IN, String> rollingPolicy,
            DeltaWriterBucketState bucketState,
            OutputFileConfig outputFileConfig)
            throws IOException {
        return DeltaWriterBucket.restore(bucketWriter, rollingPolicy, bucketState, outputFileConfig);
    }
}
