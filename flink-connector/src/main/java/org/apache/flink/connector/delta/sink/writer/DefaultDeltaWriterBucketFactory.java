package org.apache.flink.connector.delta.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;

import java.io.IOException;


@Internal
public class DefaultDeltaWriterBucketFactory<IN> implements DeltaWriterBucketFactory<IN> {

    @Override
    public DeltaWriterBucket<IN> getNewBucket(
            String bucketId,
            Path bucketPath,
            DeltaBulkBucketWriter<IN, String> bucketWriter,
            RollingPolicy<IN, String> rollingPolicy,
            OutputFileConfig outputFileConfig,
            String appId)
            throws IOException {
        return DeltaWriterBucket.getNew(bucketId, bucketPath, bucketWriter, rollingPolicy, outputFileConfig, appId);
    }

    @Override
    public DeltaWriterBucket<IN> restoreBucket(
            DeltaBulkBucketWriter<IN, String> bucketWriter,
            RollingPolicy<IN, String> rollingPolicy,
            DeltaWriterBucketState bucketState,
            OutputFileConfig outputFileConfig,
            String appId)
            throws IOException {
        return DeltaWriterBucket.restore(bucketWriter, rollingPolicy, bucketState, outputFileConfig, appId);
    }
}
