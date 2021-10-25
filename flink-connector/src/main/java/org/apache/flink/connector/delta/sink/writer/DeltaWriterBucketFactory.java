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
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;

import java.io.IOException;
import java.io.Serializable;


@Internal
public interface DeltaWriterBucketFactory<IN> extends Serializable {

    DeltaWriterBucket<IN> getNewBucket(
            String bucketId,
            Path bucketPath,
            DeltaBulkBucketWriter<IN, String> bucketWriter,
            CheckpointRollingPolicy<IN, String> rollingPolicy,
            OutputFileConfig outputFileConfig)
            throws IOException;

    DeltaWriterBucket<IN> restoreBucket(
            DeltaBulkBucketWriter<IN, String> bucketWriter,
            CheckpointRollingPolicy<IN, String> rollingPolicy,
            DeltaWriterBucketState bucketState,
            OutputFileConfig outputFileConfig)
            throws IOException;
}
