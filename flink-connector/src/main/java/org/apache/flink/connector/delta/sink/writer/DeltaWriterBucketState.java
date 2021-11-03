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

@Internal
public class DeltaWriterBucketState {

    private final String bucketId;

    /**
     * The directory where all the part files of the bucket are stored.
     */
    private final Path bucketPath;

    private final String appId;

    public DeltaWriterBucketState(
            String bucketId,
            Path bucketPath,
            String appId) {
        this.bucketId = bucketId;
        this.bucketPath = bucketPath;
        this.appId = appId;
    }

    public String getBucketId() {
        return bucketId;
    }

    public Path getBucketPath() {
        return bucketPath;
    }

    @Override
    public String toString() {
        final StringBuilder strBuilder = new StringBuilder();

        strBuilder
                .append("BucketState for bucketId=")
                .append(bucketId)
                .append(" and bucketPath=")
                .append(bucketPath);

        return strBuilder.toString();
    }

    public String getAppId() {
        return appId;
    }

}
