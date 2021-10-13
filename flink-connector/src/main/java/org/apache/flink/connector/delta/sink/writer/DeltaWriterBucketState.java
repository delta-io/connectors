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
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import javax.annotation.Nullable;

@Internal
public class DeltaWriterBucketState {

    private final String bucketId;

    /**
     * The directory where all the part files of the bucket are stored.
     */
    private final Path bucketPath;

    /**
     * The creation time of the currently open part file, or {@code Long.MAX_VALUE} if there is no
     * open part file.
     */
    private final long inProgressFileCreationTime;

    /**
     * A {@link InProgressFileWriter.InProgressFileRecoverable} for the currently open part file, or null if there is no
     * currently open part file.
     */
    @Nullable
    private final InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable;

    @Nullable
    private final String inProgressPartFileName;

    private final long recordCount;

    private final long inProgressPartFileSize;

    private final String appId;

    public DeltaWriterBucketState(
            String bucketId,
            Path bucketPath,
            long inProgressFileCreationTime,
            @Nullable InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable,
            @Nullable String inProgressPartFileName,
            long recordCount,
            long inProgressPartFileSize,
            String appId) {
        this.bucketId = bucketId;
        this.bucketPath = bucketPath;
        this.inProgressFileCreationTime = inProgressFileCreationTime;
        this.inProgressFileRecoverable = inProgressFileRecoverable;
        this.inProgressPartFileName = inProgressPartFileName;
        this.recordCount = recordCount;
        this.inProgressPartFileSize = inProgressPartFileSize;
        this.appId = appId;
    }

    public String getBucketId() {
        return bucketId;
    }

    public Path getBucketPath() {
        return bucketPath;
    }

    public long getInProgressFileCreationTime() {
        return inProgressFileCreationTime;
    }

    @Nullable
    InProgressFileWriter.InProgressFileRecoverable getInProgressFileRecoverable() {
        return inProgressFileRecoverable;
    }

    @Nullable
    String getInProgressPartFileName() {
        return inProgressPartFileName;
    }


    boolean hasInProgressFileRecoverable() {
        return inProgressFileRecoverable != null;
    }


    @Override
    public String toString() {
        final StringBuilder strBuilder = new StringBuilder();

        strBuilder
                .append("BucketState for bucketId=")
                .append(bucketId)
                .append(" and bucketPath=")
                .append(bucketPath);

        if (hasInProgressFileRecoverable()) {
            strBuilder.append(", has open part file created @ ").append(inProgressFileCreationTime);
        }

        return strBuilder.toString();
    }

    public long getRecordCount() {
        return recordCount;
    }

    public long getInProgressPartFileSize() {
        return inProgressPartFileSize;
    }

    public String getAppId() {
        return appId;
    }
}
