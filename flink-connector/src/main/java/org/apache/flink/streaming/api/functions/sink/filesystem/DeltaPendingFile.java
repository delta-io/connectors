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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import javax.annotation.Nullable;


public class DeltaPendingFile {

    @Nullable
    private final String fileName;

    @Nullable
    private final InProgressFileWriter.PendingFileRecoverable pendingFile;

    @Nullable
    private final long recordCount;

    @Nullable
    private final long fileSize;


    public DeltaPendingFile(@Nullable String fileName,
                            @Nullable InProgressFileWriter.PendingFileRecoverable pendingFile,
                            long recordCount,
                            long fileSize) {
        this.fileName = fileName;
        this.pendingFile = pendingFile;
        this.fileSize = fileSize;
        this.recordCount = recordCount;
    }

    @Nullable
    public String getFileName() {
        return fileName;
    }

    @Nullable
    public InProgressFileWriter.PendingFileRecoverable getPendingFile() {
        return pendingFile;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getRecordCount() {
        return recordCount;
    }
}
