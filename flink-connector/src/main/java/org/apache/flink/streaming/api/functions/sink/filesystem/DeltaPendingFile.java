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


import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class DeltaPendingFile {

    private final LinkedHashMap<String, String> partitionSpec;

    private final String fileName;

    private final InProgressFileWriter.PendingFileRecoverable pendingFile;

    private final long recordCount;

    private final long fileSize;

    private final long lastUpdateTime;


    public DeltaPendingFile(LinkedHashMap<String, String> partitionSpec,
                            String fileName,
                            InProgressFileWriter.PendingFileRecoverable pendingFile,
                            long recordCount,
                            long fileSize,
                            long lastUpdateTime) {
        this.partitionSpec = partitionSpec;
        this.fileName = fileName;
        this.pendingFile = pendingFile;
        this.fileSize = fileSize;
        this.recordCount = recordCount;
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getFileName() {
        return fileName;
    }

    public InProgressFileWriter.PendingFileRecoverable getPendingFile() {
        return pendingFile;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public LinkedHashMap<String, String> getPartitionSpec() {
        return new LinkedHashMap<>(partitionSpec);
    }

}
