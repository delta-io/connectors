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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public final class DeltaPendingFileSerdeUtil {

    public static void serialize(
        DeltaPendingFile deltaPendingFile,
        DataOutputView dataOutputView,
        SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable>
            pendingFileSerializer) throws IOException {
        assert deltaPendingFile.getFileName() != null;
        assert deltaPendingFile.getPendingFile() != null;

        dataOutputView.writeInt(deltaPendingFile.getPartitionSpec().size());
        for (Map.Entry<String, String> entry : deltaPendingFile.getPartitionSpec().entrySet()) {
            dataOutputView.writeUTF(entry.getKey());
            dataOutputView.writeUTF(entry.getValue());
        }

        dataOutputView.writeUTF(deltaPendingFile.getFileName());
        dataOutputView.writeLong(deltaPendingFile.getRecordCount());
        dataOutputView.writeLong(deltaPendingFile.getFileSize());
        dataOutputView.writeLong(deltaPendingFile.getLastUpdateTime());

        SimpleVersionedSerialization.writeVersionAndSerialize(
            pendingFileSerializer,
            deltaPendingFile.getPendingFile(),
            dataOutputView
        );
    }

    public static DeltaPendingFile deserialize(
        DataInputView dataInputView,
        SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable>
            pendingFileSerializer) throws IOException {
        LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
        int partitionSpecEntriesCount = dataInputView.readInt();
        for (int i = 0; i < partitionSpecEntriesCount; i++) {
            partitionSpec.put(dataInputView.readUTF(), dataInputView.readUTF());
        }

        String pendingFileName = dataInputView.readUTF();
        long pendingFileRecordCount = dataInputView.readLong();
        long pendingFileSize = dataInputView.readLong();
        long lastUpdateTime = dataInputView.readLong();
        InProgressFileWriter.PendingFileRecoverable pendingFile =
            SimpleVersionedSerialization.readVersionAndDeSerialize(
                pendingFileSerializer, dataInputView);
        return new DeltaPendingFile(
            partitionSpec,
            pendingFileName,
            pendingFile,
            pendingFileRecordCount,
            pendingFileSize,
            lastUpdateTime);
    }
}
