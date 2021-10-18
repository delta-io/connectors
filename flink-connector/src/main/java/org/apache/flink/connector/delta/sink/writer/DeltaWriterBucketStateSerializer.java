/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.util.function.FunctionWithException;

import java.io.IOException;

import static org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.InProgressFileRecoverable;
import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class DeltaWriterBucketStateSerializer
        implements SimpleVersionedSerializer<DeltaWriterBucketState> {

    private static final int MAGIC_NUMBER = 0x1e764b79;

    private final SimpleVersionedSerializer<InProgressFileRecoverable>
            inProgressFileRecoverableSerializer;


    public DeltaWriterBucketStateSerializer(
            SimpleVersionedSerializer<InProgressFileRecoverable> inProgressFileRecoverableSerializer
    ) {
        this.inProgressFileRecoverableSerializer = checkNotNull(inProgressFileRecoverableSerializer);
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(DeltaWriterBucketState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        serializeV1(state, out);
        return out.getCopyOfBuffer();
    }

    @Override
    public DeltaWriterBucketState deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        if (version == 1) {
            validateMagicNumber(in);
            return deserializeV1(in);
        }
        throw new IOException("Unrecognized version or corrupt state: " + version);
    }

    private void serializeV1(DeltaWriterBucketState state, DataOutputView dataOutputView)
            throws IOException {
        SimpleVersionedSerialization.writeVersionAndSerialize(
                SimpleVersionedStringSerializer.INSTANCE, state.getBucketId(), dataOutputView);
        dataOutputView.writeUTF(state.getBucketPath().toString());
        dataOutputView.writeLong(state.getInProgressFileCreationTime());
        dataOutputView.writeLong(state.getLastUpdateTime());
        dataOutputView.writeUTF(state.getAppId());


        // put the current open part file
        if (state.hasInProgressFileRecoverable()) {
            InProgressFileRecoverable inProgressFileRecoverable = state.getInProgressFileRecoverable();
            assert inProgressFileRecoverable != null;
            assert state.getInProgressPartFileName() != null;
            dataOutputView.writeBoolean(true);
            dataOutputView.writeUTF(state.getInProgressPartFileName());
            dataOutputView.writeLong(state.getRecordCount());
            dataOutputView.writeLong(state.getInProgressPartFileSize());
            SimpleVersionedSerialization.writeVersionAndSerialize(
                    inProgressFileRecoverableSerializer,
                    inProgressFileRecoverable,
                    dataOutputView
            );
        } else {
            dataOutputView.writeBoolean(false);
        }
    }

    private DeltaWriterBucketState deserializeV1(DataInputView in) throws IOException {
        return internalDeserialize(
                in,
                dataInputView -> SimpleVersionedSerialization.readVersionAndDeSerialize(inProgressFileRecoverableSerializer, dataInputView)
        );
    }

    private DeltaWriterBucketState internalDeserialize(
            DataInputView dataInputView,
            FunctionWithException<DataInputView, InProgressFileRecoverable, IOException> inProgressFileParser
    ) throws IOException {

        String bucketId = SimpleVersionedSerialization.readVersionAndDeSerialize(
                SimpleVersionedStringSerializer.INSTANCE,
                dataInputView
        );

        String bucketPathStr = dataInputView.readUTF();
        long creationTime = dataInputView.readLong();
        long lastUpdateTime = dataInputView.readLong();
        String appId = dataInputView.readUTF();

        // then get the current resumable stream
        InProgressFileRecoverable current = null;
        String inprogressFileName = null;
        long recordCount = 0;
        long inProgressPartFileSize = 0;
        if (dataInputView.readBoolean()) {
            inprogressFileName = dataInputView.readUTF();
            recordCount = dataInputView.readLong();
            inProgressPartFileSize = dataInputView.readLong();
            current = inProgressFileParser.apply(dataInputView);
        }

        return new DeltaWriterBucketState(
                bucketId,
                new Path(bucketPathStr),
                creationTime,
                current,
                inprogressFileName,
                recordCount,
                inProgressPartFileSize,
                lastUpdateTime,
                appId
        );
    }

    private void validateMagicNumber(DataInputView in) throws IOException {
        int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                    String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }

}
