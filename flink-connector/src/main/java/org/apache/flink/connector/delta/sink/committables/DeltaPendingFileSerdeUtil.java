package org.apache.flink.connector.delta.sink.committables;

import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaPendingFile;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import java.io.IOException;


public final class DeltaPendingFileSerdeUtil {

    static void serialize(DeltaPendingFile deltaPendingFile,
                          DataOutputView dataOutputView,
                          SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable>
                                  pendingFileSerializer) throws IOException {
        assert deltaPendingFile.getFileName() != null;
        assert deltaPendingFile.getPendingFile() != null;
        dataOutputView.writeUTF(deltaPendingFile.getFileName());
        dataOutputView.writeLong(deltaPendingFile.getRecordCount());
        dataOutputView.writeLong(deltaPendingFile.getFileSize());
        SimpleVersionedSerialization.writeVersionAndSerialize(
                pendingFileSerializer,
                deltaPendingFile.getPendingFile(),
                dataOutputView
        );
    }

    static DeltaPendingFile deserialize(DataInputView dataInputView,
                                        SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable>
                                                pendingFileSerializer) throws IOException {
        String pendingFileName = dataInputView.readUTF();
        long pendingFileRecordCount = dataInputView.readLong();
        long pendingFileSize = dataInputView.readLong();
        InProgressFileWriter.PendingFileRecoverable pendingFile =
                SimpleVersionedSerialization.readVersionAndDeSerialize(
                        pendingFileSerializer, dataInputView);
        return new DeltaPendingFile(pendingFileName, pendingFile, pendingFileRecordCount, pendingFileSize);
    }

}
