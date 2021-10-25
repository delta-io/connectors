package org.apache.flink.connector.delta.sink.utils;

import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaPendingFile;

import java.util.LinkedHashMap;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DeltaSinkTestUtils {

    public static class TestDeltaPendingFile {
        public static DeltaPendingFile getTestDeltaPendingFile(LinkedHashMap<String, String> partitionSpec) {
            return new DeltaPendingFile(
                    partitionSpec,
                    "file_name",
                    new FileSinkTestUtils.TestPendingFileRecoverable(),
                    new Random().nextInt(30000),
                    new Random().nextInt(500000),
                    System.currentTimeMillis()
            );
        }
    }

    public static class TestDeltaCommittable {

        static final String TEST_APP_ID = UUID.randomUUID().toString();
        static final long TEST_CHECKPOINT_ID = new Random().nextInt(10);

        public static DeltaCommittable getTestDeltaCommittableWithPendingFile(LinkedHashMap<String, String> partitionSpec) {
            return new DeltaCommittable(
                    TestDeltaPendingFile.getTestDeltaPendingFile(partitionSpec),
                    TEST_APP_ID,
                    TEST_CHECKPOINT_ID
            );
        }

        public static DeltaCommittable getTestDeltaCommittableWithInProgressFiles() {
            return new DeltaCommittable(
                    new FileSinkTestUtils.TestInProgressFileRecoverable(),
                    TEST_APP_ID,
                    TEST_CHECKPOINT_ID
            );
        }

        public static void validateDeltaCommittablesEquality(DeltaCommittable committable,
                                                             DeltaCommittable deserialized,
                                                             LinkedHashMap<String, String> expectedPartitionSpec) {
            assertNull(committable.getInProgressFileToCleanup());
            assertNull(deserialized.getInProgressFileToCleanup());
            assertEquals(committable.getDeltaPendingFile().getPendingFile(), deserialized.getDeltaPendingFile().getPendingFile());
            assertEquals(committable.getCheckpointId(), deserialized.getCheckpointId());
            assertEquals(committable.getAppId(), deserialized.getAppId());
            assertEquals(committable.getDeltaPendingFile().getFileName(), deserialized.getDeltaPendingFile().getFileName());
            assertEquals(committable.getDeltaPendingFile().getFileSize(), deserialized.getDeltaPendingFile().getFileSize());
            assertEquals(committable.getDeltaPendingFile().getRecordCount(), deserialized.getDeltaPendingFile().getRecordCount());
            assertEquals(committable.getDeltaPendingFile().getLastUpdateTime(), deserialized.getDeltaPendingFile().getLastUpdateTime());
            assertEquals(
                    committable.getInProgressFileToCleanup(),
                    deserialized.getInProgressFileToCleanup()
            );
            assertEquals(expectedPartitionSpec, deserialized.getDeltaPendingFile().getPartitionSpec());
        }
    }

}
