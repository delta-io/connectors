package io.delta.flink.e2e.assertions;

import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.standalone.DeltaLog;

public class DeltaLogAssertions {

    public static DeltaLogAsserter assertThat(DeltaLog deltaLog) {
        return new DeltaLogAsserter(deltaLog);
    }

    public static class DeltaLogAsserter {

        private final DeltaLog deltaLog;
        private long referenceVersion;  // TODO: assert is set before using

        public DeltaLogAsserter(DeltaLog deltaLog) {
            this.deltaLog = deltaLog;
        }

        public DeltaLogAsserter sinceVersion(long referenceVersion) {
            this.referenceVersion = referenceVersion;
            return this;
        }

        public DeltaLogAsserter hasNewRecordsCount(int expectedNewRecordsCount) {
            long currentVersion = deltaLog.update().getVersion();
            long actualNewRecordsCount = LongStream.range(referenceVersion + 1, currentVersion + 1)
                .map(this::getNumOutputRowsForVersion)
                .sum();
            assertEquals(expectedNewRecordsCount, actualNewRecordsCount);
            return this;
        }

        private long getNumOutputRowsForVersion(long version) {
            Map<String, String> currentMetrics = getOperationMetricsForVersion(version);
            return Long.parseLong(currentMetrics.get("numOutputRows"));
        }

        private long getNumAddedFilesForVersion(long version) {
            Map<String, String> currentMetrics = getOperationMetricsForVersion(version);
            return Long.parseLong(currentMetrics.get("numAddedFiles"));
        }

        private Map<String, String> getOperationMetricsForVersion(long version) {
            Optional<Map<String, String>> operationMetrics = deltaLog.getCommitInfoAt(version)
                .getOperationMetrics();
            assertTrue(operationMetrics.isPresent());
            return operationMetrics.get();
        }

    }

}
