package io.delta.flink.source;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class DeltaPendingSplitsCheckpointSerializerTest {

    private static final long SNAPSHOT_VERSION = 1L;

    @Test
    public void serializeEmptyCheckpoint() throws Exception {
        DeltaPendingSplitsCheckpoint<DeltaSourceSplit> checkpoint =
            DeltaPendingSplitsCheckpoint.fromCollectionSnapshot(-1, Collections.emptyList());

        DeltaPendingSplitsCheckpoint<DeltaSourceSplit> deSerialized =
            serializeAndDeserialize(checkpoint);

        assertCheckpointsEqual(checkpoint, deSerialized);
    }

    @Test
    public void serializeSomeSplits() throws Exception {
        DeltaPendingSplitsCheckpoint<DeltaSourceSplit> checkpoint =
            DeltaPendingSplitsCheckpoint.fromCollectionSnapshot(SNAPSHOT_VERSION,
                Arrays.asList(testSplitNoPartitions(), testSplitSinglePartition(),
                    testSplitMultiplePartitions()));

        DeltaPendingSplitsCheckpoint<DeltaSourceSplit> deSerialized =
            serializeAndDeserialize(checkpoint);

        assertCheckpointsEqual(checkpoint, deSerialized);
    }

    @Test
    public void serializeSplitsAndProcessedPaths() throws Exception {
        DeltaPendingSplitsCheckpoint<DeltaSourceSplit> checkpoint =
            DeltaPendingSplitsCheckpoint.fromCollectionSnapshot(SNAPSHOT_VERSION,
                Arrays.asList(testSplitNoPartitions(), testSplitSinglePartition(),
                    testSplitMultiplePartitions()),
                Arrays.asList(
                    new Path("file:/some/path"),
                    new Path("s3://bucket/key/and/path"),
                    new Path("hdfs://namenode:12345/path")));

        DeltaPendingSplitsCheckpoint<DeltaSourceSplit> deSerialized =
            serializeAndDeserialize(checkpoint);

        assertCheckpointsEqual(checkpoint, deSerialized);
    }

    private DeltaPendingSplitsCheckpoint<DeltaSourceSplit> serializeAndDeserialize(
        DeltaPendingSplitsCheckpoint<DeltaSourceSplit> split) throws IOException {

        DeltaPendingSplitsCheckpointSerializer<DeltaSourceSplit> serializer =
            new DeltaPendingSplitsCheckpointSerializer<>(DeltaSourceSplitSerializer.INSTANCE);
        byte[] bytes =
            SimpleVersionedSerialization.writeVersionAndSerialize(serializer, split);
        return SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);
    }

    private void assertCheckpointsEqual(
        DeltaPendingSplitsCheckpoint<DeltaSourceSplit> expected,
        DeltaPendingSplitsCheckpoint<DeltaSourceSplit> actual) {

        assertEquals(expected.getInitialSnapshotVersion(), actual.getInitialSnapshotVersion());

        assertOrderedCollectionEquals(
            expected.getSplits(),
            actual.getSplits(),
            DeltaSourceSplitSerializerTest::assertSplitsEqual);

        assertOrderedCollectionEquals(
            expected.getAlreadyProcessedPaths(),
            actual.getAlreadyProcessedPaths(),
            Assert::assertEquals);
    }

    private DeltaSourceSplit testSplitNoPartitions() {
        return new DeltaSourceSplit(
            Collections.emptyMap(),
            "random-id",
            new Path("hdfs://namenode:14565/some/path/to/a/file"),
            100_000_000,
            64_000_000,
            "host1",
            "host2",
            "host3");
    }

    private DeltaSourceSplit testSplitSinglePartition() {
        return new DeltaSourceSplit(Collections.singletonMap("col1", "val1"), "some-id",
            new Path("file:/some/path/to/a/file"), 0, 0);
    }

    private DeltaSourceSplit testSplitMultiplePartitions() {
        Map<String, String> partitions = new HashMap<>();
        partitions.put("col1", "val1");
        partitions.put("col2", "val2");
        partitions.put("col3", "val3");

        return new DeltaSourceSplit(
            partitions, "an-id", new Path("s3://some-bucket/key/to/the/object"), 0, 1234567);
    }

    private <E> void assertOrderedCollectionEquals(
        Collection<E> expected, Collection<E> actual, BiConsumer<E, E> equalityAsserter) {

        assertEquals(expected.size(), actual.size());
        Iterator<E> expectedIter = expected.iterator();
        Iterator<E> actualIter = actual.iterator();
        while (expectedIter.hasNext()) {
            equalityAsserter.accept(expectedIter.next(), actualIter.next());
        }
    }

}
