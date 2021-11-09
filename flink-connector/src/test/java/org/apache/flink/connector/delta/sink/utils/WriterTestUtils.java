package org.apache.flink.connector.delta.sink.utils;

import java.io.IOException;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.fs.local.LocalRecoverableWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.data.RowData;
import static org.junit.Assert.fail;

public class WriterTestUtils {


    public static final OnCheckpointRollingPolicy<RowData, String> ON_CHECKPOINT_ROLLING_POLICY =
        OnCheckpointRollingPolicy.build();

    public static LocalRecoverableWriter getRecoverableWriter(Path path) {
        try {
            final FileSystem fs = FileSystem.get(path.toUri());
            if (!(fs instanceof LocalFileSystem)) {
                fail(
                    "Expected Local FS but got a "
                        + fs.getClass().getName()
                        + " for path: "
                        + path);
            }
            return new LocalRecoverableWriter((LocalFileSystem) fs);
        } catch (IOException e) {
            fail();
        }
        return null;
    }

    public static DeltaBulkBucketWriter<RowData, String> createBucketWriter(Path path)
        throws IOException {
        return new DeltaBulkBucketWriter<>(
            getRecoverableWriter(path),
            DeltaSinkTestUtils.TestParquetWriterFactory.createTestWriterFactory());
    }

}
