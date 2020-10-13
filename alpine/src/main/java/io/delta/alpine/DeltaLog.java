package io.delta.alpine;

import java.io.File;

import io.delta.alpine.internal.DeltaLogImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public interface DeltaLog {
    Snapshot snapshot();
    Snapshot update();
    Snapshot getSnapshotForVersionAsOf(long version);
    Snapshot getSnapshotForTimestampAsOf(long timestamp);
    Configuration getHadoopConf();
    Path getLogPath();
    Path getDataPath();

    static DeltaLog forTable(Configuration hadoopConf, String dataPath) {
        return DeltaLogImpl.forTable(hadoopConf, dataPath);
    }

    static DeltaLog forTable(Configuration hadoopConf, File dataPath) {
        return DeltaLogImpl.forTable(hadoopConf, dataPath);
    }

    static DeltaLog forTable(Configuration hadoopConf, Path dataPath) {
        return DeltaLogImpl.forTable(hadoopConf, dataPath);
    }
}
