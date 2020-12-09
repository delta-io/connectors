package io.delta.standalone;

import io.delta.standalone.actions.CommitInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import io.delta.standalone.internal.DeltaLogImpl;

/**
 * {@link DeltaLog} is the representation of the transaction logs of a Delta table. It provides APIs
 * to access the states of a Delta table.
 *
 * You can use the following codes to create a {@link DeltaLog} instance.
 * <pre>{@code
 *   Configuration conf = ... // Create your own Hadoop Configuration instance
 *   DeltaLog deltaLog = DeltaLog.forTable(conf, "/the/delta/table/path");
 * }</pre>
 */
public interface DeltaLog {

    /**
     * @return the current {@link Snapshot} of the Delta table. You may need to call
     * {@link #update()} to access the latest snapshot if the current snapshot is stale.
     */
    Snapshot snapshot();

    /**
     * Bring {@link DeltaLog}'s current {@link Snapshot} to the latest state if there are any new
     * transaction logs.
     *
     * @return the latest snapshot after applying the new transaction logs.
     */
    Snapshot update();

    /**
     * Travel back in time to the {@link Snapshot} with the provided {@code version} number.
     *
     * @param version  the snapshot version to generate
     * @return the snapshot at the provided {@code version}
     * @throws IllegalArgumentException if the {@code version} is outside the range of available versions
     */
    Snapshot getSnapshotForVersionAsOf(long version);

    /**
     * Travel back in time to the latest {@link Snapshot} that was generated at or before {@code timestamp}.
     *
     * @param timestamp  the number of milliseconds since midnight, January 1, 1970 UTC
     * @return the snapshot nearest to, but not after, the provided {@code timestamp}
     * @throws RuntimeException if the snapshot is unable to be recreated
     * @throws IllegalArgumentException if the {@code timestamp} is before the earliest possible snapshot or after the latest possible snapshot
     */
    Snapshot getSnapshotForTimestampAsOf(long timestamp);

    /**
     * @param version  the commit version to retrieve {@link CommitInfo}
     * @return the {@link CommitInfo} of the commit at the provided version.
     */
    CommitInfo getCommitInfoAt(long version);

    /** @return the path of the Delta table. */
    Path getPath();

    /**
     * Create a {@link DeltaLog} instance representing the table located at the provide {@code path}.
     *
     * @param hadoopConf  Hadoop {@code Configuration} to use when accessing the Delta table
     * @param path  the path to the Delta table
     * @return the {@code DeltaLog} for the provided {@code path}
     */
    static DeltaLog forTable(Configuration hadoopConf, String path) {
        return DeltaLogImpl.forTable(hadoopConf, path);
    }

    /**
     * Create a {@link DeltaLog} instance representing the table located at the provide {@code path}.
     *
     * @param hadoopConf  Hadoop {@code Configuration} to use when accessing the Delta table
     * @param path  the path to the Delta table
     * @return the {@code DeltaLog} for the provided {@code path}
     */
    static DeltaLog forTable(Configuration hadoopConf, Path path) {
        return DeltaLogImpl.forTable(hadoopConf, path);
    }
}
