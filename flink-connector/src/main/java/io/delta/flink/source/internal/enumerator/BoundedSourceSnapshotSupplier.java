package io.delta.flink.source.internal.enumerator;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.utils.TransitiveOptional;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;

/**
 * An implementation of {@link SnapshotSupplier} for {#link
 * {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED}}
 * mode.
 */
public class BoundedSourceSnapshotSupplier extends SnapshotSupplier {

    public BoundedSourceSnapshotSupplier(DeltaLog deltaLog,
        DeltaSourceConfiguration sourceConfiguration) {
        super(deltaLog, sourceConfiguration);
    }

    /**
     * This method returns a {@link Snapshot} instance acquired from {@link #deltaLog}. This
     * implementation tries to quire the {@code Snapshot} in below order, stopping at first
     * non-empty result:
     * <ul>
     *     <li>If {@link DeltaSourceOptions#VERSION_AS_OF} was specified, use it to call
     *     {@link DeltaLog#getSnapshotForVersionAsOf(long)}.</li>
     *     <li>If {@link DeltaSourceOptions#TIMESTAMP_AS_OF} was specified, use it to call
     *     {@link DeltaLog#getSnapshotForTimestampAsOf(long)}.</li>
     *     <li>Get the head version using {@link DeltaLog#snapshot()}</li>
     * </ul>
     *
     * @return A {@link Snapshot} instance or throws {@link java.util.NoSuchElementException} if no
     * snapshot was found.
     */
    @Override
    public Snapshot getSnapshot() {
        return getSnapshotFromVersionAsOfOption()
            .or(this::getSnapshotFromTimestampAsOfOption)
            .or(this::getHeadSnapshot)
            .get();
    }

    private TransitiveOptional<Snapshot> getSnapshotFromVersionAsOfOption() {
        Long versionAsOf = getOptionValue(DeltaSourceOptions.VERSION_AS_OF);
        if (versionAsOf != null) {
            return TransitiveOptional.ofNullable(deltaLog.getSnapshotForVersionAsOf(versionAsOf));
        }
        return TransitiveOptional.empty();
    }

    private TransitiveOptional<Snapshot> getSnapshotFromTimestampAsOfOption() {
        Long timestampAsOf = getOptionValue(DeltaSourceOptions.TIMESTAMP_AS_OF);
        if (timestampAsOf != null) {
            return TransitiveOptional.ofNullable(
                deltaLog.getSnapshotForTimestampAsOf(timestampAsOf));
        }
        return TransitiveOptional.empty();
    }
}
