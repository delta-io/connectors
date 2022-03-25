package io.delta.flink.source.internal.enumerator;

import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.emptyList;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.enumerator.monitor.TableMonitor;
import io.delta.flink.source.internal.enumerator.processor.ChangesProcessor;
import io.delta.flink.source.internal.enumerator.processor.ContinuousTableProcessor;
import io.delta.flink.source.internal.enumerator.processor.SnapshotAndChangesTableProcessor;
import io.delta.flink.source.internal.enumerator.processor.SnapshotProcessor;
import io.delta.flink.source.internal.enumerator.supplier.ContinuousSourceSnapshotSupplier;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_TIMESTAMP;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_VERSION;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;

/**
 * An implementation of {@link SplitEnumeratorProvider} that creates a {@code
 * ContinuousSplitEnumerator} used for {@link Boundedness#CONTINUOUS_UNBOUNDED} mode.
 */
public class ContinuousSplitEnumeratorProvider implements SplitEnumeratorProvider {

    private final FileSplitAssigner.Provider splitAssignerProvider;

    private final AddFileEnumerator.Provider<DeltaSourceSplit> fileEnumeratorProvider;

    /**
     * @param splitAssignerProvider  an instance of {@link FileSplitAssigner.Provider} that will be
     *                               used for building a {@code ContinuousSplitEnumerator} by
     *                               factory methods.
     * @param fileEnumeratorProvider an instance of {@link AddFileEnumerator.Provider} that will be
     *                               used for building a {@code ContinuousSplitEnumerator} by
     *                               factory methods.
     */
    public ContinuousSplitEnumeratorProvider(
        FileSplitAssigner.Provider splitAssignerProvider,
        AddFileEnumerator.Provider<DeltaSourceSplit> fileEnumeratorProvider) {
        this.splitAssignerProvider = splitAssignerProvider;
        this.fileEnumeratorProvider = fileEnumeratorProvider;
    }

    @Override
    public SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        createInitialStateEnumerator(Path deltaTablePath, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration) {

        DeltaLog deltaLog =
            DeltaLog.forTable(configuration, SourceUtils.pathToString(deltaTablePath));

        Snapshot snapshot =
            new ContinuousSourceSnapshotSupplier(deltaLog, sourceConfiguration).getSnapshot();

        ContinuousTableProcessor tableProcessor =
            setUpTableProcessor(deltaTablePath, enumContext, sourceConfiguration, deltaLog,
                snapshot);

        return new ContinuousDeltaSourceSplitEnumerator(
            deltaTablePath, tableProcessor, splitAssignerProvider.create(emptyList()), enumContext);
    }

    @SuppressWarnings("unchecked")
    @Override
    public SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        createEnumeratorForCheckpoint(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration) {

        ContinuousTableProcessor tableProcessor =
            setUpTableProcessorFromCheckpoint(checkpoint, configuration, enumContext,
                sourceConfiguration);

        Collection<FileSourceSplit> checkpointSplits =
            (Collection<FileSourceSplit>) (Collection<?>) checkpoint.getSplits();

        return new ContinuousDeltaSourceSplitEnumerator(
            checkpoint.getDeltaTablePath(), tableProcessor, splitAssignerProvider.create(
            checkpointSplits), enumContext);
    }

    /**
     * @return A {@link ContinuousTableProcessor} instance using
     * {@link DeltaEnumeratorStateCheckpoint}
     * data.
     * <p>
     * @implNote If {@link DeltaSourceOptions#STARTING_VERSION} or {@link
     * DeltaSourceOptions#STARTING_TIMESTAMP} options were defined or if Enumerator already
     * processed initial Snapshot, the returned ContinuousTableProcessor instance will process only
     * changes applied to monitored Delta table.
     */
    private ContinuousTableProcessor setUpTableProcessorFromCheckpoint(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint, Configuration configuration,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration) {
        long snapshotVersion = checkpoint.getSnapshotVersion();

        DeltaLog deltaLog = DeltaLog.forTable(configuration,
            SourceUtils.pathToString(checkpoint.getDeltaTablePath()));

        if (checkpoint.isMonitoringForChanges()) {
            return setUpChangesProcessor(enumContext, sourceConfiguration, deltaLog,
                snapshotVersion);
        } else {
            return
                setUpSnapshotAndChangesProcessor(checkpoint.getDeltaTablePath(), enumContext,
                    sourceConfiguration, deltaLog,
                    deltaLog.getSnapshotForVersionAsOf(snapshotVersion));
        }
    }

    /**
     * @return A {@link ContinuousTableProcessor} instance.
     * <p>
     * @implNote If {@link DeltaSourceOptions#STARTING_VERSION} or {@link
     * DeltaSourceOptions#STARTING_TIMESTAMP} options were defined the returned
     * ContinuousTableProcessor instance will process only changes applied to monitored Delta
     * table.
     */
    private ContinuousTableProcessor setUpTableProcessor(Path deltaTablePath,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration, DeltaLog deltaLog, Snapshot snapshot) {

        if (isChangeStreamOnly(sourceConfiguration)) {
            return
                setUpChangesProcessor(enumContext, sourceConfiguration, deltaLog,
                    snapshot.getVersion());
        } else {
            return
                setUpSnapshotAndChangesProcessor(deltaTablePath, enumContext, sourceConfiguration,
                    deltaLog, snapshot);
        }
    }

    private ChangesProcessor setUpChangesProcessor(
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration, DeltaLog deltaLog,
        long monitorSnapshotVersion) {
        TableMonitor tableMonitor =
            new TableMonitor(deltaLog, monitorSnapshotVersion, sourceConfiguration.getValue(
                DeltaSourceOptions.UPDATE_CHECK_INTERVAL));

        return new ChangesProcessor(tableMonitor, enumContext, Collections.emptySet());
    }

    private ContinuousTableProcessor setUpSnapshotAndChangesProcessor(Path deltaTablePath,
        SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaSourceConfiguration sourceConfiguration, DeltaLog deltaLog, Snapshot snapshot) {

        // Since this is the processor for both snapshot and changes, the version for which we
        // should start monitoring for changes is snapshot.version + 1. We don't want to get
        // changes from snapshot.version.
        ChangesProcessor changesProcessor =
            setUpChangesProcessor(enumContext, sourceConfiguration, deltaLog,
                snapshot.getVersion() + 1);

        SnapshotProcessor snapshotProcessor =
            new SnapshotProcessor(deltaTablePath, snapshot,
                fileEnumeratorProvider.create(), Collections.emptySet());

        return new SnapshotAndChangesTableProcessor(snapshotProcessor, changesProcessor);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    private boolean isChangeStreamOnly(DeltaSourceConfiguration sourceConfiguration) {
        return
            sourceConfiguration.hasOption(STARTING_VERSION) ||
                sourceConfiguration.hasOption(STARTING_TIMESTAMP);
    }
}
