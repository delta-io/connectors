package io.delta.flink.source.internal.enumerator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import static java.util.Collections.singletonList;

import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.enumerator.monitor.TableMonitor;
import io.delta.flink.source.internal.enumerator.monitor.TableMonitorResult;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpointBuilder;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.delta.standalone.Snapshot;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.AddFile;

@RunWith(MockitoJUnitRunner.class)
public class ContinuousDeltaSourceSplitEnumeratorTest extends DeltaSourceSplitEnumeratorTestBase {

    @Mock
    private Snapshot startingVersionSnapshot;

    private ContinuousDeltaSourceSplitEnumerator enumerator;

    private ContinuousSplitEnumeratorProvider provider;

    @Captor
    private ArgumentCaptor<TableMonitor> tableMonitorArgumentCaptor;

    @Before
    public void setUp() {
        super.setUp();

        when(splitAssignerProvider.create(Mockito.any())).thenReturn(splitAssigner);
        when(fileEnumeratorProvider.create()).thenReturn(fileEnumerator);

        provider =
            new ContinuousSplitEnumeratorProvider(splitAssignerProvider, fileEnumeratorProvider);
    }

    @After
    public void after() {
        super.after();
    }

    @Test
    public void shouldNotReadInitialSnapshotWhenMonitoringForChanges() {

        long snapshotVersion = 10;
        long monitorVersion = snapshotVersion + 1;

        List<VersionLog> changes = mockEnumContextAndTableChange(snapshotVersion);
        long nextMonitoringVersion = changes.get(changes.size() - 1).getVersion() + 1;

        when(deltaLog.getChanges(monitorVersion, true)).thenReturn(changes.iterator());
        when(deltaLog.getPath()).thenReturn(new Path("s3//some/path"));

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint =
            DeltaEnumeratorStateCheckpointBuilder
                .builder(deltaTablePath, snapshotVersion, Collections.emptyList())
                .withMonitoringForChanges(true)
                .build();

        enumerator = setUpEnumeratorFromCheckpoint(checkpoint);
        enumerator.start();

        // verify that we did not create any snapshot, we only need to get changes from deltaLog.
        verify(deltaLog, never()).snapshot();
        verify(deltaLog, never()).getSnapshotForVersionAsOf(anyLong());
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());

        // verify that we try to get changes from Delta Log.
        verify(enumContext).callAsync(
            tableMonitorArgumentCaptor.capture(), any(BiConsumer.class), anyLong(), anyLong());
        verify(deltaLog).getChanges(monitorVersion, true);

        // verify TableMonitor starting version
        assertThat(tableMonitorArgumentCaptor.getValue().getMonitorVersion(),
            equalTo(nextMonitoringVersion));
    }

    @Test
    public void shouldReadInitialSnapshotWhenNotMonitoringForChanges() {
        long snapshotVersion = 10;
        long monitorVersion = snapshotVersion + 1;

        when(deltaLog.getSnapshotForVersionAsOf(snapshotVersion)).thenReturn(checkpointedSnapshot);
        when(checkpointedSnapshot.getVersion()).thenReturn(snapshotVersion);

        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint =
            DeltaEnumeratorStateCheckpointBuilder
                .builder(deltaTablePath, snapshotVersion, Collections.emptyList())
                .withMonitoringForChanges(false)
                .build();

        enumerator = setUpEnumeratorFromCheckpoint(checkpoint);
        enumerator.start();

        // verify that snapshot was created using version from checkpoint and not head or timestamp.
        verify(deltaLog).getSnapshotForVersionAsOf(snapshotVersion);
        verify(deltaLog, never()).snapshot();
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());

        // verify that we tried to read initial snapshot content.
        verify(checkpointedSnapshot).getAllFiles();

        // verify TableMonitor starting version
        verify(enumContext).callAsync(tableMonitorArgumentCaptor.capture(), any(),
            anyLong(), anyLong());
        assertThat(tableMonitorArgumentCaptor.getValue().getMonitorVersion(),
            equalTo(monitorVersion));
    }

    @Test
    public void shouldNotSignalNoMoreSplitsIfNone() {
        int subtaskId = 1;
        enumerator = setUpEnumeratorWithHeadSnapshot();

        when(enumContext.registeredReaders()).thenReturn(
            Collections.singletonMap(subtaskId, readerInfo));

        enumerator.handleSplitRequest(subtaskId, "testHost");

        verify(enumerator).handleNoMoreSplits(subtaskId);
        verify(enumContext, never()).signalNoMoreSplits(subtaskId);
    }

    @Test
    public void shouldOnlyReadChangesWhenStartingVersionOption() {
        long startingVersion = 10;
        sourceConfiguration.addOption(
            DeltaSourceOptions.STARTING_VERSION.key(), String.valueOf(startingVersion));

        List<VersionLog> changes = mockEnumContextAndTableChange(startingVersion);
        long nextMonitoringVersion = changes.get(changes.size() - 1).getVersion() + 1;

        when(deltaLog.getChanges(startingVersion, true)).thenReturn(changes.iterator());
        when(deltaLog.getPath()).thenReturn(new Path("s3//some/path"));

        when(deltaLog.getSnapshotForVersionAsOf(startingVersion)).thenReturn(
            startingVersionSnapshot);
        when(startingVersionSnapshot.getVersion()).thenReturn(startingVersion);

        enumerator = setUpEnumerator();
        enumerator.start();

        // verify that get snapshot for startingVersion
        verify(deltaLog).getSnapshotForVersionAsOf(startingVersion);
        verify(deltaLog, never()).snapshot();
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());

        // verify that we did not read from startingVersionSnapshot
        verify(startingVersionSnapshot, never()).getAllFiles();

        // verify that we try to get changes from Delta Log.
        verify(enumContext).callAsync(
            tableMonitorArgumentCaptor.capture(), any(BiConsumer.class), anyLong(), anyLong());
        verify(deltaLog).getChanges(startingVersion, true);

        // verify TableMonitor starting version
        assertThat(tableMonitorArgumentCaptor.getValue().getMonitorVersion(),
            equalTo(nextMonitoringVersion));
    }

    @Test
    public void shouldOnlyReadChangesWhenLatestStartingVersionOption() {
        long startingVersion = 10;
        sourceConfiguration.addOption(DeltaSourceOptions.STARTING_VERSION.key(), "latest");

        List<VersionLog> changes = mockEnumContextAndTableChange(startingVersion);
        long nextMonitoringVersion = changes.get(changes.size() - 1).getVersion() + 1;

        when(deltaLog.getChanges(startingVersion, true)).thenReturn(changes.iterator());
        when(deltaLog.getPath()).thenReturn(new Path("s3//some/path"));
        when(headSnapshot.getVersion()).thenReturn(startingVersion);

        enumerator = setUpEnumeratorWithHeadSnapshot();
        enumerator.start();

        // verify that get snapshot from head since "latest" was used as startingVersion value.
        verify(deltaLog).snapshot();
        verify(deltaLog, never()).getSnapshotForVersionAsOf(anyLong());
        verify(deltaLog, never()).getSnapshotForTimestampAsOf(anyLong());

        // verify that we did not read from startingVersionSnapshot
        verify(startingVersionSnapshot, never()).getAllFiles();

        // verify that we try to get changes from Delta Log.
        verify(enumContext).callAsync(
            tableMonitorArgumentCaptor.capture(), any(BiConsumer.class), anyLong(), anyLong());
        verify(deltaLog).getChanges(startingVersion, true);

        // verify TableMonitor starting version
        assertThat(tableMonitorArgumentCaptor.getValue().getMonitorVersion(),
            equalTo(nextMonitoringVersion));
    }

    @Test
    public void shouldOnlyReadChangesWhenStartingTimestampOption() {
        String startingTimestampString = "2022-02-24T04:55:00.001Z";
        long startingTimestamp = 1645678500001L;
        long startingVersion = 10;

        sourceConfiguration.addOption(
            DeltaSourceOptions.STARTING_TIMESTAMP.key(), startingTimestampString);

        List<VersionLog> changes = mockEnumContextAndTableChange(startingVersion);
        long nextMonitoringVersion = changes.get(changes.size() - 1).getVersion() + 1;

        when(deltaLog.getChanges(startingVersion, true)).thenReturn(changes.iterator());
        when(deltaLog.getPath()).thenReturn(new Path("s3//some/path"));

        when(deltaLog.getSnapshotForTimestampAsOf(startingTimestamp))
            .thenReturn(startingVersionSnapshot);
        when(startingVersionSnapshot.getVersion()).thenReturn(startingVersion);

        enumerator = setUpEnumerator();
        enumerator.start();

        // verify that get snapshot for startingTimestamp
        verify(deltaLog).getSnapshotForTimestampAsOf(startingTimestamp);
        verify(deltaLog, never()).getSnapshotForVersionAsOf(anyLong());
        verify(deltaLog, never()).snapshot();

        // verify that we did not read from startingVersionSnapshot
        verify(startingVersionSnapshot, never()).getAllFiles();

        // verify that we try to get changes from Delta Log.
        verify(enumContext).callAsync(
            tableMonitorArgumentCaptor.capture(), any(BiConsumer.class), anyLong(), anyLong());
        verify(deltaLog).getChanges(startingVersion, true);

        // verify TableMonitor starting version
        assertThat(tableMonitorArgumentCaptor.getValue().getMonitorVersion(),
            equalTo(nextMonitoringVersion));
    }

    @Override
    protected SplitEnumeratorProvider getProvider() {
        return this.provider;
    }

    private List<VersionLog> mockEnumContextAndTableChange(long snapshotVersion) {
        Mockito.doAnswer(invocation -> {
            TableMonitor tableMonitor = invocation.getArgument(0, TableMonitor.class);
            tableMonitor.call();
            return new TableMonitorResult(snapshotVersion, Collections.emptyList());
        }).when(enumContext)
            .callAsync(any(Callable.class), any(BiConsumer.class), anyLong(), anyLong());

        AddFile fileOne = mock(AddFile.class);
        AddFile fileTwo = mock(AddFile.class);

        return Arrays.asList(
            new VersionLog(snapshotVersion, singletonList(fileOne)),
            new VersionLog(snapshotVersion + 1, singletonList(fileTwo)));
    }

}
