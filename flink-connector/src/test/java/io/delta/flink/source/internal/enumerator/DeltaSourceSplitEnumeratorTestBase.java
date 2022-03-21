package io.delta.flink.source.internal.enumerator;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.file.AddFileEnumerator.SplitFilter;
import io.delta.flink.source.internal.file.AddFileEnumeratorContext;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import static io.delta.flink.source.internal.enumerator.SourceSplitEnumeratorTestUtils.mockFileEnumerator;
import static io.delta.flink.source.internal.enumerator.SourceSplitEnumeratorTestUtils.mockSplits;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;

/**
 * A base test class that covers common logic for both {@link BoundedDeltaSourceSplitEnumerator} and
 * {@link ContinuousDeltaSourceSplitEnumerator}. Tests here have same setup and assertions for both
 * {@code SourceSplitEnumerator} implementations.
 *
 * @implNote The child class has to implement the {@link #createEnumerator()} method, which returns
 * concrete {@link DeltaSourceSplitEnumerator} implementation.
 */
public abstract class DeltaSourceSplitEnumeratorTestBase {

    protected static final String TEST_PATH = "/some/path/file.txt";

    @Mock
    protected Path deltaTablePath;

    @Mock
    protected AddFileEnumerator<DeltaSourceSplit> fileEnumerator;

    @Mock
    protected AddFileEnumerator.Provider<DeltaSourceSplit> fileEnumeratorProvider;

    @Mock
    protected FileSplitAssigner.Provider splitAssignerProvider;

    @Mock
    protected FileSplitAssigner splitAssigner;

    @Mock
    protected SplitEnumeratorContext<DeltaSourceSplit> enumContext;

    @Mock
    protected DeltaLog deltaLog;

    @Mock
    protected Snapshot headSnapshot;

    @Mock
    protected Snapshot checkpointedSnapshot;

    @Mock
    protected ReaderInfo readerInfo;
    protected MockedStatic<SourceUtils> sourceUtils;
    protected MockedStatic<DeltaLog> deltaLogStatic;
    protected DeltaSourceConfiguration sourceConfiguration;
    @Mock
    private DeltaSourceSplit split;
    @Captor
    private ArgumentCaptor<List<FileSourceSplit>> splitsCaptor;
    private DeltaSourceSplitEnumerator enumerator;

    protected void setUp() {
        sourceConfiguration = new DeltaSourceConfiguration();
        deltaLogStatic = Mockito.mockStatic(DeltaLog.class);
        deltaLogStatic.when(() -> DeltaLog.forTable(any(Configuration.class), anyString()))
            .thenReturn(this.deltaLog);

        sourceUtils = Mockito.mockStatic(SourceUtils.class);
        sourceUtils.when(() -> SourceUtils.pathToString(deltaTablePath))
            .thenReturn(TEST_PATH);
    }

    protected void after() {
        sourceUtils.close();
        deltaLogStatic.close();
    }

    @Test
    public void shouldHandleFailedReader() {
        enumerator = setupEnumeratorWithHeadSnapshot();

        // Mock reader failure.
        when(enumContext.registeredReaders()).thenReturn(Collections.emptyMap());

        int subtaskId = 1;
        enumerator.handleSplitRequest(subtaskId, "testHost");
        verify(enumContext, never()).assignSplit(any(DeltaSourceSplit.class), anyInt());
    }

    @Test
    public void shouldAssignSplitToReader() {
        int subtaskId = 1;
        enumerator = setupEnumeratorWithHeadSnapshot();

        when(enumContext.registeredReaders()).thenReturn(
            Collections.singletonMap(subtaskId, readerInfo));

        String host = "testHost";
        when(splitAssigner.getNext(host)).thenReturn(Optional.of(split))
            .thenReturn(Optional.empty());

        // handle request split when there is a split to assign
        enumerator.handleSplitRequest(subtaskId, host);
        verify(enumContext).assignSplit(split, subtaskId);
        verify(enumContext, never()).signalNoMoreSplits(anyInt());

        // check that we clear split from enumerator after assigning them.
        enumerator.handleSplitRequest(subtaskId, host);
        verify(enumContext).assignSplit(split, subtaskId); // the one from previous assignment.
        verify(enumerator).handleNoMoreSplits(subtaskId);
    }

    @Test
    public void shouldAddSplitBack() {
        int subtaskId = 1;
        enumerator = setupEnumeratorWithHeadSnapshot();

        when(enumContext.registeredReaders()).thenReturn(
            Collections.singletonMap(subtaskId, readerInfo));

        String testHost = "testHost";
        enumerator.handleSplitRequest(subtaskId, testHost);
        verify(enumerator).handleNoMoreSplits(subtaskId);

        enumerator.addSplitsBack(Collections.singletonList(split), subtaskId);

        //capture the assigned split to mock assigner and use it in getNext mock
        verify(splitAssigner).addSplits(splitsCaptor.capture());

        when(splitAssigner.getNext(testHost)).thenReturn(
            Optional.ofNullable(splitsCaptor.getValue().get(0)));
        enumerator.handleSplitRequest(subtaskId, testHost);
        verify(enumContext).assignSplit(split, subtaskId);
    }

    @Test
    public void shouldReadInitialSnapshot() {

        enumerator = setupEnumeratorWithHeadSnapshot();

        List<DeltaSourceSplit> mockSplits = mockSplits();
        when(fileEnumerator.enumerateSplits(any(AddFileEnumeratorContext.class),
            any(SplitFilter.class)))
            .thenReturn(mockSplits);

        enumerator.start();

        verify(splitAssigner).addSplits(splitsCaptor.capture());
        assertThat(splitsCaptor.getValue(), equalTo(mockSplits));
    }

    @Test
    public void shouldNotProcessAlreadyProcessedPaths() {
        enumerator = setupEnumeratorWithHeadSnapshot();

        AddFile mockAddFile = mock(AddFile.class);
        when(mockAddFile.getPath()).thenReturn("add/file/path.parquet");
        when(headSnapshot.getAllFiles()).thenReturn(Collections.singletonList(mockAddFile));

        mockFileEnumerator(fileEnumerator);

        enumerator.start();

        verify(splitAssigner).addSplits(splitsCaptor.capture());
        assertThat(splitsCaptor.getValue().size(), equalTo(1));

        // Reprocess the same data again
        enumerator.start();

        verify(splitAssigner, times(2)).addSplits(splitsCaptor.capture());
        assertThat(splitsCaptor.getValue().isEmpty(), equalTo(true));
    }

    @SuppressWarnings("unchecked")
    protected <T> T setupEnumeratorWithHeadSnapshot() {
        when(deltaLog.snapshot()).thenReturn(headSnapshot);
        return (T) spy(createEnumerator());
    }

    @SuppressWarnings("unchecked")
    protected <T> T setupEnumeratorFromCheckpoint(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint) {
        return (T) spy(createEnumerator(checkpoint));
    }

    protected abstract DeltaSourceSplitEnumerator createEnumerator();

    protected abstract DeltaSourceSplitEnumerator createEnumerator(
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint);
}
