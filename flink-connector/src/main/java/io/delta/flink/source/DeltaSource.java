package io.delta.flink.source;

import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.enumerator.SplitEnumeratorProvider;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaPendingSplitsCheckpointSerializer;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.state.DeltaSourceSplitSerializer;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.file.src.impl.FileSourceReader;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import static org.apache.flink.util.Preconditions.checkNotNull;

import io.delta.standalone.actions.AddFile;

/**
 * A unified data source that reads Delta Table - both in batch and in streaming mode.
 *
 * <p>This source supports all (distributed) file systems and object stores that can be accessed
 * via the Flink's {@link FileSystem} class.
 *
 * </p>
 *
 * @param <T> The type of the events/records produced by this source.
 * @implNote <h2>Batch and Streaming</h2>
 *
 * <p>This source supports both bounded/batch and continuous/streaming modes. For the
 * bounded/batch case, the Delta Source processes all {@link AddFile} from Delta Table Snapshot. In
 * the continuous/streaming case, the source periodically checks the Delta Table for any appending
 * changes and reads them.
 *
 * <h2>Format Types</h2>
 *
 * <p>The reading of each file happens through file readers defined by <i>file format</i>. These
 * define the parsing logic for the contents of the underlying Parquet files.
 *
 * <p>A {@link BulkFormat} reads batches of records from a file at a time.
 * @implNote <h2>Discovering / Enumerating Files</h2>
 * <p>The way that the source lists the files to be processes is defined by the {@code
 * AddFileEnumerator}. The {@code AddFileEnumerator} is responsible to select the relevant {@code
 * AddFile} and to optionally splits files into multiple regions (= file source splits) that can be
 * read in parallel.
 */
// TODO: include basic bounded + continuous creation example (when DeltaSourceBuilder.java API is
//  finalized).
// TODO: Add Marker interfaces for internal classes to hide them in public methods.
public final class DeltaSource<T>
    implements Source<T, DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>,
    ResultTypeQueryable<T> {

    // ---------------------------------------------------------------------------------------------
    // ALL NON TRANSIENT FIELDS HAVE TO BE SERIALIZABLE
    // ---------------------------------------------------------------------------------------------
    private static final long serialVersionUID = 1L;

    private final Path tablePath;

    private final BulkFormat<T, DeltaSourceSplit> readerFormat;

    private final SplitEnumeratorProvider splitEnumeratorProvider;

    private final SerializableConfiguration serializableConf;

    private final DeltaSourceOptions sourceOptions;

    // ---------------------------------------------------------------------------------------------

    DeltaSource(Path tablePath, BulkFormat<T, DeltaSourceSplit> readerFormat,
        SplitEnumeratorProvider splitEnumeratorProvider, Configuration configuration,
        DeltaSourceOptions sourceOptions) {

        this.tablePath = tablePath;
        this.readerFormat = readerFormat;
        this.splitEnumeratorProvider = splitEnumeratorProvider;
        this.serializableConf = new SerializableConfiguration(configuration);
        this.sourceOptions = sourceOptions;
    }

    /**
     * Builds a new {@code DeltaSource} using a {@link BulkFormat} to read batches of records from
     * files.
     *
     * <p>Examples for bulk readers are compressed and vectorized formats such as ORC or Parquet.
     */
    static <T> DeltaSource<T> forBulkFileFormat(Path deltaTablePath,
        BulkFormat<T, DeltaSourceSplit> reader, SplitEnumeratorProvider splitEnumeratorProvider,
        Configuration configuration, DeltaSourceOptions sourceOptions) {
        checkNotNull(deltaTablePath, "deltaTablePath");
        checkNotNull(reader, "reader");
        checkNotNull(splitEnumeratorProvider, "splitEnumeratorProvider");

        return new DeltaSource<>(deltaTablePath, reader, splitEnumeratorProvider, configuration,
            sourceOptions);
    }

    @Override
    public SimpleVersionedSerializer<DeltaSourceSplit> getSplitSerializer() {
        return DeltaSourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        getEnumeratorCheckpointSerializer() {
        return new DeltaPendingSplitsCheckpointSerializer<>(DeltaSourceSplitSerializer.INSTANCE);
    }

    @Override
    public Boundedness getBoundedness() {
        return splitEnumeratorProvider.getBoundedness();
    }

    @Override
    public SourceReader<T, DeltaSourceSplit> createReader(SourceReaderContext readerContext)
        throws Exception {
        return new FileSourceReader<>(readerContext, readerFormat,
            readerContext.getConfiguration());
    }

    @Override
    public SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        createEnumerator(
        SplitEnumeratorContext<DeltaSourceSplit> enumContext) {
        return splitEnumeratorProvider.createEnumerator(tablePath, serializableConf.conf(),
            enumContext, sourceOptions);
    }

    @Override
    public SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorStateCheckpoint<DeltaSourceSplit>>
        restoreEnumerator(SplitEnumeratorContext<DeltaSourceSplit> enumContext,
        DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> checkpoint) throws Exception {

        return splitEnumeratorProvider.createEnumerator(
            checkpoint, serializableConf.conf(), enumContext, sourceOptions);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return readerFormat.getProducedType();
    }

    @VisibleForTesting
    Path getTablePath() {
        return tablePath;
    }
}
