package io.delta.standalone;

import java.util.List;

import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;

/**
 * {@link Snapshot} provides APIs to access the Delta table state (such as table metadata, active
 * files) at some version.
 *
 * See <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md">Delta Transaction Log Protocol</a>
 * for more details about the transaction logs.
 */
public interface Snapshot {

    /**
     * @return all of the files present in this snapshot
     */
    List<AddFile> getAllFiles();

    /**
     * @return the table metadata for this snapshot
     */
    Metadata getMetadata();

    /**
     * @return the version for this snapshot
     */
    long getVersion();

    /**
     * Creates a {@link CloseableIterator} which can iterate over data belonging to the version of
     * this snapshot. It provides no iteration ordering guarantee among data.
     *
     * @return a {@link CloseableIterator} to iterate over data
     */
    CloseableIterator<RowRecord> open();
}
