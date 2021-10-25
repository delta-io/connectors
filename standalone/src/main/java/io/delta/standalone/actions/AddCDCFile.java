// TODO: copyright

package io.delta.standalone.actions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

/**
 * A change file containing CDC data for the Delta version it's within. Non-CDC readers should
 * ignore this, CDC readers should scan all ChangeFiles in a version rather than computing
 * changes from AddFile and RemoveFile actions.
 */
public final class AddCDCFile implements FileAction {
    @Nonnull
    private final String path;

    @Nonnull
    private final Map<String, String> partitionValues;

    private final long size;

    @Nullable
    private final Map<String, String> tags;

    public AddCDCFile(@Nonnull String path, @Nonnull Map<String, String> partitionValues, long size,
                      @Nullable Map<String, String> tags) {
        this.path = path;
        this.partitionValues = partitionValues;
        this.size = size;
        this.tags = tags;
    }

    /**
     * @return the relative path or the absolute path that should be added to the table. If it's a
     *         relative path, it's relative to the root of the table. Note: the path is encoded and
     *         should be decoded by {@code new java.net.URI(path)} when using it.
     */
    @Override
    @Nonnull
    public String getPath() {
        return path;
    }

    /**
     * @return an unmodifiable {@code Map} from partition column to value for
     *         this file. Partition values are stored as strings, using the following formats.
     *         An empty string for any type translates to a null partition value.
     * @see <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Partition-Value-Serialization" target="_blank">Delta Protocol Partition Value Serialization</a>
     */
    @Nonnull
    public Map<String, String> getPartitionValues() {
        return Collections.unmodifiableMap(partitionValues);
    }

    /**
     * @return the size of this file in bytes
     */
    public long getSize() {
        return size;
    }

    /**
     * @return an unmodifiable {@code Map} containing metadata about this file
     */
    @Nullable
    public Map<String, String> getTags() {
        return tags != null ? Collections.unmodifiableMap(tags) : null;
    }

    @Override
    public boolean isDataChange() {
        return false;
    }
}
