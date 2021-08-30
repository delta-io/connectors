package io.delta.standalone.actions;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class RemoveFile implements FileAction {
    private final String path;
    private final Optional<Long> deletionTimestamp;
    private final boolean dataChange;
    private final boolean extendedFileMetadata;
    private final Map<String, String> partitionValues;
    private final long size;
    private final Map<String, String> tags;

    public RemoveFile(String path, Optional<Long> deletionTimestamp, boolean dataChange,
                      boolean extendedFileMetadata, Map<String, String> partitionValues, long size,
                      Map<String, String> tags) {
        this.path = path;
        this.deletionTimestamp = deletionTimestamp;
        this.dataChange = dataChange;
        this.extendedFileMetadata = extendedFileMetadata;
        this.partitionValues = partitionValues;
        this.size = size;
        this.tags = tags;
    }

    public RemoveFile(RemoveFileBuilder builder) {
        this.path = builder.path;
        this.deletionTimestamp = builder.deletionTimestamp;
        this.dataChange = builder.dataChange;
        this.extendedFileMetadata = builder.extendedFileMetadata;
        this.partitionValues = builder.partitionValues;
        this.size = builder.size;
        this.tags = builder.tags;
    }

    @Override
    public String getPath() {
        return path;
    }

    public Optional<Long> getDeletionTimestamp() {
        return deletionTimestamp;
    }

    @Override
    public boolean isDataChange() {
        return dataChange;
    }

    public boolean isExtendedFileMetadata() {
        return extendedFileMetadata;
    }

    public Map<String, String> getPartitionValues() {
        return Collections.unmodifiableMap(partitionValues);
    }

    public long getSize() {
        return size;
    }

    public Map<String, String> getTags() {
        return Collections.unmodifiableMap(tags);
    }

    /**
     * Builder class for RemoveFile. Enables construction of RemoveFile object with default values.
     */
    public static class RemoveFileBuilder {
        private final String path;
        private final Optional<Long> deletionTimestamp;
        private boolean dataChange = true;
        private boolean extendedFileMetadata = false;
        private Map<String, String> partitionValues;
        private long size = 0;
        private Map<String, String> tags;

        public RemoveFileBuilder(String path, Optional<Long> deletionTimestamp) {
            this.path = path;
            this.deletionTimestamp = deletionTimestamp;
        }

        public RemoveFileBuilder dataChange(boolean dataChange) {
            this.dataChange = dataChange;
            return this;
        }

        public RemoveFileBuilder extendedFileMetadata(boolean extendedFileMetadata) {
            this.extendedFileMetadata = extendedFileMetadata;
            return this;
        }

        public RemoveFileBuilder partitionValues(Map<String, String> partitionValues) {
            this.partitionValues = partitionValues;
            return this;
        }

        public RemoveFileBuilder size(long size) {
            this.size = size;
            return this;
        }

        public RemoveFileBuilder tags(Map<String, String> tags) {
            this.tags = tags;
            return this;
        }

        public RemoveFile build() {
            RemoveFile removeFile = new RemoveFile(this);
            return removeFile;
        }
    }
}
