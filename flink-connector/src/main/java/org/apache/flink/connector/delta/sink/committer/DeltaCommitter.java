package org.apache.flink.connector.delta.sink.committer;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class DeltaCommitter implements Committer<DeltaCommittable> {


    private final BucketWriter<?, ?> bucketWriter;


    public DeltaCommitter(BucketWriter<?, ?> bucketWriter) {
        this.bucketWriter = checkNotNull(bucketWriter);
    }

    @Override
    public List<DeltaCommittable> commit(List<DeltaCommittable> committables) throws IOException {
        for (DeltaCommittable committable : committables) {
            if (committable.hasDeltaPendingFile()) {
                // We should always use commitAfterRecovery which contains additional checks.
                assert committable.getDeltaPendingFile() != null;
                bucketWriter.recoverPendingFile(committable.getDeltaPendingFile().getPendingFile()).commitAfterRecovery();
            }

            if (committable.hasInProgressFileToCleanup()) {
                bucketWriter.cleanupInProgressFileRecoverable(
                        committable.getInProgressFileToCleanup()
                );
            }
        }

        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {

    }

}
