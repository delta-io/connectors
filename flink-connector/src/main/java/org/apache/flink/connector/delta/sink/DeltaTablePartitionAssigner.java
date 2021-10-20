package org.apache.flink.connector.delta.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.table.utils.PartitionPathUtils;

import java.io.Serializable;
import java.util.LinkedHashMap;

public class DeltaTablePartitionAssigner<T> implements BucketAssigner<T, String> {

    private static final long serialVersionUID = -6033643154550226022L;

    private final DeltaPartitionComputer<T> partitionComputer;

    public DeltaTablePartitionAssigner(DeltaPartitionComputer<T> partitionComputer) {
        this.partitionComputer = partitionComputer;
    }

    @Override
    public String getBucketId(T element, BucketAssigner.Context context) {
        LinkedHashMap<String, String> partitionValues = this.partitionComputer.generatePartitionValues(element, context);
        return PartitionPathUtils.generatePartitionPath(partitionValues);
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }

    @Override
    public String toString() {
        return "TablePartitionAssigner";
    }

    public interface DeltaPartitionComputer<T> extends Serializable {

        /**
         * Compute partition values from record.
         *
         * @param element input record.
         * @return partition values.
         */
        LinkedHashMap<String, String> generatePartitionValues(T element, BucketAssigner.Context context);
    }

}