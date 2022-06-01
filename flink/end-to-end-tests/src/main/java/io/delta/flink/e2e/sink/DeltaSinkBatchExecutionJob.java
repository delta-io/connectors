package io.delta.flink.e2e.sink;

import io.delta.flink.sink.internal.DeltaSinkInternal;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import static io.delta.flink.e2e.sink.DeltaSinkTestUtils.createDeltaSink;

public class DeltaSinkBatchExecutionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String tablePathS3 = "s3a://databricks-performance-benchmarks-data/" +
            "test-partitioned-delta-table-initial-state/";
        getPipeline(env, tablePathS3);
        env.execute(DeltaSinkBatchExecutionJob.class.getName());
    }

    public static void getPipeline(StreamExecutionEnvironment env, String deltaTablePath) {
        DeltaSinkInternal<RowData> deltaSink = createDeltaSink(
            deltaTablePath,
            false // isTablePartitioned
        );

        env.fromCollection(DeltaSinkTestUtils.getTestRowData(10000))
            .setParallelism(1)
            .sinkTo(deltaSink)
            .setParallelism(3);
    }
}
