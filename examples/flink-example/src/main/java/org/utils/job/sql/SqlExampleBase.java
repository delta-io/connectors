package org.utils.job.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class SqlExampleBase {

    protected abstract Table runSqlJob(
        String tablePath,
        StreamTableEnvironment tableEnv
    ) throws Exception;

    protected StreamExecutionEnvironment createTestStreamEnv(boolean isStreaming) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

        if (isStreaming) {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }

        return env;
    }

    protected StreamTableEnvironment createTableStreamingEnv(boolean isStreaming) {
        return StreamTableEnvironment.create(
            createTestStreamEnv(isStreaming)
        );
    }

    protected StreamTableEnvironment createTableStreamingEnv(StreamExecutionEnvironment env) {
        return StreamTableEnvironment.create(env);
    }
}