package io.delta.flink.e2e.sink;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import static io.delta.flink.e2e.assertions.DeltaLogAssertions.assertThat;

import io.delta.standalone.DeltaLog;

class DeltaSinkBatchExecutionJobE2ETest {

    @Disabled
    @Test
    void xxx() {
        List<StepConfig> stepConfigs = new ArrayList<>();

        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
            .withRegion(Regions.US_WEST_2)
            .build();

        HadoopJarStepConfig jobConf = new HadoopJarStepConfig()
            .withJar("command-runner.jar")
            .withArgs("flink", "run", "-m", "yarn-cluster", "-d", "/home/hadoop/job.jar",
                "--class", "io.delta.flink.e2e.sink.DeltaSinkBatchExecutionJob");

        StepConfig flinkRunWordCount = new StepConfig()
            .withName("TEST")
            .withActionOnFailure("CONTINUE")
            .withHadoopJarStep(jobConf);

        stepConfigs.add(flinkRunWordCount);

        AddJobFlowStepsResult res = emr.addJobFlowSteps(new AddJobFlowStepsRequest()
            .withJobFlowId("j-2C9F0DTBF5XK0")
            .withSteps(stepConfigs));
    }

    @Test
    void shouldAddNewRecords() throws Exception {
        // GIVEN
        String tablePathS3a = "s3a://databricks-performance-benchmarks-data/" +
            "test-partitioned-delta-table-initial-state/";
        // AND
        DeltaLog deltaLog = DeltaLog.forTable(DeltaSinkTestUtils.getHadoopConf(), tablePathS3a);
        long initialDeltaVersion = deltaLog.snapshot().getVersion();
        // AND
        StreamExecutionEnvironment env = getExecutionEnvironment();
        env.getConfig().setExecutionMode(ExecutionMode.BATCH);

        // WHEN
        DeltaSinkBatchExecutionJob.getPipeline(env, tablePathS3a);
        env.execute("[E2E] Sink: add new records test");

        // THEN
        assertThat(deltaLog)
            .sinceVersion(initialDeltaVersion)
            .hasNewRecordsCount(10_000);
    }


    private StreamExecutionEnvironment getExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
            "localhost",
            8081,
            "/home/grzegorz/Projects/grzegorz8/connectors/flink/end-to-end-tests/target/" +
                "scala-2.12/delta-flink-end-to-end-tests-assembly-0.4.0-SNAPSHOT.jar"
        );
        env.getConfig().setExecutionMode(ExecutionMode.BATCH);
        return env;
    }

}
