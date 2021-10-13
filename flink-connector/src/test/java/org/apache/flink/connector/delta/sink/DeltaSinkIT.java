/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.delta.sink;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.CommitInfo;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;


public class DeltaSinkIT {

    private static final String[] myIntArray = {"name", "surname", "age"};
    private static final LogicalType[] types = {new VarCharType(VarCharType.MAX_LENGTH),
            new VarCharType(VarCharType.MAX_LENGTH),
            new IntType()};

    private static final RowType ROW_TYPE = RowType.of(types, myIntArray);

    protected static final int NUM_SINKS = 3;

    protected static final int NUM_RECORDS = 1000;


    @Test
    public void testDeltaSink() throws Exception {
        // GIVEN
        String testDeltaTablePath = resetTestDeltaTableFromBkp();
        org.apache.hadoop.conf.Configuration conf = getHadoopConf();
        DeltaLog deltaLog = DeltaLog.forTable(conf, testDeltaTablePath);
        List<AddFile> initialDeltaFiles = deltaLog.snapshot().getAllFiles();
        assert initialDeltaFiles.size() == 2;

        JobGraph jobGraph = createJobGraph(conf, testDeltaTablePath);

        // WHEN
        try (MiniCluster miniCluster = getMiniCluster()) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        } catch (Exception e) {
            throw e;
        }

        // THEN
        List<AddFile> finalDeltaFiles = deltaLog.update().getAllFiles();
        assert finalDeltaFiles.size() > initialDeltaFiles.size();

        CommitInfo commitInfo = deltaLog.getCommitInfoAt(deltaLog.snapshot().getVersion());
        Optional<Map<String, String>> operationMetrics = commitInfo.getOperationMetrics();
        assert operationMetrics.isPresent();
        assert operationMetrics.get().get("numOutputRows").equals(String.valueOf(NUM_RECORDS));
        assert operationMetrics.get().get("numAddedFiles").equals(String.valueOf(finalDeltaFiles.size() - initialDeltaFiles.size()));
        assert Integer.parseInt(operationMetrics.get().get("numOutputBytes")) > 0;
    }

    protected JobGraph createJobGraph(org.apache.hadoop.conf.Configuration conf,
                                      String testDeltaTablePath) throws IOException {

        StreamExecutionEnvironment env = getTestStreamEnv();

        ParquetWriterFactory<RowData> factory = ParquetRowDataBuilder.createWriterFactory(ROW_TYPE, conf, true);

        DeltaSink<RowData> deltaSink = DeltaSink
                .forDeltaFormat(new Path(testDeltaTablePath), conf, factory)
                .build();

        env.fromCollection(getTestRows())
                .setParallelism(1)
                .map((MapFunction<RowData, RowData>) value -> {
                    Thread.sleep(1);
                    return value;
                })
                .setParallelism(NUM_SINKS)
                .sinkTo(deltaSink)
                .setParallelism(NUM_SINKS);

        StreamGraph streamGraph = env.getStreamGraph();
        return streamGraph.getJobGraph();
    }

    private MiniCluster getMiniCluster() {
        final Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "18081-19000");
        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(4)
                        .setConfiguration(config)
                        .build();
        return new MiniCluster(cfg);
    }

    private org.apache.hadoop.conf.Configuration getHadoopConf() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("parquet.compression", "SNAPPY");
        return conf;
    }

    private String resetTestDeltaTableFromBkp() throws IOException {
        String path = getClass().getResource("/test/test-table-4").getPath();
        String pathBkp = getClass().getResource("/test/test-table-4-bkp").getPath();
        FileUtils.deleteDirectory(new File(path));
        FileUtils.copyDirectory(new File(pathBkp), new File(path));
        return path;
    }

    private StreamExecutionEnvironment getTestStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        env.configure(config, getClass().getClassLoader());
        env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
        return env;
    }

    protected List<RowData> getTestRows() {
        List<RowData> rows = new ArrayList<>(NUM_RECORDS);
        for (int i = 0; i < NUM_RECORDS; i++) {
            Integer v = i;
            rows.add(
                    CONVERTER.toInternal(Row.of(String.valueOf(v), String.valueOf((v + v)), v))
            );
        }
        return rows;
    }

    private static final DataFormatConverters.DataFormatConverter<RowData, Row> CONVERTER =
            DataFormatConverters.getConverterForDataType(
                    TypeConversions.fromLogicalToDataType(ROW_TYPE)
            );

}
