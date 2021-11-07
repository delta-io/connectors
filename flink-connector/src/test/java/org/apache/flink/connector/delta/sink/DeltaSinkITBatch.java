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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.TestDeltaLakeTable;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.TestFileSystem;
import org.apache.flink.connector.file.sink.BatchExecutionFileSinkITCase;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.CommitInfo;


public class DeltaSinkITBatch extends BatchExecutionFileSinkITCase {

    String deltaTablePath;

    @Before
    public void setup() {
        try {
            deltaTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            TestDeltaLakeTable.initializeTestStateForNonPartitionedDeltaTable(deltaTablePath);
        } catch (IOException e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @Override
    @Test
    public void testFileSink() throws Exception {
        runDeltaSinkTest();
    }

    public void runDeltaSinkTest() throws Exception {
        // GIVEN
        org.apache.hadoop.conf.Configuration conf = getHadoopConf();
        DeltaLog deltaLog = DeltaLog.forTable(conf, deltaTablePath);
        List<AddFile> initialDeltaFiles = deltaLog.snapshot().getAllFiles();
        long initialVersion = deltaLog.snapshot().getVersion();
        assert initialDeltaFiles.size() == 2;

        JobGraph jobGraph = createJobGraph(deltaTablePath);

        // WHEN
        try (MiniCluster miniCluster = getMiniCluster()) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }

        // THEN
        TestFileSystem.validateIfPathContainsParquetFilesWithData(deltaTablePath);

        List<AddFile> finalDeltaFiles = deltaLog.update().getAllFiles();
        assert finalDeltaFiles.size() > initialDeltaFiles.size();
        Iterator<Long> it = LongStream.range(
            initialVersion + 1, deltaLog.snapshot().getVersion() + 1).iterator();
        long totalRowsAdded = 0;
        long totalAddedFiles = 0;
        while (it.hasNext()) {
            long currentVersion = it.next();
            CommitInfo currentCommitInfo = deltaLog.getCommitInfoAt(currentVersion);
            Optional<Map<String, String>> operationMetrics =
                currentCommitInfo.getOperationMetrics();
            assert operationMetrics.isPresent();
            totalRowsAdded += Long.parseLong(operationMetrics.get().get("numOutputRows"));
            totalAddedFiles += Long.parseLong(operationMetrics.get().get("numAddedFiles"));

            assert Integer.parseInt(operationMetrics.get().get("numOutputBytes")) > 0;
        }

        assert totalAddedFiles == finalDeltaFiles.size() - initialDeltaFiles.size();
        assert totalRowsAdded == NUM_RECORDS;
    }

    protected JobGraph createJobGraph(String path) {
        StreamExecutionEnvironment env = getTestStreamEnv();

        env.fromCollection(getTestRows())
            .setParallelism(1)
            .sinkTo(createDeltaSink())
            .setParallelism(NUM_SINKS);

        StreamGraph streamGraph = env.getStreamGraph();
        return streamGraph.getJobGraph();
    }

    private DeltaSink<RowData> createDeltaSink() {
        ParquetWriterFactory<RowData> factory =
            ParquetRowDataBuilder.createWriterFactory(
                TestDeltaLakeTable.TEST_ROW_TYPE, getHadoopConf(), true);

        return DeltaSink
            .forDeltaFormat(
                new Path(deltaTablePath),
                getHadoopConf(),
                factory,
                TestDeltaLakeTable.TEST_ROW_TYPE)
            .build();
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

    private StreamExecutionEnvironment getTestStreamEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        env.configure(config, getClass().getClassLoader());
        return env;
    }


    protected List<RowData> getTestRows() {
        List<RowData> rows = new ArrayList<>(NUM_RECORDS);
        for (int i = 0; i < NUM_RECORDS; i++) {
            Integer v = i;
            rows.add(
                TestDeltaLakeTable.CONVERTER.toInternal(
                    Row.of(
                        String.valueOf(v),
                        String.valueOf((v + v)),
                        v)
                )
            );
        }
        return rows;
    }

}
