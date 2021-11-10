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

package org.apache.flink.connector.delta.sink.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.delta.sink.DeltaSink;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.TestRowData;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.table.data.RowData;
import static org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.HadoopConfTest;

public class ITCaseUtils {

    public static DeltaSink<RowData> createDeltaSink(String deltaTablePath) {
        ParquetWriterFactory<RowData> factory =
            DeltaSinkTestUtils.TestParquetWriterFactory.createTestWriterFactory();

        return DeltaSink
            .forDeltaFormat(
                new Path(deltaTablePath),
                HadoopConfTest.getHadoopConf(),
                factory,
                TestRowData.TEST_ROW_TYPE)
            .build();
    }

    public static MiniCluster getMiniCluster() {
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
}
