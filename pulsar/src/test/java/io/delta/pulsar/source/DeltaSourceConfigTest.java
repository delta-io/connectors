/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.delta.pulsar.source;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;


/**
 * Test config.
 */
public class DeltaSourceConfigTest {

    @Test
    public void testLoadConfig() {
        Map<String, Object> map = new HashMap<>();
        map.put("startSnapshotVersion", 1);
        map.put("fetchHistoryData", true);
        map.put("tablePath", "/tmp/test.conf");
        map.put("fileSystemType", "filesystem");
        map.put("parquetParseParallelism", 3);
        map.put("maxReadBytesSizeOneRound", 1024 * 1024);
        map.put("maxReadRowCountOneRound", 1000);
        map.put("checkpointInterval", 30);

        try {
            DeltaSourceConfig config = DeltaSourceConfig.load(map);
            assertEquals(1, config.getStartSnapshotVersion().longValue());
            assertTrue(config.getFetchHistoryData());
            assertEquals("/tmp/test.conf", config.getTablePath());
            assertEquals("filesystem", config.getFileSystemType());
            assertEquals(3, config.getParquetParseParallelism());
            assertEquals(1024 * 1024, config.getMaxReadBytesSizeOneRound());
            assertEquals(1000, config.getMaxReadRowCountOneRound());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testRequiredField() {
        Map<String, Object> map = new HashMap<>();
        map.put("startSnapshotVersion", 1);
        map.put("fetchHistoryData", true);

        try {
            DeltaSourceConfig config = DeltaSourceConfig.load(map);
            config.validate();
            fail();
        } catch (Exception e) {
            assertEquals("tablePath: null or fileSystemType: null should be set.", e.getMessage());
        }
    }

    @Test
    public void testDefaultValue() {
        Map<String, Object> map = new HashMap<>();
        map.put("tablePath", "/tmp/test.conf");
        map.put("fileSystemType", "filesystem");

        try {
            DeltaSourceConfig config = DeltaSourceConfig.load(map);
            config.validate();

            assertEquals(DeltaSourceConfig.LATEST, config.getStartSnapshotVersion().longValue());
            assertFalse(config.getFetchHistoryData());
            assertEquals("/tmp/test.conf", config.getTablePath());
            assertEquals("filesystem", config.getFileSystemType());
            assertEquals(Runtime.getRuntime().availableProcessors(),
                config.getParquetParseParallelism());
            assertEquals(DeltaSourceConfig.DEFAULT_MAX_READ_BYTES_SIZE_ONE_ROUND,
                config.getMaxReadBytesSizeOneRound());
            assertEquals(DeltaSourceConfig.DEFAULT_MAX_READ_ROW_COUNT_ONE_ROUND,
                config.getMaxReadRowCountOneRound());
            assertEquals(DeltaSourceConfig.DEFAULT_CHECKPOINT_INTERVAL,
                config.getCheckpointInterval());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testValidateStartSnapshotVersion() {
        Map<String, Object> map = new HashMap<>();
        map.put("tablePath", "/tmp/test.conf");
        map.put("fileSystemType", "filesystem");
        map.put("startSnapshotVersion", 1);
        map.put("startTimestamp", 11111);

        try {
            DeltaSourceConfig config = DeltaSourceConfig.load(map);
            config.validate();
            fail();
        } catch (Exception e) {
            assertEquals("startSnapshotVersion and startTimeStamp "
                    + "should not be set at the same time.",
                e.getMessage());
        }
    }

    @Test
    public void testInvalidFileSystemType() {
        Map<String, Object> map = new HashMap<>();
        map.put("tablePath", "/tmp/test.conf");
        map.put("fileSystemType", "gcs");

        try {
            DeltaSourceConfig config = DeltaSourceConfig.load(map);
            config.validate();
            fail();
        } catch (Exception e) {
            assertEquals("fileSystemType: gcs is not support yet. "
                    + "Current support type: fileSystem and s3",
                e.getMessage());
        }
    }

    @Test
    public void testInvalidS3Parameters() {
        Map<String, Object> map = new HashMap<>();
        map.put("tablePath", "/tmp/test.conf");
        map.put("fileSystemType", "s3");

        try {
            DeltaSourceConfig config = DeltaSourceConfig.load(map);
            config.validate();
            fail();
        } catch (Exception e) {
            assertEquals("s3aAccesskey or s3aSecretkey should be configured for s3",
                e.getMessage());
        }
    }

    @Test
    public void testInvalidParquetParseParallelism() {
        Map<String, Object> map = new HashMap<>();
        map.put("tablePath", "/tmp/test.conf");
        map.put("fileSystemType", "filesystem");
        map.put("parquetParseParallelism", 1024);

        try {
            DeltaSourceConfig config = DeltaSourceConfig.load(map);
            config.validate();
            assertEquals(DeltaSourceConfig.DEFAULT_PARQUET_PARSE_PARALLELISM,
                config.getParquetParseParallelism());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testInvalidMaxReadBytesSizeOneRound() {
        Map<String, Object> map = new HashMap<>();
        map.put("tablePath", "/tmp/test.conf");
        map.put("fileSystemType", "filesystem");
        map.put("maxReadBytesSizeOneRound", -1);

        try {
            DeltaSourceConfig config = DeltaSourceConfig.load(map);
            config.validate();
            assertEquals(DeltaSourceConfig.DEFAULT_MAX_READ_BYTES_SIZE_ONE_ROUND,
                config.getMaxReadBytesSizeOneRound());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testInvalidSourceConnectorQueueSize() {
        Map<String, Object> map = new HashMap<>();
        map.put("tablePath", "/tmp/test.conf");
        map.put("fileSystemType", "filesystem");
        map.put("sourceConnectorQueueSize", -1);

        try {
            DeltaSourceConfig config = DeltaSourceConfig.load(map);
            config.validate();
            assertEquals(DeltaSourceConfig.DEFAULT_SOURCE_CONNECTOR_QUEUE_SIZE,
                config.getSourceConnectorQueueSize());
        } catch (Exception e) {
            fail();
        }
    }
}
