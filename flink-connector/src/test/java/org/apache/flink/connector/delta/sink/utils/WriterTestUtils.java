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

import java.io.IOException;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.fs.local.LocalRecoverableWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.DeltaBulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.data.RowData;
import static org.junit.Assert.fail;

public class WriterTestUtils {

    public static final OnCheckpointRollingPolicy<RowData, String> ON_CHECKPOINT_ROLLING_POLICY =
        OnCheckpointRollingPolicy.build();

    public static LocalRecoverableWriter getRecoverableWriter(Path path) {
        try {
            final FileSystem fs = FileSystem.get(path.toUri());
            if (!(fs instanceof LocalFileSystem)) {
                fail(
                    "Expected Local FS but got a "
                        + fs.getClass().getName()
                        + " for path: "
                        + path);
            }
            return new LocalRecoverableWriter((LocalFileSystem) fs);
        } catch (IOException e) {
            fail();
        }
        return null;
    }

    public static DeltaBulkBucketWriter<RowData, String> createBucketWriter(Path path)
        throws IOException {
        return new DeltaBulkBucketWriter<>(
            getRecoverableWriter(path),
            DeltaSinkTestUtils.TestParquetWriterFactory.createTestWriterFactory());
    }
}
