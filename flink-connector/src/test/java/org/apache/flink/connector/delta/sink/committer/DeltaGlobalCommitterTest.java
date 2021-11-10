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

package org.apache.flink.connector.delta.sink.committer;

import java.io.IOException;

import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.HadoopConfTest;
import org.apache.flink.core.fs.Path;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.delta.standalone.DeltaLog;

/**
 * Tests for {@link DeltaGlobalCommitter}.
 */
// TODO cover all corner cases
public class DeltaGlobalCommitterTest {

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private Path tablePath;
    private DeltaLog deltaLog;

    @Before
    public void setup() throws IOException {
        tablePath = new Path(TEMPORARY_FOLDER.newFolder().toURI());
        deltaLog = DeltaLog.forTable(HadoopConfTest.getHadoopConf(), tablePath.getPath());
    }

    /**
     * Test cases to cover:
     * test commit twice same data (after second trial DeltaLog should have the same version)
     * test with non-matching datastream schema and canTryUpdateSchema set to false
     * test with non-matching datastream schema and canTryUpdateSchema set to true
     * test with different stream's partition values
     * test with committables from different checkpoint intervals (both should pass)
     * test with committables from different checkpoint intervals,
     * one outdated (only one should pass)
     * test with committables from different checkpoint intervals with different schemas
     * test with committables from the same checkpoint interval containg different
     * partition columns (should fail)
     */

    @Test
    public void test() throws Exception {}
}
