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

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittable;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.HadoopConfTest;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.TestDeltaCommittable;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.TestDeltaLakeTable;
import org.apache.flink.core.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DeltaGlobalCommitter}.
 */
@RunWith(Parameterized.class)
public class DeltaGlobalCommitterTestParametrized {

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Parameterized.Parameters
    public static Collection<Object[]> params() {
        return Arrays.asList(
                new Object[]{false, 0, TestDeltaLakeTable.getEmptyTestPartitionSpec(), false},
                new Object[]{false, 0, TestDeltaLakeTable.getTestPartitionSpec(), false},
                new Object[]{false, 1, TestDeltaLakeTable.getEmptyTestPartitionSpec(), true},
                new Object[]{false, 1, TestDeltaLakeTable.getTestPartitionSpec(), true}
        );
    }

    @Parameterized.Parameter(0)
    public boolean canTryUpdateSchema;

    @Parameterized.Parameter(1)
    public int expectedTableVersionAfterCommit;

    @Parameterized.Parameter(2)
    public LinkedHashMap<String, String> partitionSpec;

    @Parameterized.Parameter(3)
    public boolean initializeTableBeforeCommit;

    private Path tablePath;
    private DeltaLog deltaLog;

    @Before
    public void setup() throws IOException {
        tablePath = new Path(TEMPORARY_FOLDER.newFolder().toURI());
        if (initializeTableBeforeCommit) {
            if (partitionSpec.isEmpty()) {
                TestDeltaLakeTable.initializeTestStateForNonPartitionedDeltaTable(tablePath.getPath());
            } else {
                TestDeltaLakeTable.initializeTestStateForPartitionedDeltaTable(tablePath.getPath());
            }
        }
        deltaLog = DeltaLog.forTable(HadoopConfTest.getHadoopConf(), tablePath.getPath());
    }

    @After
    public void teardown() {

    }

    @Test
    public void testCommitToDeltaTableInAppendMode() throws Exception {
        //GIVEN
        List<String> partitionColumns = new ArrayList<>(partitionSpec.keySet());
        DeltaGlobalCommitter globalCommitter = new DeltaGlobalCommitter(HadoopConfTest.getHadoopConf(), tablePath, TestDeltaLakeTable.TEST_ROW_TYPE, canTryUpdateSchema);
        List<DeltaCommittable> deltaCommittables = TestDeltaCommittable.getListOfDeltaCommittables(3, partitionSpec);
        List<DeltaGlobalCommittable> globalCommittables = Collections.singletonList(new DeltaGlobalCommittable(deltaCommittables));

        // WHEN
        globalCommitter.commit(globalCommittables);

        // THEN
        validateCurrentSnapshotState(
                deltaLog,
                expectedTableVersionAfterCommit,
                deltaCommittables.size(),
                partitionColumns,
                initializeTableBeforeCommit
        );
        validateCurrentTableFiles(deltaLog.update(), partitionSpec);
    }

    private static void validateCurrentSnapshotState(DeltaLog deltaLog,
                                                     int expectedTableVersionAfterUpdate,
                                                     int deltaCommittablesSize,
                                                     List<String> partitionColumns,
                                                     boolean initializeTableBeforeCommit) {
        int initialTableFilesCount = 0;
        if (initializeTableBeforeCommit) {
            initialTableFilesCount = deltaLog.snapshot().getAllFiles().size();
        }
        Snapshot snapshot = deltaLog.update();
        assertEquals(snapshot.getVersion(), expectedTableVersionAfterUpdate);
        assertEquals(snapshot.getAllFiles().size(), deltaCommittablesSize + initialTableFilesCount);
        assertEquals(snapshot.getMetadata().getPartitionColumns(), partitionColumns);
    }

    private void validateCurrentTableFiles(Snapshot snapshot,
                                           LinkedHashMap<String, String> partitionSpec) throws URISyntaxException, IOException {
        CloseableIterator<AddFile> filesIterator = snapshot.scan().getFiles();
        while (filesIterator.hasNext()) {
            AddFile addFile = filesIterator.next();
            assertEquals(addFile.getPartitionValues(), partitionSpec);
            assertTrue(addFile.getSize() > 0);
            assert (!addFile.getPath().isEmpty());
        }
    }

}
