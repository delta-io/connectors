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
import org.apache.flink.connector.delta.sink.SchemaConverter;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittable;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.HadoopConfTest;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.TestDeltaCommittable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DeltaCommitter}.
 */
// TODO refactor this test for more code reuse and DRY
// TODO cover all corner cases
public class DeltaGlobalCommitterTest {

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    // TODO move setup for initial test Delta table to the TestUtils class
    private static final RowType ROW_TYPE = new RowType(Arrays.asList(
            new RowType.RowField("name", new VarCharType(VarCharType.MAX_LENGTH)),
            new RowType.RowField("surname", new VarCharType(VarCharType.MAX_LENGTH)),
            new RowType.RowField("age", new IntType())
    ));

    private Path tablePath;
    private DeltaLog deltaLog;

    @Before
    public void setup() throws IOException {
        tablePath = new Path(TEMPORARY_FOLDER.newFolder().toURI());
        deltaLog = DeltaLog.forTable(HadoopConfTest.getHadoopConf(), tablePath.getPath());
    }

    @After
    public void teardown() {

    }

    @Test
    public void testCommitToNewDeltaTableInAppendMode() throws Exception {
        //GIVEN
        LinkedHashMap<String, String> emptyPartitionSpec = new LinkedHashMap<>();
        DeltaGlobalCommitter globalCommitter = new DeltaGlobalCommitter(HadoopConfTest.getHadoopConf(), tablePath, ROW_TYPE, false);
        List<DeltaCommittable> deltaCommittables = Arrays.asList(
                TestDeltaCommittable.getTestDeltaCommittableWithPendingFile(emptyPartitionSpec),
                TestDeltaCommittable.getTestDeltaCommittableWithPendingFile(emptyPartitionSpec),
                TestDeltaCommittable.getTestDeltaCommittableWithPendingFile(emptyPartitionSpec)
        );
        List<DeltaGlobalCommittable> globalCommittables = Arrays.asList(
                new DeltaGlobalCommittable(deltaCommittables)
        );

        // WHEN
        globalCommitter.commit(globalCommittables);

        // THEN
        assertEquals(deltaLog.snapshot().getVersion(), -1);
        Snapshot snapshot = deltaLog.update();
        assertEquals(snapshot.getVersion(), 0);
        assertEquals(snapshot.getAllFiles().size(), deltaCommittables.size());
        assertEquals(snapshot.getMetadata().getSchema().toJson(), new SchemaConverter().toDeltaFormat(ROW_TYPE).toJson());
        assertTrue(snapshot.getMetadata().getPartitionColumns().isEmpty());
        CloseableIterator<AddFile> filesIterator = snapshot.scan().getFiles();
        while (filesIterator.hasNext()) {
            AddFile addFile = filesIterator.next();
            assertTrue(addFile.getPartitionValues().isEmpty());
            assertTrue(addFile.getSize() > 0);
            assert (!addFile.getPath().isEmpty());
        }

    }

    @Test
    public void testCommitToNewDeltaTableInAppendModeWithPartitionColumns() throws Exception {
        //GIVEN
        LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<String, String>() {{
            put("a", "b");
            put("c", "d");
        }};
        DeltaGlobalCommitter globalCommitter = new DeltaGlobalCommitter(HadoopConfTest.getHadoopConf(), tablePath, ROW_TYPE, false);
        List<DeltaCommittable> deltaCommittables = Arrays.asList(
                TestDeltaCommittable.getTestDeltaCommittableWithPendingFile(partitionSpec),
                TestDeltaCommittable.getTestDeltaCommittableWithPendingFile(partitionSpec),
                TestDeltaCommittable.getTestDeltaCommittableWithPendingFile(partitionSpec)
        );
        List<DeltaGlobalCommittable> globalCommittables = Arrays.asList(
                new DeltaGlobalCommittable(deltaCommittables)
        );

        // WHEN
        globalCommitter.commit(globalCommittables);

        // THEN
        assertEquals(deltaLog.snapshot().getVersion(), -1);
        Snapshot snapshot = deltaLog.update();
        assertEquals(snapshot.getVersion(), 0);
        assertEquals(snapshot.getAllFiles().size(), deltaCommittables.size());
        assertEquals(snapshot.getMetadata().getPartitionColumns(), Arrays.asList("a", "c"));
        CloseableIterator<AddFile> filesIterator = snapshot.scan().getFiles();
        while (filesIterator.hasNext()) {
            AddFile addFile = filesIterator.next();
            assertEquals(addFile.getPartitionValues(), partitionSpec);
            assertTrue(addFile.getSize() > 0);
            assert (!addFile.getPath().isEmpty());
        }

    }

}
