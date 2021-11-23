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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.flink.connector.delta.sink.SchemaConverter;
import org.apache.flink.connector.delta.sink.committables.DeltaCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittable;
import org.apache.flink.connector.delta.sink.committables.DeltaGlobalCommittableSerializer;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.delta.standalone.DeltaLog;

/**
 * Tests for {@link DeltaGlobalCommitter}.
 */
// TODO cover all corner cases
public class DeltaGlobalCommitterTest {

    private final String TEST_APP_ID = UUID.randomUUID().toString();

    private final long TEST_CHECKPOINT_ID = new Random().nextInt(10);

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private Path tablePath;

    @Before
    public void setup() throws IOException {
        tablePath = new Path(TEMPORARY_FOLDER.newFolder().toURI());

    }

    @Test
    public void testCommitTwice() throws Exception {
        //GIVEN
        int numAddedFiles = 3;
        DeltaSinkTestUtils.initializeTestStateForPartitionedDeltaTable(tablePath.getPath());
        DeltaLog deltaLog = DeltaLog.forTable(
            DeltaSinkTestUtils.getHadoopConf(), tablePath.getPath());
        assertEquals(deltaLog.snapshot().getVersion(), 0);
        int initialTableFilesCount = deltaLog.snapshot().getAllFiles().size();

        List<DeltaCommittable> deltaCommittables = DeltaSinkTestUtils.getListOfDeltaCommittables(
            numAddedFiles, DeltaSinkTestUtils.getTestPartitionSpec());
        List<DeltaGlobalCommittable> globalCommittables =
            Collections.singletonList(new DeltaGlobalCommittable(deltaCommittables));

        DeltaGlobalCommitter globalCommitter = new DeltaGlobalCommitter(
            DeltaSinkTestUtils.getHadoopConf(),
            tablePath,
            DeltaSinkTestUtils.TEST_ROW_TYPE,
            false // canTryUpdateSchema
        );

        // WHEN
        globalCommitter.commit(globalCommittables);
        deltaLog.update();
        assertEquals(deltaLog.snapshot().getVersion(), 1);
        globalCommitter.commit(globalCommittables);

        // THEN
        // after trying to commit same committables nothing should change in DeltaLog
        deltaLog.update();
        assertEquals(1, deltaLog.snapshot().getVersion());
        assertEquals(
            initialTableFilesCount + numAddedFiles,
            deltaLog.snapshot().getAllFiles().size());
    }

    @Test
    public void testCanTryUpdateSchemaSetToTrue() throws Exception {
        //GIVEN
        DeltaSinkTestUtils.initializeTestStateForPartitionedDeltaTable(tablePath.getPath());
        DeltaLog deltaLog = DeltaLog.forTable(
            DeltaSinkTestUtils.getHadoopConf(), tablePath.getPath());

        List<DeltaCommittable> deltaCommittables = DeltaSinkTestUtils.getListOfDeltaCommittables(
            3, DeltaSinkTestUtils.getTestPartitionSpec());
        List<DeltaGlobalCommittable> globalCommittables =
            Collections.singletonList(new DeltaGlobalCommittable(deltaCommittables));

        List<RowType.RowField> fields = new ArrayList<>(
            DeltaSinkTestUtils.TEST_ROW_TYPE.getFields());
        fields.add(new RowType.RowField("someNewField", new IntType()));
        RowType updatedSchema = new RowType(fields);

        DeltaGlobalCommitter globalCommitter = new DeltaGlobalCommitter(
            DeltaSinkTestUtils.getHadoopConf(),
            tablePath,
            updatedSchema,
            true // canTryUpdateSchema
        );

        // WHEN
        globalCommitter.commit(globalCommittables);

        // THEN
        // schema before deltaLog.update() is in old format, but after update it equals to the new
        // format
        assertEquals(deltaLog.snapshot().getMetadata().getSchema().toJson(),
            new SchemaConverter().toDeltaFormat(DeltaSinkTestUtils.TEST_ROW_TYPE).toJson());
        deltaLog.update();
        assertEquals(deltaLog.snapshot().getMetadata().getSchema().toJson(),
            new SchemaConverter().toDeltaFormat(updatedSchema).toJson());
    }

    @Test(expected = RuntimeException.class)
    public void testCanTryUpdateSchemaSetToFalse() throws Exception {
        //GIVEN
        DeltaSinkTestUtils.initializeTestStateForPartitionedDeltaTable(tablePath.getPath());

        List<DeltaCommittable> deltaCommittables = DeltaSinkTestUtils.getListOfDeltaCommittables(
            3, DeltaSinkTestUtils.getTestPartitionSpec());
        List<DeltaGlobalCommittable> globalCommittables =
            Collections.singletonList(new DeltaGlobalCommittable(deltaCommittables));

        // new schema drops one of the previous columns
        List<RowType.RowField> fields = new ArrayList<>(
            DeltaSinkTestUtils.TEST_ROW_TYPE
                .getFields()
                .subList(0, DeltaSinkTestUtils.TEST_ROW_TYPE.getFields().size() - 2)
        );
        fields.add(new RowType.RowField("someNewField", new IntType()));
        RowType updatedSchema = new RowType(fields);

        DeltaGlobalCommitter globalCommitter = new DeltaGlobalCommitter(
            DeltaSinkTestUtils.getHadoopConf(),
            tablePath,
            updatedSchema,
            false // canTryUpdateSchema
        );

        // WHEN
        globalCommitter.commit(globalCommittables);
    }

    @Test(expected = RuntimeException.class)
    public void testCanTryUpdateIncompatibleSchema() throws Exception {
        //GIVEN
        DeltaSinkTestUtils.initializeTestStateForPartitionedDeltaTable(tablePath.getPath());

        List<DeltaCommittable> deltaCommittables = DeltaSinkTestUtils.getListOfDeltaCommittables(
            3, DeltaSinkTestUtils.getTestPartitionSpec());
        List<DeltaGlobalCommittable> globalCommittables =
            Collections.singletonList(new DeltaGlobalCommittable(deltaCommittables));

        // new schema drops one of the previous columns
        List<RowType.RowField> fields = new ArrayList<>(
            DeltaSinkTestUtils.TEST_ROW_TYPE
                .getFields()
                .subList(0, DeltaSinkTestUtils.TEST_ROW_TYPE.getFields().size() - 2)
        );
        fields.add(new RowType.RowField("someNewField", new IntType()));
        RowType updatedSchema = new RowType(fields);

        DeltaGlobalCommitter globalCommitter = new DeltaGlobalCommitter(
            DeltaSinkTestUtils.getHadoopConf(),
            tablePath,
            updatedSchema,
            true // canTryUpdateSchema
        );

        // WHEN
        globalCommitter.commit(globalCommittables);
    }

    @Test(expected = RuntimeException.class)
    public void testWrongStreamPartitionValues() throws Exception {
        //GIVEN
        DeltaSinkTestUtils.initializeTestStateForPartitionedDeltaTable(tablePath.getPath());

        LinkedHashMap<String, String> nonMatchingPartitionSpec =
            DeltaSinkTestUtils.getTestPartitionSpec();
        nonMatchingPartitionSpec.remove(nonMatchingPartitionSpec.keySet().toArray()[0]);
        List<DeltaCommittable> deltaCommittables = DeltaSinkTestUtils.getListOfDeltaCommittables(
            1, nonMatchingPartitionSpec);
        List<DeltaGlobalCommittable> globalCommittables =
            Collections.singletonList(new DeltaGlobalCommittable(deltaCommittables));

        DeltaGlobalCommitter globalCommitter = new DeltaGlobalCommitter(
            DeltaSinkTestUtils.getHadoopConf(),
            tablePath,
            DeltaSinkTestUtils.TEST_ROW_TYPE,
            false // canTryUpdateSchema
        );

        // WHEN
        globalCommitter.commit(globalCommittables);
    }

    @Test
    public void testCommittablesFromDifferentCheckpointInterval() {
        //GIVEN
        int numAddedFiles1 = 3;
        int numAddedFiles2 = 5;
        DeltaLog deltaLog = DeltaLog.forTable(
            DeltaSinkTestUtils.getHadoopConf(), tablePath.getPath());
        assertEquals(-1, deltaLog.snapshot().getVersion());

        List<DeltaCommittable> deltaCommittables = DeltaSinkTestUtils.getListOfDeltaCommittables(
            numAddedFiles1, new LinkedHashMap<>(), 1);
        deltaCommittables.addAll(DeltaSinkTestUtils.getListOfDeltaCommittables(
            numAddedFiles2, new LinkedHashMap<>(), 2));
        List<DeltaGlobalCommittable> globalCommittables =
            Collections.singletonList(new DeltaGlobalCommittable(deltaCommittables));

        DeltaGlobalCommitter globalCommitter = new DeltaGlobalCommitter(
            DeltaSinkTestUtils.getHadoopConf(),
            tablePath,
            DeltaSinkTestUtils.TEST_ROW_TYPE,
            false // canTryUpdateSchema
        );

        // WHEN
        globalCommitter.commit(globalCommittables);

        // THEN
        // we should have committed both checkpoints intervals so current snapshot version should
        // be 1 and should contain files from both intervals.
        deltaLog.update();
        assertEquals(1, deltaLog.snapshot().getVersion());
        assertEquals(
            numAddedFiles1 + numAddedFiles2,
            deltaLog.snapshot().getAllFiles().size());
    }

    /**
     * TODO
     * Test cases to cover:
     * test with committables from different checkpoint intervals, one outdated (only one should
     * pass)
     * test with committables from different checkpoint intervals with different schemas
     * test with committables from the same checkpoint interval containg different partition columns
     * (should fail)
     */

    @Test
    public void test() throws Exception {
    }

    @Test
    public void testGlobalCommittableSerializerWithCommittables() throws IOException {
        // GIVEN
        LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
        partitionSpec.put("a", "b");
        partitionSpec.put("c", "d");

        List<DeltaCommittable> deltaCommittables = Arrays.asList(
            new DeltaCommittable(
                DeltaSinkTestUtils.getTestDeltaPendingFile(partitionSpec),
                TEST_APP_ID,
                TEST_CHECKPOINT_ID),
            new DeltaCommittable(
                DeltaSinkTestUtils.getTestDeltaPendingFile(partitionSpec),
                TEST_APP_ID,
                TEST_CHECKPOINT_ID + 1)
        );
        DeltaGlobalCommittable globalCommittable = new DeltaGlobalCommittable(deltaCommittables);

        // WHEN
        DeltaGlobalCommittable deserialized = serializeAndDeserialize(globalCommittable);

        // THEN
        for (int i = 0; i < deserialized.getDeltaCommittables().size(); i++) {
            DeltaSinkTestUtils.validateDeltaCommittablesEquality(
                globalCommittable.getDeltaCommittables().get(i),
                deserialized.getDeltaCommittables().get(i),
                partitionSpec
            );
        }
    }

    @Test
    public void testGlobalCommittableSerializerWithEmptyCommittables() throws IOException {
        // GIVEN
        DeltaGlobalCommittable globalCommittable = new DeltaGlobalCommittable(new ArrayList<>());

        // WHEN
        DeltaGlobalCommittable deserialized = serializeAndDeserialize(globalCommittable);

        // THEN
        assertTrue(globalCommittable.getDeltaCommittables().isEmpty());
        assertTrue(deserialized.getDeltaCommittables().isEmpty());
    }

    ///////////////////////////////////////////////////
    // serde test utils
    ///////////////////////////////////////////////////

    private DeltaGlobalCommittable serializeAndDeserialize(DeltaGlobalCommittable globalCommittable)
        throws IOException {
        DeltaGlobalCommittableSerializer serializer =
            new DeltaGlobalCommittableSerializer(
                new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
                    FileSinkTestUtils.TestPendingFileRecoverable::new)
            );
        byte[] data = serializer.serialize(globalCommittable);
        return serializer.deserialize(serializer.getVersion(), data);
    }
}
