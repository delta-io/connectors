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

package org.apache.flink.connector.delta.sink.committables;

import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.TestDeltaCommittable;
import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils.TestDeltaPendingFile;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

/**
 * Tests the serialization and deserialization for {@link DeltaGlobalCommittable}.
 */
public class DeltaGlobalCommittableSerializerTest {

    private final String TEST_APP_ID = UUID.randomUUID().toString();
    private final long TEST_CHECKPOINT_ID = new Random().nextInt(10);

    @Test
    public void testGlobalCommittableSerializerWithCommittables() throws IOException {
        // GIVEN
        LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<String, String>() {{
            put("a", "b");
            put("c", "d");
        }};
        List<DeltaCommittable> deltaCommittables = Arrays.asList(
                new DeltaCommittable(TestDeltaPendingFile.getTestDeltaPendingFile(partitionSpec), TEST_APP_ID, TEST_CHECKPOINT_ID),
                new DeltaCommittable(TestDeltaPendingFile.getTestDeltaPendingFile(partitionSpec), TEST_APP_ID, TEST_CHECKPOINT_ID + 1)
        );
        DeltaGlobalCommittable globalCommittable = new DeltaGlobalCommittable(deltaCommittables);

        // WHEN
        DeltaGlobalCommittable deserialized = serializeAndDeserialize(globalCommittable);

        // THEN
        for (int i = 0; i < deserialized.getDeltaCommittables().size(); i++) {
            TestDeltaCommittable.validateDeltaCommittablesEquality(
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


    private DeltaGlobalCommittable serializeAndDeserialize(DeltaGlobalCommittable globalCommittable)
            throws IOException {
        DeltaGlobalCommittableSerializer serializer =
                new DeltaGlobalCommittableSerializer(
                        new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(FileSinkTestUtils.TestPendingFileRecoverable::new)
                );
        byte[] data = serializer.serialize(globalCommittable);
        return serializer.deserialize(serializer.getVersion(), data);
    }


}
