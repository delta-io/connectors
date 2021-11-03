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
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * Tests the serialization and deserialization for {@link DeltaCommittable}.
 */
public class DeltaCommittableSerializerTest {

    @Test
    public void testCommittableWithPendingFileForNonPartitionedTable() throws IOException {
        // GIVEN
        LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
        DeltaCommittable committable = TestDeltaCommittable.getTestDeltaCommittableWithPendingFile(partitionSpec);

        // WHEN
        DeltaCommittable deserialized = serializeAndDeserialize(committable);

        // THEN
        TestDeltaCommittable.validateDeltaCommittablesEquality(committable, deserialized, partitionSpec);
    }

    @Test
    public void testCommittableWithPendingFileForPartitionedTable() throws IOException {
        // GIVEN
        LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<String, String>() {{
            put("a", "b");
            put("c", "d");
        }};
        DeltaCommittable committable = TestDeltaCommittable.getTestDeltaCommittableWithPendingFile(partitionSpec);

        // WHEN
        DeltaCommittable deserialized = serializeAndDeserialize(committable);

        // THEN
        TestDeltaCommittable.validateDeltaCommittablesEquality(committable, deserialized, partitionSpec);
    }

    private DeltaCommittable serializeAndDeserialize(DeltaCommittable committable)
            throws IOException {
        DeltaCommittableSerializer serializer =
                new DeltaCommittableSerializer(
                        new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(FileSinkTestUtils.TestPendingFileRecoverable::new));
        byte[] data = serializer.serialize(committable);
        return serializer.deserialize(serializer.getVersion(), data);
    }

}
