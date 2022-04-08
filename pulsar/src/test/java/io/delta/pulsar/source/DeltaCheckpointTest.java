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

import org.testng.annotations.Test;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


/**
 * DeltaCheckpoint test.
 */
public class DeltaCheckpointTest {

    @Test
    public void testGetStateKey() {
        String key = "checkpoint0";
        assertEquals(key, DeltaCheckpoint.getStatekey(0));
    }

    @Test
    public void testDeltaCheckpointCompare() {
        DeltaCheckpoint checkpointA =
            new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, 5);
        DeltaCheckpoint checkpointB =
            new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, 5);
        assertTrue(checkpointA.compareTo(checkpointB) < 0);

        checkpointA = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, 5);
        checkpointB = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, 3);
        assertTrue(checkpointA.compareTo(checkpointB) < 0);

        checkpointA = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, 3);
        checkpointB = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, 5);
        assertTrue(checkpointA.compareTo(checkpointB) < 0);

        checkpointA = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, 5);
        checkpointB = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, 5);
        assertTrue(checkpointA.compareTo(checkpointB) > 0);

        checkpointA = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, 5);
        checkpointB = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, 3);
        assertTrue(checkpointA.compareTo(checkpointB) > 0);

        checkpointA = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, 3);
        checkpointB = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, 5);
        assertTrue(checkpointA.compareTo(checkpointB) > 0);

        checkpointA = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, 5);
        checkpointB = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, 3);
        assertTrue(checkpointA.compareTo(checkpointB) > 0);

        checkpointA = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, 3);
        checkpointB = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, 5);
        assertTrue(checkpointA.compareTo(checkpointB) < 0);

        checkpointA = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, 3);
        checkpointB = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, 3);
        assertTrue(checkpointA.compareTo(checkpointB) == 0);

        checkpointA = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, 5);
        checkpointB = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, 3);
        assertTrue(checkpointA.compareTo(checkpointB) > 0);

        checkpointA = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, 3);
        checkpointB = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, 5);
        assertTrue(checkpointA.compareTo(checkpointB) < 0);

        checkpointA = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, 3);
        checkpointB = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, 3);
        assertTrue(checkpointA.compareTo(checkpointB) == 0);
    }

    @Test
    public void testCompareVersionAndIndex() {
        DeltaCheckpoint checkpointA =
            new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, 3);
        DeltaCheckpoint checkpointB =
            new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, 3);
        checkpointA.setMetadataChangeFileIndex(5L);
        checkpointB.setMetadataChangeFileIndex(3L);
        assertTrue(checkpointA.compareVersionAndIndex(checkpointB) > 0);

        checkpointA.setMetadataChangeFileIndex(3L);
        checkpointB.setMetadataChangeFileIndex(5L);
        assertTrue(checkpointA.compareVersionAndIndex(checkpointB) < 0);

        checkpointA.setMetadataChangeFileIndex(3L);
        checkpointB.setMetadataChangeFileIndex(3L);
        assertTrue(checkpointA.compareVersionAndIndex(checkpointB) == 0);

        checkpointA.setMetadataChangeFileIndex(3L);
        checkpointB.setMetadataChangeFileIndex(3L);
        checkpointA.setRowNum(100L);
        checkpointB.setRowNum(1000L);
        assertTrue(checkpointA.compareVersionAndIndex(checkpointB) == 0);
        assertTrue(checkpointA.compareTo(checkpointB) < 0);

        checkpointA.setMetadataChangeFileIndex(3L);
        checkpointB.setMetadataChangeFileIndex(3L);
        checkpointA.setRowNum(1000L);
        checkpointB.setRowNum(1000L);
        checkpointA.setSeqCount(20L);
        checkpointB.setSeqCount(200L);
        assertTrue(checkpointA.compareVersionAndIndex(checkpointB) == 0);
        assertTrue(checkpointA.compareTo(checkpointB) == 0);
    }
}
