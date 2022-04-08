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

package io.delta.pulsar;

/**
 * DeltaLake connect stats.
 */
public interface DeltaLakeConnectorStats {
    String SOURCE_SCOPE = "source";
    String SINK_SCOPE = "sink";

    // source connector
    String RECORD_SEND_COUNT = SOURCE_SCOPE + "_record_send_count";
    String CURRENT_READ_DELTA_TABLE_VERSION = SOURCE_SCOPE + "_current_read_delta_table_version";
    String PREPARE_READ_FILES_COUNT = SOURCE_SCOPE + "_prepare_read_files_count";
    String PREPARE_READ_FILES_BYTES_SIZE = SOURCE_SCOPE + "_prepare_read_files_bytes_size";
    String MAX_READ_FILES_CONCURRENCY = SOURCE_SCOPE + "_max_read_files_concurrency";
    String GET_DELTA_ACTION_LATENCY = SOURCE_SCOPE + "_get_delta_action_latency";
    String FETCH_ADN_PARSE_FILE_LATENCY = SOURCE_SCOPE + "_fetch_and_parse_file_latency";
    String FILTERED_FILES = SOURCE_SCOPE + "_filtered_files_count";

}
