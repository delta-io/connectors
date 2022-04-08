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

import java.io.IOException;
import java.io.Serializable;
import java.util.Locale;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.pulsar.common.Category;
import io.delta.pulsar.common.FieldContext;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.Strings;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * The configuration class for {@link io.delta.pulsar.DeltaSource}.
 */

@Data
@Slf4j
public class DeltaSourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final long DEFAULT_MAX_READ_BYTES_SIZE_ONE_ROUND =
        Double.valueOf(Runtime.getRuntime().totalMemory() * 0.2).longValue();
    public static final int DEFAULT_MAX_READ_ROW_COUNT_ONE_ROUND = 100_000;
    public static final int DEFAULT_PARQUET_PARSE_PARALLELISM =
        Runtime.getRuntime().availableProcessors();
    public static final int DEFAULT_CHECKPOINT_INTERVAL = 30;
    public static final String FILE_SYSTEM = "filesystem";
    public static final String S3 = "s3";
    public static final long LATEST = -1;
    public static final long EARLIEST = -2;
    public static final int DEFAULT_SOURCE_CONNECTOR_QUEUE_SIZE = 100_000;

    @Category
    private static final String CATEGORY_SOURCE = "Source";
    @Category
    private static final String CATEGORY_SINK = "Sink";

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "The start version of the delta lake table to fetch cdc log."
    )
    Long startSnapshotVersion;

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "The start time stamp of the delta lake table to fetch cdc log. Time unit: second"
    )
    Long startTimestamp;

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "Whether fetch the history data of the table. Default is: false"
    )
    Boolean fetchHistoryData = false;

    @FieldContext(
        category = CATEGORY_SOURCE,
        required = true,
        doc = "The table path to fetch"
    )
    String tablePath;

    @FieldContext(
        category = CATEGORY_SOURCE,
        required = true,
        doc = "The file system type of store delta lake table. Values are filesystem or s3"
    )
    String fileSystemType;

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "If the file system type is s3, the access key should be set."
    )
    String s3aAccesskey;

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "If the file system type is s3, the secret key should be set."
    )
    String s3aSecretKey;

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "The parallelism of parsing parquet file. Default is 8"
    )
    int parquetParseParallelism = DEFAULT_PARQUET_PARSE_PARALLELISM;

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "The max read bytes size in one round. Default is 20% of heap memory"
    )
    long maxReadBytesSizeOneRound = DEFAULT_MAX_READ_BYTES_SIZE_ONE_ROUND;

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "The max read number of rows in one round. Default is 1_000_000"
    )
    int maxReadRowCountOneRound = DEFAULT_MAX_READ_ROW_COUNT_ONE_ROUND;

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "checkpoint interval, time unit: second, Default is 30s"
    )
    int checkpointInterval = DEFAULT_CHECKPOINT_INTERVAL;

    @FieldContext(
        category = CATEGORY_SOURCE,
        doc = "source connector queue size, used for store record before send to pulsar topic. "
            + "Default is 100_000"
    )
    int sourceConnectorQueueSize = DEFAULT_SOURCE_CONNECTOR_QUEUE_SIZE;


    /**
     * Validate if the configuration is valid.
     */
    public void validate() throws IOException {
        if (startSnapshotVersion != null && startTimestamp != null) {
            log.error("startSnapshotVersion: {} and startTimeStamp: {} "
                    + "should not be set at the same time.",
                startSnapshotVersion, startTimestamp);
            throw new IllegalArgumentException("startSnapshotVersion and startTimeStamp "
                + "should not be set at the same time.");
        } else if (startSnapshotVersion == null && startTimestamp == null) {
            startSnapshotVersion = LATEST;
        }

        if (StringUtils.isBlank(tablePath)
            || StringUtils.isBlank(fileSystemType)) {
            log.error("tablePath: {} or fileSystemType: {} should be set.",
                tablePath, fileSystemType);
            throw new IllegalArgumentException("tablePath: " + tablePath + " or fileSystemType: "
                + fileSystemType + " should be set.");
        }

        fileSystemType = fileSystemType.toLowerCase(Locale.ROOT);

        switch (fileSystemType) {
            case S3:
                if (Strings.isNullOrEmpty(s3aAccesskey) || Strings.isNullOrEmpty(s3aSecretKey)) {
                    log.error("s3aAccesskey or s3aSecretkey should be configured for s3");
                    throw new IllegalArgumentException("s3aAccesskey or s3aSecretkey "
                        + "should be configured for s3");
                }
                break;
            case FILE_SYSTEM:
                break;
            default:
                log.error("fileSystemType: {} is not support yet. Current support type: "
                        + "fileSystem and s3",
                    fileSystemType);
                throw new IOException("fileSystemType: " + fileSystemType
                    + " is not support yet. Current support type: fileSystem and s3");
        }

        if (parquetParseParallelism > DEFAULT_PARQUET_PARSE_PARALLELISM
            || parquetParseParallelism <= 0) {
            log.warn("parquetParseParallelism: {} is out of limit, using default 2 * cpus: {}",
                parquetParseParallelism, DEFAULT_PARQUET_PARSE_PARALLELISM);
            parquetParseParallelism = DEFAULT_PARQUET_PARSE_PARALLELISM;
        }

        if (maxReadBytesSizeOneRound <= 0) {
            log.warn("maxReadBytesSizeOneRound: {} should be > 0, using default: {}",
                maxReadBytesSizeOneRound, DEFAULT_MAX_READ_BYTES_SIZE_ONE_ROUND);
            maxReadBytesSizeOneRound = DEFAULT_MAX_READ_BYTES_SIZE_ONE_ROUND;
        }

        if (maxReadRowCountOneRound <= 0) {
            log.warn("maxReadRowCountOneRound: {} should be > 0, using default: {}",
                maxReadRowCountOneRound, DEFAULT_MAX_READ_ROW_COUNT_ONE_ROUND);
            maxReadRowCountOneRound = DEFAULT_MAX_READ_ROW_COUNT_ONE_ROUND;
        }

        if (sourceConnectorQueueSize <= 0) {
            log.warn("sourceConnectorQueueSize: {} should be >0, using default: {}",
                sourceConnectorQueueSize, DEFAULT_SOURCE_CONNECTOR_QUEUE_SIZE);
            sourceConnectorQueueSize = DEFAULT_SOURCE_CONNECTOR_QUEUE_SIZE;
        }
    }

    /**
     * Pase DeltaLakeConnectorConfig from map.
     *
     * @param map
     * @return
     * @throws IOException
     */
    public static DeltaSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = ObjectMapperFactory.getThreadLocal();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map),
            DeltaSourceConfig.class);
    }

    public static ObjectMapper jsonMapper() {
        return ObjectMapperFactory.getThreadLocal();
    }

    @Override
    public String toString() {
        try {
            return jsonMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            log.error("Failed to write DeltaLakeConnectorConfig ", e);
            return "";
        }
    }
}
