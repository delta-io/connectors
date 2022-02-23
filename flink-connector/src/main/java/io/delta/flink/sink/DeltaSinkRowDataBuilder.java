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

package io.delta.flink.sink;

import java.util.Collections;
import java.util.List;

import io.delta.flink.sink.internal.DeltaBucketAssigner;
import io.delta.flink.sink.internal.DeltaPartitionComputer;
import io.delta.flink.sink.internal.DeltaSinkBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

/**
 * A builder class for {@link DeltaSink} for a stream of {@link RowData}.
 * <p>
 * For most common use cases use {@link DeltaSink#forRowData} utility method to instantiate the
 * sink. After instantiation of this builder you can either call
 * {@link DeltaSinkRowDataBuilder#build()} method to get the instance of a {@link DeltaSink} or
 * configure additional behaviour (like merging of the schema or setting partition columns) and then
 * build the sink.
 */
public class DeltaSinkRowDataBuilder {

    /**
     * Delta table's root path
     */
    private final Path tableBasePath;

    /**
     * Flink's logical type to indicate the structure of the events in the stream
     */
    private final RowType rowType;

    /**
     * Hadoop's {@link Configuration} object
     */
    private final Configuration conf;

    /**
     * Indicator whether we should try to update table's schema with stream's schema in case
     * those will not match. The update is not guaranteed as there will be still some checks
     * performed whether the updates to the schema are compatible.
     */
    private boolean mergeSchema;

    /**
     * List of partition column names in the order they should be applied when creating a
     * destination path.
     */
    private List<String> partitionKeys = Collections.emptyList();

    /**
     * Creates instance of the builder for {@link DeltaSink}.
     *
     * @param tableBasePath path to a Delta table
     * @param conf          Hadoop's conf object
     * @param rowType       Flink's logical type to indicate the structure of the events in
     *                      the stream
     * @param mergeSchema   indicator whether we should try to update table's schema with
     *                      stream's schema in case those will not match. The update is not
     *                      guaranteed as there will be still some checks performed whether
     *                      the updates to the schema are compatible.
     */
    public DeltaSinkRowDataBuilder(
        Path tableBasePath,
        Configuration conf,
        RowType rowType,
        boolean mergeSchema) {
        this.tableBasePath = tableBasePath;
        this.conf = conf;
        this.rowType = rowType;
        this.mergeSchema = mergeSchema;
    }

    /**
     * Sets the sink's option whether in case of any differences between stream's schema and Delta
     * table's schema we should try to update it during commit to the
     * {@link io.delta.standalone.DeltaLog}. The update is not guaranteed as there will be some
     * compatibility checks performed.
     *
     * @param mergeSchema indicator whether we should try to update table's schema with stream's
     *                    schema in case those will not match. The update is not guaranteed as there
     *                    will be still some checks performed whether the updates to the schema are
     *                    compatible.
     * @return builder for {@link DeltaSink}
     */
    public DeltaSinkRowDataBuilder withMergeSchema(final boolean mergeSchema) {
        this.mergeSchema = mergeSchema;
        return this;
    }

    /**
     * Sets list of partition fields that will be extracted of incoming {@link RowData} events.
     * <p>
     * Provided fields' names must correspond to the names provided in the {@link RowType} object
     * for this sink and must be in the same order as expected order of occurrence in the partition
     * path that will be generated.
     *
     * @param partitionKeys list of partition column names in the order they should be applied when
     *                      creating destination path.
     * @return builder for {@link DeltaSink}
     */
    public DeltaSinkRowDataBuilder withPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
        return this;
    }

    /**
     * Creates the actual sink.
     *
     * @return constructed {@link DeltaSink} object
     */
    public DeltaSink<RowData> build() {
        conf.set("parquet.compression", "SNAPPY");
        ParquetWriterFactory<RowData> writerFactory = ParquetRowDataBuilder.createWriterFactory(
            rowType,
            conf,
            true // utcTimestamp
        );

        DeltaSinkBuilder<RowData> sinkBuilderInternal =
            new DeltaSinkBuilder.DefaultDeltaFormatBuilder<>(
                tableBasePath,
                conf,
                writerFactory,
                resolveBucketAssigner(),
                OnCheckpointRollingPolicy.build(),
                rowType,
                mergeSchema
            );
        return new DeltaSink<>(sinkBuilderInternal);
    }

    private BucketAssigner<RowData, String> resolveBucketAssigner() {
        if (this.partitionKeys.isEmpty()) {
            return new BasePathBucketAssigner<>();
        }
        DeltaPartitionComputer<RowData> partitionComputer =
            new DeltaPartitionComputer.DeltaRowDataPartitionComputer(
                rowType, partitionKeys);
        return new DeltaBucketAssigner<>(partitionComputer);
    }
}
