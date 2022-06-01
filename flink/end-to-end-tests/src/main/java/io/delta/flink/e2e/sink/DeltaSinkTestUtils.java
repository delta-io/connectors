package io.delta.flink.e2e.sink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import io.delta.flink.sink.DeltaSink;
import io.delta.flink.sink.internal.DeltaBucketAssigner;
import io.delta.flink.sink.internal.DeltaPartitionComputer;
import io.delta.flink.sink.internal.DeltaSinkBuilder;
import io.delta.flink.sink.internal.DeltaSinkInternal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

public class DeltaSinkTestUtils {

    public static final RowType TEST_ROW_TYPE = new RowType(Arrays.asList(
        new RowType.RowField("name", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("surname", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("age", new IntType())
    ));

    public static DeltaSinkInternal<RowData> createDeltaSink(String deltaTablePath,
                                                             boolean isTablePartitioned) {
        if (isTablePartitioned) {
            DeltaSinkBuilder<RowData> builder = new DeltaSinkBuilder.DefaultDeltaFormatBuilder<>(
                new Path(deltaTablePath),
                DeltaSinkTestUtils.getHadoopConf(),
                ParquetRowDataBuilder.createWriterFactory(
                    DeltaSinkTestUtils.TEST_ROW_TYPE,
                    DeltaSinkTestUtils.getHadoopConf(),
                    true // utcTimestamp
                ),
                new BasePathBucketAssigner<>(),
                OnCheckpointRollingPolicy.build(),
                DeltaSinkTestUtils.TEST_ROW_TYPE,
                false // mergeSchema
            );
            return builder
                .withBucketAssigner(getTestPartitionAssigner())
                .build();
        }
        return DeltaSink
            .forRowData(
                new Path(deltaTablePath),
                DeltaSinkTestUtils.getHadoopConf(),
                DeltaSinkTestUtils.TEST_ROW_TYPE).build();
    }

    public static org.apache.hadoop.conf.Configuration getHadoopConf() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("parquet.compression", "SNAPPY");
        conf.set("io.delta.standalone.PARQUET_DATA_TIME_ZONE_ID", "UTC");
        return conf;
    }

    public static DeltaBucketAssigner<RowData> getTestPartitionAssigner() {
        DeltaPartitionComputer<RowData> partitionComputer =
            (element, context) -> new LinkedHashMap<String, String>() {
                {
                    put("col1", Integer.toString(ThreadLocalRandom.current().nextInt(0, 2)));
                    put("col2", Integer.toString(ThreadLocalRandom.current().nextInt(0, 2)));
                }
            };
        return new DeltaBucketAssigner<>(partitionComputer);
    }

    public static List<RowData> getTestRowData(int num_records) {
        List<RowData> rows = new ArrayList<>(num_records);
        for (int i = 0; i < num_records; i++) {
            Integer v = i;
            rows.add(
                CONVERTER.toInternal(
                    Row.of(
                        String.valueOf(v),
                        String.valueOf((v + v)),
                        v)
                )
            );
        }
        return rows;
    }

    public static final DataFormatConverters.DataFormatConverter<RowData, Row> CONVERTER =
        DataFormatConverters.getConverterForDataType(
            TypeConversions.fromLogicalToDataType(TEST_ROW_TYPE)
        );

}
