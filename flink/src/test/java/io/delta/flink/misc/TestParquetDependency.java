package io.delta.flink.misc;

import io.delta.utils.ParquetSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;

import io.delta.standalone.types.LongType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructType;

public class TestParquetDependency {

    @Test
    public void test_dependency() {
        StructType schema = new StructType()
            .add("col1", new StringType())
            .add("col2", new LongType());
        MessageType messageType = ParquetSchemaConverter.deltaToParquet(schema);
    }
}
