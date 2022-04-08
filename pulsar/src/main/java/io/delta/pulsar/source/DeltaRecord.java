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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.FieldSchemaBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;

import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.ByteType;
import io.delta.standalone.types.DateType;
import io.delta.standalone.types.DecimalType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.NullType;
import io.delta.standalone.types.ShortType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.TimestampType;

/**
 * A record wrapping an dataChange or metaChange message.
 */
@Data
@Slf4j
public class DeltaRecord implements Record<GenericRecord> {
    protected static final String OP_FIELD = "op";
    protected static final String PARTITION_VALUE_FIELD = "partition_value";
    protected static final String CAPTURE_TS_FIELD = "capture_ts";
    protected static final String TS_FIELD = "ts";

    protected static final String OP_ADD_RECORD = "c";
    protected static final String OP_DELETE_RECORD = "r";
    protected static final String OP_META_SCHEMA = "m";

    private Map<String, String> properties;
    private GenericRecord value;
    protected static GenericSchema<GenericRecord> pulsarSchema;
    protected static StructType deltaSchema;
    private String topic;
    protected static Map<Integer, Long> msgSeqCntMap;
    private long sequence;
    private long partition;
    private String partitionValue;
    private AtomicInteger processingException;

    public static GenericSchema<GenericRecord> convertToPulsarSchema(StructType deltaSchema)
        throws IOException {
        // convert delta schema to pulsar topic schema
        RecordSchemaBuilder builder = SchemaBuilder.record(deltaSchema.getTypeName());
        FieldSchemaBuilder fbuilder = null;
        for (int i = 0; i < deltaSchema.getFields().length; i++) {
            StructField field = deltaSchema.getFields()[i];
            fbuilder = builder.field(field.getName());
            if (field.isNullable()){
                fbuilder = fbuilder.optional();
            } else {
                fbuilder = fbuilder.required();
            }
            if (field.getDataType() instanceof StringType) {
                fbuilder = fbuilder.type(SchemaType.STRING);
            } else if (field.getDataType() instanceof BooleanType) {
                fbuilder = fbuilder.type(SchemaType.BOOLEAN);
            } else if (field.getDataType() instanceof BinaryType) {
                fbuilder = fbuilder.type(SchemaType.BYTES);
            } else if (field.getDataType() instanceof DoubleType) {
                fbuilder = fbuilder.type(SchemaType.DOUBLE);
            } else if (field.getDataType() instanceof FloatType) {
                fbuilder = fbuilder.type(SchemaType.FLOAT);
            } else if (field.getDataType() instanceof IntegerType) {
                fbuilder = fbuilder.type(SchemaType.INT32);
            } else if (field.getDataType() instanceof LongType) {
                fbuilder = fbuilder.type(SchemaType.INT64);
            } else if (field.getDataType() instanceof ShortType) {
                fbuilder = fbuilder.type(SchemaType.INT16);
            } else if (field.getDataType() instanceof ByteType) {
                fbuilder = fbuilder.type(SchemaType.INT8);
            } else if (field.getDataType() instanceof NullType) {
                fbuilder = fbuilder.type(SchemaType.NONE);
            } else if (field.getDataType() instanceof DateType) {
                fbuilder = fbuilder.type(SchemaType.DATE);
            } else if (field.getDataType() instanceof TimestampType) {
                fbuilder = fbuilder.type(SchemaType.TIMESTAMP);
            } else if (field.getDataType() instanceof DecimalType) {
                fbuilder = fbuilder.type(SchemaType.DOUBLE);
            } else { // not support other data type
                fbuilder = fbuilder.type(SchemaType.STRING);
            }
        }
        if (fbuilder == null) {
            throw new IOException("filed is empty, can not covert to pulsar schema");
        }

        GenericSchema<GenericRecord> pulsarSchema = Schema.generic(builder.build(SchemaType.AVRO));
        log.info("Converted delta Schema: {} to pulsar schema: {}",
            deltaSchema, pulsarSchema.getSchemaInfo().getSchemaDefinition());
        return pulsarSchema;
    }


    @Override
    public Optional<String> getTopicName() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getKey() {
        return Optional.of(partitionValue);
    }

    @Override
    public Schema<GenericRecord> getSchema() {
        return pulsarSchema;
    }

    @Override
    public GenericRecord getValue() {
        return value;
    }

    @Override
    public Optional<Long> getEventTime() {
        try {
            Long s = Long.parseLong(properties.get(TS_FIELD));
            return Optional.of(s);
        } catch (NumberFormatException e) {
            log.error("Failed to get event time ", e);
            return Optional.of(0L);
        }
    }

    @Override
    public Optional<String> getPartitionId() {
        return Optional.of(String.format("%s-%d", topic, partition));
    }

    @Override
    public Optional<Integer> getPartitionIndex() {
        return Optional.empty();
    }

    @Override
    public Optional<Long> getRecordSequence() {
        return Optional.of(sequence);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void ack() {
        // TODO
    }

    @Override
    public void fail() {
        log.info("send message partition {} sequence {} failed", partition, sequence);
        processingException.incrementAndGet();
    }

    @Override
    public Optional<String> getDestinationTopic() {
        return Optional.of(this.topic);
    }

    @Override
    public Optional<Message<GenericRecord>> getMessage() {
        return Optional.empty();
    }
}
