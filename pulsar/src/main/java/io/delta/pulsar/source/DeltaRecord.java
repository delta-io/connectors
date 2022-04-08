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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.FieldSchemaBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;

import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;
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
    private DeltaReader.RowRecordData rowRecordData;
    protected static Map<Integer, Long> msgSeqCntMap;
    protected static Map<Integer, DeltaCheckpoint> saveCheckpointMap;
    private long sequence;
    private long partition;
    private String partitionValue;
    private AtomicInteger processingException;

    public DeltaRecord(DeltaReader.RowRecordData rowRecordData,
                       String topic,
                       StructType deltaSchema,
                       GenericSchema<GenericRecord> pulsarSchema,
                       AtomicInteger processingException) throws IOException {
        checkArgument(!(deltaSchema != null && pulsarSchema != null),
            "deltaSchema and pulsarSchema shouldn't be set at the same time");
        this.rowRecordData = rowRecordData;
        this.processingException = processingException;
        properties = new HashMap<>();
        this.topic = topic;

        if (deltaSchema != null && !deltaSchema.equals(DeltaRecord.deltaSchema)) {
            DeltaRecord.deltaSchema = deltaSchema;
            DeltaRecord.pulsarSchema = convertToPulsarSchema(deltaSchema);
        }

        if (pulsarSchema != null && !pulsarSchema.equals(DeltaRecord.pulsarSchema)) {
            DeltaRecord.pulsarSchema = pulsarSchema;
        }

        Action action = rowRecordData.nextCursor.act;
        if (action instanceof AddFile) {
            AddFile addFile = (AddFile) action;
            partitionValue = DeltaReader.partitionValueToString(addFile.getPartitionValues());
            properties.put(OP_FIELD, OP_ADD_RECORD);
            properties.put(PARTITION_VALUE_FIELD,
                DeltaReader.partitionValueToString(addFile.getPartitionValues()));
            properties.put(CAPTURE_TS_FIELD, String.valueOf(System.currentTimeMillis()));
            properties.put(TS_FIELD, String.valueOf(addFile.getModificationTime()));
            value = getGenericRecord(DeltaRecord.deltaSchema,
                DeltaRecord.pulsarSchema, rowRecordData);
        } else if (action instanceof RemoveFile) {
            RemoveFile removeFile = (RemoveFile) action;
            long deleteTimestamp = removeFile.getDeletionTimestamp().isPresent()
                ? removeFile.getDeletionTimestamp().get() : System.currentTimeMillis();
            partitionValue = DeltaReader.partitionValueToString(removeFile.getPartitionValues());
            properties.put(OP_FIELD, OP_DELETE_RECORD);
            properties.put(PARTITION_VALUE_FIELD,
                DeltaReader.partitionValueToString(removeFile.getPartitionValues()));
            properties.put(CAPTURE_TS_FIELD,  String.valueOf(System.currentTimeMillis()));
            properties.put(TS_FIELD, String.valueOf(deleteTimestamp));
            value = getGenericRecord(DeltaRecord.deltaSchema,
                DeltaRecord.pulsarSchema, rowRecordData);
        } else {
            log.error("DeltaRecord: Not Support this kind of record {}", action);
            throw new IOException("DeltaRecord: not support this kind of record");
        }

        String partitionValueStr = properties.get(PARTITION_VALUE_FIELD);
        partition = DeltaReader.getPartitionIdByDeltaPartitionValue(partitionValueStr,
                        DeltaReader.topicPartitionNum);
        if (msgSeqCntMap == null) {
            msgSeqCntMap = new ConcurrentHashMap<>();
        }
        if (saveCheckpointMap == null) {
            saveCheckpointMap = new ConcurrentHashMap<>();
        }
        Long msgCount = msgSeqCntMap.getOrDefault((int) partition, 0L);
        sequence = msgCount++;
        msgSeqCntMap.put((int) partition, msgCount);
    }

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

    public static GenericRecord getGenericRecord(StructType deltaSchema,
                                           GenericSchema<GenericRecord> pulsarSchema,
                                           DeltaReader.RowRecordData rowRecordData) {
        GenericRecordBuilder builder;
        if (deltaSchema != null) {
            builder = pulsarSchema.newRecordBuilder();
            for (int i = 0;
                 i < deltaSchema.getFields().length && i < rowRecordData.parquetSchema.size();
                 i++) {
                StructField field = deltaSchema.getFields()[i];
                Object value;
                try {
                    if (field.getDataType() instanceof StringType) {
                        value = rowRecordData.simpleGroup.getString(i, 0);
                    } else if (field.getDataType() instanceof BooleanType) {
                        value = rowRecordData.simpleGroup.getBoolean(i, 0);
                    } else if (field.getDataType() instanceof BinaryType) {
                        value = rowRecordData.simpleGroup.getBinary(i, 0);
                    } else if (field.getDataType() instanceof DoubleType) {
                        value = rowRecordData.simpleGroup.getDouble(i, 0);
                    } else if (field.getDataType() instanceof FloatType) {
                        value = rowRecordData.simpleGroup.getFloat(i, 0);
                    } else if (field.getDataType() instanceof IntegerType) {
                        value = rowRecordData.simpleGroup.getInteger(i, 0);
                    } else if (field.getDataType() instanceof LongType) {
                        value = rowRecordData.simpleGroup.getLong(i, 0);
                    } else if (field.getDataType() instanceof ShortType) {
                        value = rowRecordData.simpleGroup.getInteger(i, 0);
                    } else if (field.getDataType() instanceof ByteType) {
                        value = rowRecordData.simpleGroup.getInteger(i, 0);
                    } else if (field.getDataType() instanceof NullType) {
                        value = rowRecordData.simpleGroup.getInteger(i, 0);
                    } else if (field.getDataType() instanceof DateType) {
                        value = rowRecordData.simpleGroup.getTimeNanos(i, 0);
                    } else if (field.getDataType() instanceof TimestampType) {
                        value = rowRecordData.simpleGroup.getTimeNanos(i, 0);
                    } else if (field.getDataType() instanceof DecimalType) {
                        value = rowRecordData.simpleGroup.getDouble(i, 0);
                    } else { // not support other data type
                        value = rowRecordData.simpleGroup.getValueToString(i, 0);
                    }
                } catch (RuntimeException e) {
                    log.warn("Failed to get value, using null instead. ", e);
                    value = null;
                }
                builder.set(field.getName(), value);
            }
            return builder.build();
        } else if (pulsarSchema != null) {
            builder = pulsarSchema.newRecordBuilder();
            final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
            parser.setValidateDefaults(false);
            org.apache.avro.Schema avroSchema =
                    parser.parse(new String(pulsarSchema.getSchemaInfo().getSchema(),
                        StandardCharsets.UTF_8));
            log.info("parquet Schema: {} schemaInfo: {}",
                rowRecordData.parquetSchema, pulsarSchema.getSchemaInfo().getSchemaDefinition());
            for (int i = 0;
                 i < avroSchema.getFields().size() && i < rowRecordData.parquetSchema.size();
                 i++) {
                org.apache.avro.Schema.Field field = avroSchema.getFields().get(i);
                Object value;
                Set<org.apache.avro.Schema.Type> typeSet = new HashSet<>();
                field.schema().getTypes().forEach(t -> {
                    typeSet.add(t.getType());
                });
                if (typeSet.contains(org.apache.avro.Schema.Type.STRING)) {
                    value = rowRecordData.simpleGroup.getString(i, 0);
                } else if (typeSet.contains(org.apache.avro.Schema.Type.INT)) {
                    value = rowRecordData.simpleGroup.getInteger(i, 0);
                } else if (typeSet.contains(org.apache.avro.Schema.Type.LONG)) {
                    value = rowRecordData.simpleGroup.getLong(i, 0);
                } else if (typeSet.contains(org.apache.avro.Schema.Type.FLOAT)) {
                    value = rowRecordData.simpleGroup.getFloat(i, 0);
                } else if (typeSet.contains(org.apache.avro.Schema.Type.DOUBLE)) {
                    value = rowRecordData.simpleGroup.getDouble(i, 0);
                } else {
                    value = rowRecordData.simpleGroup.getValueToString(i, 0);
                }

                builder.set(field.name(), value);
            }
            return builder.build();
        } else {
            return null;
        }
    }

    public static void checkArgument(boolean expression, @NonNull Object errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    public static Map<Integer, DeltaCheckpoint> currentSnapshot() {
        return saveCheckpointMap;
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
        DeltaCheckpoint checkpoint;
        DeltaReader.ReadCursor cursor = rowRecordData.nextCursor;
        checkpoint = cursor.isFullSnapShot
            ? new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, cursor.version)
            : new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, cursor.version);

        checkpoint.setMetadataChangeFileIndex(cursor.changeIndex);
        checkpoint.setRowNum(cursor.rowNum);
        checkpoint.setSeqCount(sequence);
        saveCheckpointMap.put((int) partition, checkpoint);
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
