/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone.types;

import java.util.Objects;
import java.util.Optional;

import io.delta.standalone.internal.DeltaColumnMapping;

/**
 * A field inside a {@link StructType}.
 */
public final class StructField {
    private final String name;
    private final DataType dataType;
    private final boolean nullable;
    private final FieldMetadata metadata;

    /**
     * Constructor with default {@code nullable = true}.
     *
     * @param name  the name of this field
     * @param dataType  the data type of this field
     */
    public StructField(String name, DataType dataType) {
        this(name, dataType, true);
    }

    /**
     * @param name  the name of this field
     * @param dataType  the data type of this field
     * @param nullable  indicates if values of this field can be {@code null} values
     */
    public StructField(String name, DataType dataType, boolean nullable) {
        this(name, dataType, nullable, FieldMetadata.builder().build());
    }

    /**
     * @param name  the name of this field
     * @param dataType  the data type of this field
     * @param nullable  indicates if values of this field can be {@code null} values
     * @param metadata  metadata for this field
     */
    public StructField(String name, DataType dataType, boolean nullable, FieldMetadata metadata) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        this.metadata = metadata;
    }

    /**
     * @return the name of this field
     */
    public String getName() {
        return name;
    }

    /**
     * @return the data type of this field
     */
    public DataType getDataType() {
        return dataType;
    }

    /**
     * @return whether this field allows to have a {@code null} value.
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * @return the metadata for this field
     */
    public FieldMetadata getMetadata() {
        return metadata;
    }

    /**
     * @return if exists, the physical column name used to refer the column
     *         in underlying table data files
     */
    public Optional<String> getPhysicalName() {
        return Optional.ofNullable(
                        metadata.get(DeltaColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY()))
                .map(Object::toString);
    }

    /**
     * @return if exists, the physical column id used to refer the column
     *         in underlying table data files.
     */
    public Optional<String> getPhysicalId() {
        return Optional.ofNullable(
                        metadata.get(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY()))
                .map(Object::toString);
    }

    /**
     * Builds a readable {@code String} representation of this {@code StructField}.
     */
    protected void buildFormattedString(String prefix, StringBuilder builder) {
        final String nextPrefix = prefix + "    |";
        builder.append(String.format("%s-- %s: %s (nullable = %b) (metadata =%s)\n",
                prefix, name, dataType.getTypeName(), nullable, metadata.toString()));
        DataType.buildFormattedString(dataType, nextPrefix, builder);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StructField that = (StructField) o;
        return name.equals(that.name) && dataType.equals(that.dataType) && nullable == that.nullable
                && metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, nullable, metadata);
    }

    /**
     * @return Copy of this {@link StructField} with a new data type.
     */
    public StructField withNewDataType(DataType newDataType) {
        return new StructField(name, newDataType, nullable, metadata);
    }

    /**
     * @return Copy of this {@link StructField} with a new field name.
     */
    public StructField withNewName(String newName) {
        return new StructField(newName, dataType, nullable, metadata);
    }

    /**
     * @return Copy of this {@link StructField} with new metadata.
     */
    public StructField withNewMetadata(FieldMetadata newMetadata) {
        return new StructField(name, dataType, nullable, newMetadata);
    }
}
