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
 * Copyright (2020) The Delta Lake Project Authors.
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

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

/**
 * todo
 */
public final class FieldMetadata {
    private final Map<String, Object> metadata;


    /**
     * todo
     * @param metadata
     */
    private FieldMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    // getters

    // TODO is this acceptable to do? it's more of a "pretty" string?
    @Override
    public String toString(){
        return metadata.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.toString())
                .collect(Collectors.joining(", ", "{", "}"));
    }

    private static Boolean valueEquals(Object v1, Object v2) {
    }

    // TODO test this!!
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldMetadata that = (FieldMetadata) o;
        if (this.metadata.size() != that.metadata.size()) return false;
        return this.metadata.entrySet().stream().allMatch(e ->
                e.getValue().equals(that.metadata.get(e.getKey())) ||
                        (// TODO array equals????);
    }



//    @Override
//    public boolean equals(Object o){
//        // todo
//    }

    // contains
    // getters!
    //  hashcode
    // toJsonValue??

    public static Builder builder() {
        return new Builder();
    }

    private static class Builder {
        private Map<String, Object> metadata =new HashMap<String, Object>();

        public Builder putNull(String key) {
            metadata.put(key, null);
            return this;
        }

        public Builder putLong(String key, Long value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putDouble(String key, Double value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putBoolean(String key, Boolean value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putString(String key, String value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putMetadata(String key, FieldMetadata value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putLongArray(String key, Long[] value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putDoubleArray(String key, Double[] value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putBooleanArray(String key, Boolean[] value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putStringArray(String key, String[] value) {
            metadata.put(key, value);
            return this;
        }

        public Builder putMetadataArray(String key, FieldMetadata[] value) {
            metadata.put(key, value);
            return this;
        }

        public FieldMetadata build() {
            return new FieldMetadata(this.metadata);
        }
    }
}
