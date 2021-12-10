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

package io.delta.standalone.util;

import io.delta.standalone.types.StructType;

/**
 * Useful schema ({@link StructType}) functions.
 */
public final class SchemaUtils {

    private SchemaUtils(){};

    /**
     * Whether a new schema can replace the existing schema of a Delta table.
     * <p>
     * Returns false if the new schema:
     * <ul>
     *     <li>Drops any column that is present in the current schema</li>
     *     <li>Converts nullable=true to nullable=false for any column</li>
     *     <li>Changes any datatype</li>
     * </ul>
     *
     * @param existingSchema  the Delta table's current schema
     * @param newSchema  the new schema to update the table with
     * @return whether the new schema is compatible with the existing schema
     */
    public static boolean isWriteCompatible(StructType existingSchema, StructType newSchema) {
        return io.delta.standalone.internal.util.SchemaUtils.isWriteCompatible(
                existingSchema,
                newSchema);
    }
}
