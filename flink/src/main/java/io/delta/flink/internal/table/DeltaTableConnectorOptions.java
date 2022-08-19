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

package io.delta.flink.internal.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Options for the Flink TableAPI's DeltaSink connector.
 */
public class DeltaTableConnectorOptions {

    /**
     * Root path of the DeltaLake's table.
     */
    public static final ConfigOption<String> TABLE_PATH =
        ConfigOptions.key("table-path")
            .stringType()
            .noDefaultValue();

    /**
     * Path to folder with Hadoop Conf containing files such as:
     * <ul>
     *     <li>core-site.xml</li
     *     <li>hdfs-site.xml</li
     *     <li>yarn-site.xml</li
     *     <li>mapred-site.xml</li
     * </ul>
     */
    public static final ConfigOption<String> HADOOP_CONF_DIR =
        ConfigOptions.key("hadoop-conf-dir")
            .stringType()
            .noDefaultValue();

    /**
     * Indicator whether we should try to update table's schema with stream's schema in case
     * those will not match. The update is not guaranteed as there will be still some checks
     * performed whether the updates to the schema are compatible.
     */
    public static final ConfigOption<Boolean> MERGE_SCHEMA =
        ConfigOptions.key("mergeSchema")
            .booleanType()
            .defaultValue(false);
}
