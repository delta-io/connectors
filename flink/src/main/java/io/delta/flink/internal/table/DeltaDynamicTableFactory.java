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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

/**
 * Creates a {@link DynamicTableSink} instance representing DeltaLake table.
 *
 * <p>
 * This implementation automatically resolves all necessary object for creating instance of {@link
 * io.delta.flink.sink.DeltaSink} except Delta table's path that needs to be provided explicitly.
 */
public class DeltaDynamicTableFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "delta";

    public final org.apache.flink.configuration.Configuration emptyClusterConfig =
        new org.apache.flink.configuration.Configuration();

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {

        FactoryUtil.TableFactoryHelper helper =
            FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig tableOptions = helper.getOptions();
        ResolvedSchema tableSchema = context.getCatalogTable().getResolvedSchema();

        Configuration conf = resolveHadoopConf(tableOptions);

        RowType rowType = (RowType) tableSchema.toSinkRowDataType().getLogicalType();

        Boolean shouldTryUpdateSchema = tableOptions
            .getOptional(DeltaTableConnectorOptions.MERGE_SCHEMA)
            .orElse(DeltaTableConnectorOptions.MERGE_SCHEMA.defaultValue());

        return new DeltaDynamicTableSink(
            new Path(tableOptions.get(DeltaTableConnectorOptions.TABLE_PATH)),
            conf,
            rowType,
            shouldTryUpdateSchema,
            context.getCatalogTable()
        );
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DeltaTableConnectorOptions.TABLE_PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DeltaTableConnectorOptions.HADOOP_CONF_DIR);
        options.add(DeltaTableConnectorOptions.MERGE_SCHEMA);
        return options;
    }

    /**
     * Tries to resolve Hadoop conf from conf dir provided as table option or from environment
     * variable HADOOP_HOME and HADOOP_CONF_DIR.
     * <p>
     * The configuration will be resolved in below order, where every next element will override
     * configuration loaded from previous one.
     * <ul>
     *     <li>HADOOP_HOME environment variable</li>
     *     <li>HADOOP_CONF_DIR environment variable</li>
     *     <li>"hadoop-conf-dir" table property</li>
     * </ul>
     *
     * @param tableOptions Flink Table's options resolved for given table
     * @return {@link Configuration} object
     */
    private Configuration resolveHadoopConf(ReadableConfig tableOptions) {

        Configuration userHadoopConf = null;
        Optional<String> hadoopConfDirOptional =
            tableOptions.getOptional(DeltaTableConnectorOptions.HADOOP_CONF_DIR);

        if (hadoopConfDirOptional.isPresent()) {
            userHadoopConf = loadHadoopConfFromFolder(hadoopConfDirOptional.get());
            if (userHadoopConf == null) {
                throw new RuntimeException(
                    "Failed to resolve Hadoop userHadoopConf file from given path",
                    new FileNotFoundException(
                        "Couldn't resolve Hadoop userHadoopConf at given path: " +
                            hadoopConfDirOptional.get()));
            }
        }

        // We are using helper method HadoopUtils.getHadoopConfiguration to resolve
        // cluster's Hadoop configuration. This method looks for Hadoop config in env variables and
        // Flink cluster configuration. For this moment DynamicTableSinkFactory does not have
        // access to Flink's configuration that is why we are passing "dummy" config as an argument.
        Configuration hadoopConfiguration = HadoopUtils.getHadoopConfiguration(emptyClusterConfig);
        if (userHadoopConf != null) {
            hadoopConfiguration.addResource(userHadoopConf);
        }

        return hadoopConfiguration;
    }

    /**
     * Returns a new Hadoop Configuration object using the path to the hadoop conf configured or
     * null if path does not exist.
     *
     * @param hadoopConfDir Hadoop conf directory path.
     * @return A Hadoop configuration instance or null if path does not exist.
     */
    private Configuration loadHadoopConfFromFolder(String hadoopConfDir) {
        if (new File(hadoopConfDir).exists()) {
            List<File> possibleConfFiles = new ArrayList<>();
            List<String> possibleConfigs = Arrays.asList(
                "core-site.xml",
                "hdfs-site.xml",
                "yarn-site.xml",
                "mapred-site.xml"
            );
            for (String confPath : possibleConfigs) {
                File confFile = new File(hadoopConfDir, confPath);
                if (confFile.exists()) {
                    possibleConfFiles.add(confFile);
                }
            }

            if (!possibleConfFiles.isEmpty()) {
                Configuration hadoopConfiguration = new Configuration();
                for (File confFile : possibleConfFiles) {
                    hadoopConfiguration.addResource(
                        new org.apache.hadoop.fs.Path(confFile.getAbsolutePath()));
                }
                return hadoopConfiguration;
            }
        }
        return null;
    }
}
