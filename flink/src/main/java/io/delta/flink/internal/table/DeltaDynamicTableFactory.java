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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

/**
 * Creates a {@link DynamicTableSink} and {@link DynamicTableSource} instance representing DeltaLake
 * table.
 *
 * <p>
 * This implementation automatically resolves all necessary object for creating instance of {@link
 * io.delta.flink.sink.DeltaSink} and {@link io.delta.flink.source.DeltaSource} except Delta table's
 * path that needs to be provided explicitly.
 */
public class DeltaDynamicTableFactory implements DynamicTableSinkFactory,
    DynamicTableSourceFactory {

    public static final String IDENTIFIER = "delta";

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

        Configuration conf =
            HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());

        RowType rowType = (RowType) tableSchema.toPhysicalRowDataType().getLogicalType();

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
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper =
            FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig tableOptions = helper.getOptions();
        Configuration hadoopConf =
            HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());

        List<String> columns = ((RowType) context
            .getCatalogTable()
            .getResolvedSchema()
            .toPhysicalRowDataType()
            .getLogicalType()
        ).getFieldNames();

        return new DeltaDynamicTableSource(
            hadoopConf,
            tableOptions,
            columns
        );
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DeltaTableConnectorOptions.TABLE_PATH);
        return options;
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
        options.add(DeltaTableConnectorOptions.MERGE_SCHEMA);

        // TODO With Delta Catalog, this option will be injected only through query hints.
        //  The DDL validation will be done in Delta Catalog during "createTable" operation.
        options.add(DeltaFlinkJobSpecificOptions.MODE);
        return options;
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
