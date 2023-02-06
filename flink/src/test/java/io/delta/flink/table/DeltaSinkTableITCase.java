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

package io.delta.flink.table;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import io.delta.flink.sink.utils.CheckpointCountingSource;
import io.delta.flink.sink.utils.CheckpointCountingSource.RowProducer;
import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.TestParquetReader;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.delta.flink.utils.DeltaTestUtils.buildCluster;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;

public class DeltaSinkTableITCase {

    private static final int PARALLELISM = 2;

    private static final Logger LOG = LoggerFactory.getLogger(DeltaSinkTableITCase.class);

    private static final String TEST_SOURCE_TABLE_NAME = "test_source_table";

    private static final String TEST_SINK_TABLE_NAME = "test_compact_sink_table";

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    public static final RowType TEST_ROW_TYPE = new RowType(Arrays.asList(
        new RowType.RowField("col1", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("col2", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("col3", new IntType())
    ));

    private static ExecutorService testWorkers;

    private final MiniClusterWithClientResource miniClusterResource = buildCluster(PARALLELISM);

    protected RowType testRowType;

    @BeforeAll
    public static void beforeAll() throws IOException {
        testWorkers = Executors.newCachedThreadPool(r -> {
            final Thread thread = new Thread(r);
            thread.setUncaughtExceptionHandler((t, e) -> {
                t.interrupt();
                throw new RuntimeException(e);
            });
            return thread;
        });
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        testWorkers.shutdownNow();
        TEMPORARY_FOLDER.delete();
    }

    @BeforeEach
    public void setUp() {
        try {
            miniClusterResource.before();
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @AfterEach
    public void afterEach() {
        miniClusterResource.after();
    }

    /**
     * @return Stream of test {@link Arguments} elements. Arguments are in order:
     * <ul>
     *     <li>isPartitioned</li>
     *     <li>includeOptionalOptions</li>
     *     <li>useStaticPartition</li>
     *     <li>useBoundedMode</li>
     * </ul>
     */
    private static Stream<Arguments> tableArguments() {
        return Stream.of(
            Arguments.of(false, false, false, false),
            Arguments.of(false, true, false, false),
            Arguments.of(true, false, false, false),
            Arguments.of(true, false, true, false),
            Arguments.of(false, false, false, true)
        );
    }

    @ParameterizedTest(
        name = "isPartitioned = {0}, " +
            "includeOptionalOptions = {1}, " +
            "useStaticPartition = {2}, " +
            "useBoundedMode = {3}")
    @MethodSource("tableArguments")
    public void testInsertQueryWithAllFields(
            boolean isPartitioned,
            boolean includeOptionalOptions,
            boolean useStaticPartition,
            boolean useBoundedMode) throws Exception {

        int expectedNumberOfRows = 20;
        String deltaTablePath = setupTestFolders(includeOptionalOptions);

        // Column `col1` would be a partition column if isPartitioned or useStaticPartition
        // set to true.
        // Column `col3` would be a partition column if isPartitioned is set to true.
        String insertSql = buildInsertAllFieldsSql(useStaticPartition);

        DeltaLog deltaLog =
            testTableJob(
                deltaTablePath,
                isPartitioned,
                includeOptionalOptions,
                useStaticPartition,
                useBoundedMode,
                insertSql,
                expectedNumberOfRows
            );

        Snapshot snapshot = deltaLog.update();

        // Validate that every inserted column has non value.
        int recordCount = 0;
        try (CloseableIterator<RowRecord> open = snapshot.open()) {
            while (open.hasNext()) {
                recordCount++;
                RowRecord record = open.next();
                assertThat(record.getString("col1"), notNullValue());
                assertThat(record.getString("col2"), notNullValue());
                assertThat(record.getInt("col3"), notNullValue());
            }
        }

        //
        assertThat(recordCount, equalTo(expectedNumberOfRows));
    }

    @ParameterizedTest(
        name = "isPartitioned = {0}, " +
            "includeOptionalOptions = {1}, " +
            "useStaticPartition = {2}, " +
            "useBoundedMode = {3}")
    @MethodSource("tableArguments")
    public void testInsertQueryWithOneFiled(
            boolean isPartitioned,
            boolean includeOptionalOptions,
            boolean useStaticPartition,
            boolean useBoundedMode) throws Exception {

        int expectedNumberOfRows = 20;

        // Column `col1` would be a partition column if isPartitioned or useStaticPartition
        // set to true.
        // Column `col3` would be a partition column if isPartitioned is set to true.
        String insertSql = buildInsertOneFieldSql(useStaticPartition);

        DeltaLog deltaLog =
            testTableJob(
                TEMPORARY_FOLDER.newFolder().getAbsolutePath(),
                isPartitioned,
                includeOptionalOptions,
                useStaticPartition,
                useBoundedMode,
                insertSql,
                expectedNumberOfRows
            );

        // Validate that every inserted column has null or not null value depends on the settings
        int recordCount = 0;
        try (CloseableIterator<RowRecord> open = deltaLog.update().open()) {
            while (open.hasNext()) {
                recordCount++;
                RowRecord record = open.next();
                assertThat(record.getString("col1"), notNullValue());
                assertThat(
                    record.getString("col2"),
                    (useStaticPartition) ? notNullValue() : nullValue()
                );
                assertThat(
                    record.isNullAt("col3"),
                    (isPartitioned) ? equalTo(false) : equalTo(true)
                );
            }
        }
        assertThat(recordCount, equalTo(expectedNumberOfRows));
    }

    @ParameterizedTest(
        name = "isPartitioned = {0}, " +
            "includeOptionalOptions = {1}, " +
            "useStaticPartition = {2}, " +
            "useBoundedMode = {3}")
    @MethodSource("tableArguments")
    public void testInsertQueryWithOneFiledWithNullCasts(
            boolean isPartitioned,
            boolean includeOptionalOptions,
            boolean useStaticPartition,
            boolean useBoundedMode) throws Exception {

        int expectedNumberOfRows = 20;

        // Column `col1` would be a partition column if isPartitioned or useStaticPartition
        // set to true.
        // Column `col3` would be a partition column if isPartitioned is set to true.
        String insertSql = buildInsertOneFieldSqlNullCasts(
            useStaticPartition,
            includeOptionalOptions
        );

        DeltaLog deltaLog =
            testTableJob(
                TEMPORARY_FOLDER.newFolder().getAbsolutePath(),
                isPartitioned,
                includeOptionalOptions,
                useStaticPartition,
                useBoundedMode,
                insertSql,
                expectedNumberOfRows
            );

        // Validate that every inserted column has null or not null value depends on the settings
        int recordCount = 0;
        try (CloseableIterator<RowRecord> open = deltaLog.update().open()) {
            while (open.hasNext()) {
                recordCount++;
                RowRecord record = open.next();
                assertThat(record.getString("col1"), notNullValue());
                assertThat(
                    record.getString("col2"),
                    (useStaticPartition) ? notNullValue() : nullValue()
                );
                assertThat(
                    record.isNullAt("col3"),
                    (isPartitioned) ? equalTo(false) : equalTo(true)
                );
            }
        }
        assertThat(recordCount, equalTo(expectedNumberOfRows));
    }

    private String buildInsertAllFieldsSql(boolean useStaticPartition) {

        if (useStaticPartition) {
            return String.format(
                "INSERT INTO %s PARTITION(col1='val1') SELECT col2, col3 FROM %s",
                DeltaSinkTableITCase.TEST_SINK_TABLE_NAME,
                DeltaSinkTableITCase.TEST_SOURCE_TABLE_NAME
            );
        }

        return String.format(
            "INSERT INTO %s SELECT * FROM %s",
            DeltaSinkTableITCase.TEST_SINK_TABLE_NAME,
            DeltaSinkTableITCase.TEST_SOURCE_TABLE_NAME
        );
    }

    private String buildInsertOneFieldSql(boolean useStaticPartition) {

        if (useStaticPartition) {
            return String.format(
                "INSERT INTO %s PARTITION(col1='val1') (col2) (SELECT col2 FROM %s)",
                DeltaSinkTableITCase.TEST_SINK_TABLE_NAME,
                DeltaSinkTableITCase.TEST_SOURCE_TABLE_NAME
            );
        }

        return String.format(
            "INSERT INTO %s (col1) (SELECT col1 FROM %s)",
            DeltaSinkTableITCase.TEST_SINK_TABLE_NAME,
            DeltaSinkTableITCase.TEST_SOURCE_TABLE_NAME
        );
    }

    private String buildInsertOneFieldSqlNullCasts(
        boolean useStaticPartition,
        boolean includeOptionalOptions) {

        if (useStaticPartition) {
            return String.format(
                "INSERT INTO %s PARTITION(col1='val1') (SELECT col2, cast(null as INT)"
                    + ((includeOptionalOptions) ? ", cast(null as INT) " : "")
                    + " FROM %s)",
                DeltaSinkTableITCase.TEST_SINK_TABLE_NAME,
                DeltaSinkTableITCase.TEST_SOURCE_TABLE_NAME
            );
        }

        return String.format(
            "INSERT INTO %s (SELECT col1, cast(null as VARCHAR), cast(null as INT) "
                + ((includeOptionalOptions) ? ", cast(null as INT) " : "")
                + "FROM %s)",
            DeltaSinkTableITCase.TEST_SINK_TABLE_NAME,
            DeltaSinkTableITCase.TEST_SOURCE_TABLE_NAME
        );
    }

    private DeltaLog testTableJob(
            String deltaTablePath,
            boolean isPartitioned,
            boolean includeOptionalOptions,
            boolean useStaticPartition,
            boolean useBoundedMode,
            String insertSql,
            int expectedNumberOfRows) throws Exception {

        // GIVEN
        DeltaLog deltaLog = DeltaLog.forTable(DeltaTestUtils.getHadoopConf(), deltaTablePath);
        List<AddFile> initialDeltaFiles = deltaLog.snapshot().getAllFiles();

        // WHEN
        runFlinkJob(
            deltaTablePath,
            useBoundedMode,
            includeOptionalOptions,
            isPartitioned,
            insertSql,
            expectedNumberOfRows);

        DeltaTestUtils.waitUntilDeltaLogExists(deltaLog, deltaLog.snapshot().getVersion() + 1);

        // THEN
        validateTargetTable(
            isPartitioned,
            includeOptionalOptions,
            useStaticPartition,
            deltaLog,
            initialDeltaFiles,
            expectedNumberOfRows
        );

        return deltaLog;
    }

    @SuppressWarnings("unchecked")
    private void validateTargetTable(
            boolean isPartitioned,
            boolean includeOptionalOptions,
            boolean useStaticPartition,
            DeltaLog deltaLog,
            List<AddFile> initialDeltaFiles,
            int expectedRecordCount) throws IOException {

        int tableRecordsCount =
            TestParquetReader.readAndValidateAllTableRecords(
                deltaLog,
                TEST_ROW_TYPE,
                DataFormatConverters.getConverterForDataType(
                    TypeConversions.fromLogicalToDataType(TEST_ROW_TYPE))
            );

        Snapshot snapshot = deltaLog.update();
        List<AddFile> files = snapshot.getAllFiles();
        assertThat(files.size() > initialDeltaFiles.size(), equalTo(true));
        assertThat(tableRecordsCount, equalTo(expectedRecordCount));

        if (isPartitioned) {
            assertThat(
                deltaLog.snapshot().getMetadata().getPartitionColumns(),
                CoreMatchers.is(Arrays.asList("col1", "col3"))
            );
        } else {
            assertThat(
                deltaLog.snapshot().getMetadata().getPartitionColumns().isEmpty(),
                equalTo(true)
            );
        }

        List<String> expectedTableCols = includeOptionalOptions ?
            Arrays.asList("col1", "col2", "col3", "col4") : Arrays.asList("col1", "col2", "col3");
        assertThat(
            Arrays.asList(deltaLog.snapshot().getMetadata().getSchema().getFieldNames()),
            CoreMatchers.is(expectedTableCols));

        if (useStaticPartition) {
            for (AddFile file : deltaLog.snapshot().getAllFiles()) {
                assertThat(file.getPartitionValues().get("col1"), equalTo("val1"));
            }
        }
    }

    public String setupTestFolders(boolean includeOptionalOptions) {
        try {
            String deltaTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            // one of the optional options is whether the sink should try to update the table's
            // schema, so we are initializing an existing table to test this behaviour
            if (includeOptionalOptions) {
                testRowType = DeltaSinkTestUtils.addNewColumnToSchema(TEST_ROW_TYPE);
            } else {
                testRowType = TEST_ROW_TYPE;
            }
            return deltaTablePath;
        } catch (IOException e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    /**
     * Run Flink Job and block current thread until job finishes.
     */
    private void runFlinkJob(
            String deltaTablePath,
            boolean useBoundedMode,
            boolean includeOptionalOptions,
            boolean isPartitioned,
            String insertSql,
            int expectedNumberOfRows) {

        StreamExecutionEnvironment streamEnv = getTestStreamEnv(!useBoundedMode);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        if (useBoundedMode) {
            // will use datagen for Source Table
            String sourceSql = buildSourceTableSql(expectedNumberOfRows, includeOptionalOptions);
            tableEnv.executeSql(sourceSql);
        } else {
            // Since Delta Sink's Global Committer lags 1 commit behind rest of the pipeline,
            // in Streaming mode we cannot use datagen since it will end just after emitting last
            // record, and we will need extra Flink commit to commit this last records in Delta
            // Log. Because of that we are using CheckpointCountingSource which will wait extra
            // one commit after emitting entire data set. CheckpointCountingSource can't be used
            // in bounded mode though.
            CheckpointCountingSource source;
            int recordsPerCheckpoint = 5;
            int numberOfCheckpoints =
                (int) Math.ceil(expectedNumberOfRows / (double) recordsPerCheckpoint);
            if (includeOptionalOptions) {
                source =
                    new CheckpointCountingSource(
                        recordsPerCheckpoint,
                        numberOfCheckpoints,
                        new ExtraColumnRowProducer());
            } else {
                source =
                    new CheckpointCountingSource(
                        recordsPerCheckpoint,
                        numberOfCheckpoints,
                        new RowTypeColumnarRowProducer());
            }

            DataStreamSource<RowData> streamSource = streamEnv.addSource(source).setParallelism(1);
            tableEnv.createTemporaryView(TEST_SOURCE_TABLE_NAME, streamSource);
        }

        String sinkSql = buildSinkTableSql(deltaTablePath, includeOptionalOptions, isPartitioned);
        tableEnv.executeSql(sinkSql);

        try {
            tableEnv.executeSql(insertSql).await();
        } catch (Exception e) {
            if (!e.getMessage().contains("Failed to wait job finish")) {
                throw new RuntimeException(e);
            }
        }
    }

    private StreamExecutionEnvironment getTestStreamEnv(boolean streamingMode) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

        if (streamingMode) {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }

        return env;
    }

    private String buildSourceTableSql(int rows, boolean includeOptionalOptions) {

        String additionalCol = includeOptionalOptions ? ", col4 INT " : "";
        return String.format(
            "CREATE TABLE %s ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + additionalCol
                + ") WITH ("
                + " 'connector' = 'datagen',"
                + "'number-of-rows' = '%s',"
                + " 'rows-per-second' = '5'"
                + ")",
            DeltaSinkTableITCase.TEST_SOURCE_TABLE_NAME,
            rows);
    }

    private String buildSinkTableSql(
            String tablePath,
            boolean includeOptionalOptions,
            boolean isPartitioned) {

        String optionalTableOptions = (includeOptionalOptions ? "'mergeSchema' = 'true', " : "");

        String partitionedClause = isPartitioned ? "PARTITIONED BY (col1, col3) " : "";
        String additionalCol = includeOptionalOptions ? ", col4 INT " : "";

        return String.format(
            "CREATE TABLE %s ("
                + " col1 VARCHAR,"
                + " col2 VARCHAR,"
                + " col3 INT"
                + additionalCol
                + ") "
                + partitionedClause
                + "WITH ("
                + " 'connector' = 'delta',"
                + optionalTableOptions
                + " 'table-path' = '%s'"
                + ")",
            DeltaSinkTableITCase.TEST_SINK_TABLE_NAME, tablePath);
    }

    private static class RowTypeColumnarRowProducer implements RowProducer {

        @SuppressWarnings("unchecked")
        private static final DataFormatConverters.DataFormatConverter<RowData, Row>
            ROW_TYPE_CONVERTER = DataFormatConverters.getConverterForDataType(
            TypeConversions.fromLogicalToDataType(TEST_ROW_TYPE)
        );

        @Override
        public int emitRecordsBatch(int nextValue, SourceContext<RowData> ctx, int batchSize) {
            for (int i = 0; i < batchSize; ++i) {
                RowData row = ROW_TYPE_CONVERTER.toInternal(
                    Row.of(
                        String.valueOf(nextValue),
                        String.valueOf((nextValue + nextValue)),
                        nextValue
                    )
                );
                ctx.collect(row);
                nextValue++;
            }

            return nextValue;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            LogicalType[] fieldTypes = TEST_ROW_TYPE.getFields().stream()
                .map(RowField::getType).toArray(LogicalType[]::new);
            String[] fieldNames = TEST_ROW_TYPE.getFieldNames().toArray(new String[0]);
            return InternalTypeInfo.of(RowType.of(fieldTypes, fieldNames));
        }
    }

    private static class ExtraColumnRowProducer implements RowProducer {

        private static final RowType EXTRA_COLUMN_ROW_TYPE = new RowType(Arrays.asList(
            new RowType.RowField("col1", new VarCharType(VarCharType.MAX_LENGTH)),
            new RowType.RowField("col2", new VarCharType(VarCharType.MAX_LENGTH)),
            new RowType.RowField("col3", new IntType()),
            new RowType.RowField("col4", new IntType())
        ));

        @SuppressWarnings("unchecked")
        private static final DataFormatConverters.DataFormatConverter<RowData, Row>
            EXTRA_COLUMN_ROW_TYPE_CONVERTER = DataFormatConverters.getConverterForDataType(
            TypeConversions.fromLogicalToDataType(EXTRA_COLUMN_ROW_TYPE)
        );


        @Override
        public int emitRecordsBatch(int nextValue, SourceContext<RowData> ctx, int batchSize) {
            for (int i = 0; i < batchSize; ++i) {
                RowData row = EXTRA_COLUMN_ROW_TYPE_CONVERTER.toInternal(
                    Row.of(
                        String.valueOf(nextValue),
                        String.valueOf((nextValue + nextValue)),
                        nextValue,
                        nextValue
                    )
                );
                ctx.collect(row);
                nextValue++;
            }
            return nextValue;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            LogicalType[] fieldTypes = EXTRA_COLUMN_ROW_TYPE.getFields().stream()
                .map(RowField::getType).toArray(LogicalType[]::new);
            String[] fieldNames = EXTRA_COLUMN_ROW_TYPE.getFieldNames().toArray(new String[0]);
            return InternalTypeInfo.of(RowType.of(fieldTypes, fieldNames));
        }
    }
}
