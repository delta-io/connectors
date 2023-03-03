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

package io.delta.flink.table.it.suite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.delta.flink.internal.options.DeltaOptionValidationException;
import io.delta.flink.internal.table.DeltaFlinkJobSpecificOptions.QueryMode;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.ExecutionITCaseTestConstants;
import io.delta.flink.utils.FailoverType;
import io.delta.flink.utils.RecordCounterToFail.FailCheck;
import io.delta.flink.utils.TableUpdateDescriptor;
import io.delta.flink.utils.TestDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.rules.TemporaryFolder;
import static io.delta.flink.utils.DeltaTestUtils.buildCluster;
import static io.delta.flink.utils.DeltaTestUtils.getTestStreamEnv;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.AGE_COLUMN_VALUES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.DATA_COLUMN_NAMES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.DATA_COLUMN_TYPES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.NAME_COLUMN_VALUES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.SMALL_TABLE_COUNT;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.SURNAME_COLUMN_VALUES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

// TODO DC, FlinkSQL_PR_8 - This test class is almost moved to table_feature_branch
//  with an exception of one test (tagged as `to-do'). Update feature branch if any
//  new test is added here.
public abstract class DeltaSourceTableTestSuite {

    private static final int PARALLELISM = 2;

    private static final String TEST_SOURCE_TABLE_NAME = "sourceTable";

    private static final String SMALL_TABLE_SCHEMA = "name VARCHAR, surname VARCHAR, age INT";

    private static final String LARGE_TABLE_SCHEMA = "col1 BIGINT, col2 BIGINT, col3 VARCHAR";

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private final MiniClusterWithClientResource miniClusterResource = buildCluster(PARALLELISM);

    /**
     * Schema for this table has only {@link ExecutionITCaseTestConstants#DATA_COLUMN_NAMES} of type
     * {@link ExecutionITCaseTestConstants#DATA_COLUMN_TYPES} columns.
     */
    private String nonPartitionedTablePath;

    // TODO would have been nice to make a TableInfo class that contained the path (maybe a
    //  generator so it is always random), column names, column types, so all this information
    //  was coupled together. This class could be used for all IT tests where we use predefined
    //  Tables - https://github.com/delta-io/connectors/issues/499
    /**
     * Schema for this table has only
     * {@link ExecutionITCaseTestConstants#LARGE_TABLE_ALL_COLUMN_NAMES} of type
     * {@link ExecutionITCaseTestConstants#LARGE_TABLE_ALL_COLUMN_TYPES} columns.
     * Column types are long, long, String
     */
    private String nonPartitionedLargeTablePath;

    @BeforeAll
    public static void beforeAll() throws IOException {
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TEMPORARY_FOLDER.delete();
    }

    public static void assertNoMoreColumns(List<Row> resultData, int extraColumnIndex) {
        resultData.forEach(rowData ->
            assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> rowData.getField(extraColumnIndex),
                "Found row with extra column."
            )
        );
    }

    @BeforeEach
    public void setUp() {
        try {
            miniClusterResource.before();
            nonPartitionedTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            nonPartitionedLargeTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

            DeltaTestUtils.initTestForNonPartitionedTable(nonPartitionedTablePath);
            DeltaTestUtils.initTestForNonPartitionedLargeTable(nonPartitionedLargeTablePath);
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @AfterEach
    public void afterEach() {
        miniClusterResource.after();
    }

    /**
     * Flink by design does not allow using Streaming sources in Batch environment. This tests
     * verifies if source created by streaming query is in fact Continuous/Streaming source.
     */
    @Test
    public void testThrowIfUsingStreamingSourceInBatchEnv() {

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(false) // streamingMode = false
        );

        setupDeltaCatalog(tableEnv);

        // CREATE Source TABLE
        tableEnv.executeSql(
            buildSourceTableSql(nonPartitionedTablePath, SMALL_TABLE_SCHEMA)
        );

        String selectSql = "SELECT * FROM sourceTable /*+ OPTIONS('mode' = 'streaming') */";

        RuntimeException exception =
            assertThrows(RuntimeException.class, () -> tableEnv.executeSql(selectSql));

        assertThat(exception.getMessage())
            .contains(
                "Querying an unbounded table",
                "in batch mode is not allowed. The table source is unbounded."
            );
    }

    /**
     * Flink allows using bounded sources in streaming environment. This tests verifies if source
     * created by simple SELECT statement tha should be backed by bounded source can be run in
     * streaming environment.
     */
    @Test
    public void testUsingBatchSourceInStreamingEnv() throws Exception {

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(true) // streamingMode = true
        );

        setupDeltaCatalog(tableEnv);

        // CREATE Source TABLE
        tableEnv.executeSql(
            buildSourceTableSql(nonPartitionedTablePath, SMALL_TABLE_SCHEMA)
        );

        String selectSql = "SELECT * FROM sourceTable";

        // WHEN
        TableResult tableResult = tableEnv.executeSql(selectSql);

        // THEN
        List<Row> results = new ArrayList<>();
        tableResult.await(10, TimeUnit.SECONDS);
        try (CloseableIterator<Row> collect = tableResult.collect()) {
            while (collect.hasNext()) {
                results.add(collect.next());
            }
        }

        // A rough assertion on an actual data. Full assertions are done in other tests.
        assertThat(results).hasSize(2);
    }

    @ParameterizedTest(name = "mode = {0}")
    @ValueSource(strings = {"", "batch", "BATCH", "baTCH"})
    public void testBatchTableJob(String jobMode) throws Exception {

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(false) // streamingMode = false
        );

        setupDeltaCatalog(tableEnv);

        // CREATE Source TABLE
        tableEnv.executeSql(
            buildSourceTableSql(nonPartitionedTablePath, SMALL_TABLE_SCHEMA)
        );

        String connectorModeHint = StringUtils.isNullOrWhitespaceOnly(jobMode) ?
            "" : String.format("/*+ OPTIONS('mode' = '%s') */", jobMode);

        String selectSql = String.format("SELECT * FROM sourceTable %s", connectorModeHint);

        TableResult resultTable = tableEnv.executeSql(selectSql);

        List<Row> resultData = new ArrayList<>();
        try (CloseableIterator<Row> collect = resultTable.collect()) {
            while (collect.hasNext()) {
                resultData.add(collect.next());
            }
        }

        List<String> readNames =
            resultData.stream()
                .map(row -> row.getFieldAs(0).toString()).collect(Collectors.toList());

        Set<String> readSurnames =
            resultData.stream()
                .map(row -> row.getFieldAs(1).toString())
                .collect(Collectors.toSet());

        Set<Integer> readAge =
            resultData.stream().map(row -> (int) row.getFieldAs(2)).collect(Collectors.toSet());

        // THEN
        assertThat(resultData)
            .withFailMessage("Source read different number of rows that Delta Table have."
                + "\nExpected: %d,\nActual: %d", SMALL_TABLE_COUNT, resultData.size())
            .hasSize(SMALL_TABLE_COUNT);

        // check for column values
        assertThat(readNames)
            .withFailMessage("Source produced different values for [name] column")
            .containsExactlyElementsOf(NAME_COLUMN_VALUES);

        assertThat(readSurnames)
            .withFailMessage("Source produced different values for [surname] column")
            .containsExactlyElementsOf(SURNAME_COLUMN_VALUES);

        assertThat(readAge)
            .withFailMessage("Source produced different values for [age] column")
            .containsExactlyElementsOf(AGE_COLUMN_VALUES);

        // Checking that we don't have more columns.
        assertNoMoreColumns(resultData, 3);
    }

    @ParameterizedTest(name = "mode = {0}")
    @ValueSource(strings = {"streaming", "STREAMING", "streamING"})
    public void testStreamingTableJob(String jobMode) throws Exception {

        int numberOfTableUpdateBulks = 5;
        int rowsPerTableUpdate = 5;
        int initialTableSize = 2;

        TestDescriptor testDescriptor = DeltaTestUtils.prepareTableUpdates(
            nonPartitionedTablePath,
            RowType.of(DATA_COLUMN_TYPES, DATA_COLUMN_NAMES),
            initialTableSize,
            new TableUpdateDescriptor(numberOfTableUpdateBulks, rowsPerTableUpdate)
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(true) // streamingMode = true
        );

        setupDeltaCatalog(tableEnv);

        // CREATE Source TABLE
        tableEnv.executeSql(
            buildSourceTableSql(nonPartitionedTablePath, SMALL_TABLE_SCHEMA)
        );

        String connectorModeHint = StringUtils.isNullOrWhitespaceOnly(jobMode) ?
            "" : String.format("/*+ OPTIONS('mode' = '%s') */", jobMode);

        String selectSql = String.format("SELECT * FROM sourceTable %s", connectorModeHint);

        Table resultTable = tableEnv.sqlQuery(selectSql);

        DataStream<Row> rowDataStream = tableEnv.toDataStream(resultTable);

        List<List<Row>> resultData = DeltaTestUtils.testContinuousStream(
            FailoverType.NONE,
            testDescriptor,
            (FailCheck) readRows -> true,
            rowDataStream,
            miniClusterResource
        );

        int totalNumberOfRows = resultData.stream().mapToInt(List::size).sum();

        // Each row has a unique column across all Delta table data. We are converting List or
        // read rows to set of values for that unique column.
        // If there were any duplicates or missing values we will catch them here by comparing
        // size of that Set to expected number of rows.
        Set<String> uniqueValues =
            resultData.stream().flatMap(Collection::stream)
                .map(row -> row.getFieldAs(1).toString())
                .collect(Collectors.toSet());

        // THEN
        assertThat(totalNumberOfRows)
            .withFailMessage("Source read different number of rows that Delta Table have.")
            .isEqualTo(initialTableSize + numberOfTableUpdateBulks * rowsPerTableUpdate);

        assertThat(uniqueValues)
            .withFailMessage("Source Produced Different Rows that were in Delta Table")
            .hasSize(initialTableSize + numberOfTableUpdateBulks * rowsPerTableUpdate);
    }

    @Test
    public void testSelectWithWhereFilter() throws Exception {

        // GIVEN
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(false) // streamingMode = false
        );

        setupDeltaCatalog(tableEnv);

        // CREATE Source TABLE
        tableEnv.executeSql(
            buildSourceTableSql(nonPartitionedLargeTablePath, LARGE_TABLE_SCHEMA)
        );

        // WHEN
        String selectSql = "SELECT * FROM sourceTable WHERE col1 > 500";

        Table resultTable = tableEnv.sqlQuery(selectSql);

        DataStream<Row> rowDataStream = tableEnv.toDataStream(resultTable);

        List<Row> resultData = DeltaTestUtils.testBoundedStream(rowDataStream, miniClusterResource);

        // THEN
        List<Long> readCol1Values =
            resultData.stream()
                .map(row -> (long) row.getFieldAs(0))
                .sorted()
                .collect(Collectors.toList());

        // THEN
        // The table that we read has 1100 records, where col1 with sequence value from 0 to 1099.
        // the WHERE query filters all records with col1 <= 500, so we expect 599 records
        // produced by SELECT query.
        assertThat(resultData)
            .withFailMessage("SELECT with WHERE read different number of rows that expected.")
            .hasSize(599);

        assertThat(readCol1Values)
            .withFailMessage("SELECT with WHERE read different unique values for column col1.")
            .hasSize(599);

        assertThat(readCol1Values.get(0)).isEqualTo(501L);
        assertThat(readCol1Values.get(readCol1Values.size() - 1)).isEqualTo(1099L);

        // Checking that we don't have more columns.
        assertNoMoreColumns(resultData, 3);
    }

    // TODO FlinkSQL_PR_8
    @ParameterizedTest(name = "mode = {0}")
    @ValueSource(strings = {"batch", "streaming"})
    public void testThrowOnInvalidQueryHints(String queryMode) {

        StreamExecutionEnvironment testStreamEnv =
            QueryMode.BATCH.name().equals(queryMode) ? getTestStreamEnv(false)
                : getTestStreamEnv(true);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(testStreamEnv);

        setupDeltaCatalog(tableEnv);

        String invalidQueryHints = String.format(""
            + "'spark.some.option' = '10',"
            + "'delta.logStore' = 'someValue',"
            + "'io.delta.storage.S3DynamoDBLogStore.ddb.region' = 'Poland',"
            + "'parquet.writer.max-padding' = '10',"
            + "'delta.appendOnly' = 'true',"
            + "'customOption' = 'value',"
            + "'%s' = '10'", DeltaSourceOptions.VERSION_AS_OF.key());

        // CREATE Source TABLE
        tableEnv.executeSql(
            buildSourceTableSql(nonPartitionedTablePath, SMALL_TABLE_SCHEMA)
        );

        String selectSql =
            String.format("SELECT * FROM sourceTable /*+ OPTIONS(%s) */", invalidQueryHints);

        ValidationException exception =
            assertThrows(ValidationException.class, () -> tableEnv.executeSql(selectSql));

        assertThat(exception.getCause().getMessage())
            .isEqualTo(""
                + "Only job specific options are allowed in SELECT SQL statement.\n"
                + "Invalid options used: \n"
                + " - 'delta.appendOnly'\n"
                + " - 'spark.some.option'\n"
                + " - 'delta.logStore'\n"
                + " - 'customOption'\n"
                + " - 'io.delta.storage.S3DynamoDBLogStore.ddb.region'\n"
                + " - 'parquet.writer.max-padding'\n"
                + "Allowed options:\n"
                + " - 'mode'\n"
                + " - 'startingTimestamp'\n"
                + " - 'ignoreDeletes'\n"
                + " - 'updateCheckIntervalMillis'\n"
                + " - 'startingVersion'\n"
                + " - 'ignoreChanges'\n"
                + " - 'versionAsOf'\n"
                + " - 'updateCheckDelayMillis'\n"
                + " - 'timestampAsOf'");
    }

    // TODO FlinkSQL_PR_8
    @ParameterizedTest(name = "queryHint = {0}")
    @ValueSource(
        strings = {
            "'versionAsOf' = '10', 'timestampAsOf' = '2022-02-24T04:55:00.001', 'mode' = 'batch'",
            "'startingVersion' = '10', 'startingTimestamp' = '2022-02-24T04:55:00.001', 'mode' = "
                + "'streaming'"
        })
    public void testThrowOnMutuallyExcludedQueryHints(String queryHints) {

        StreamExecutionEnvironment testStreamEnv =
            queryHints.contains(QueryMode.BATCH.name()) ? getTestStreamEnv(false)
                : getTestStreamEnv(true);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(testStreamEnv);

        setupDeltaCatalog(tableEnv);

        // CREATE Source TABLE
        tableEnv.executeSql(
            buildSourceTableSql(nonPartitionedTablePath, SMALL_TABLE_SCHEMA)
        );

        String selectSql =
            String.format("SELECT * FROM sourceTable /*+ OPTIONS(%s) */", queryHints);

        DeltaOptionValidationException exception =
            assertThrows(
                DeltaOptionValidationException.class, () -> tableEnv.executeSql(selectSql));

        assertThat(exception.getMessage())
            .contains("Used mutually exclusive options for Source definition.");
    }

    // TODO FlinkSQL_PR_8
    @ParameterizedTest(name = "queryHint = {0}")
    @ValueSource(
        strings = {
            "'versionAsOf' = '10', 'mode' = 'streaming'",
            "'timestampAsOf' = '2022-02-24T04:55:00.001', 'mode' = 'streaming'",
            "'startingVersion' = '10', 'mode' = 'batch'",
            "'startingTimestamp' = '2022-02-24T04:55:00.001', 'mode' = 'batch'"
        })
    public void testThrowWhenInvalidOptionForMode(String queryHints) {

        StreamExecutionEnvironment testStreamEnv =
            queryHints.contains(QueryMode.BATCH.name()) ? getTestStreamEnv(false)
                : getTestStreamEnv(true);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(testStreamEnv);

        setupDeltaCatalog(tableEnv);

        // CREATE Source TABLE
        tableEnv.executeSql(
            buildSourceTableSql(nonPartitionedTablePath, SMALL_TABLE_SCHEMA)
        );

        String selectSql =
            String.format("SELECT * FROM sourceTable /*+ OPTIONS(%s) */", queryHints);

        DeltaOptionValidationException exception =
            assertThrows(
                DeltaOptionValidationException.class, () -> tableEnv.executeSql(selectSql));

        assertThat(exception.getMessage())
            .contains("Used inapplicable option for source configuration.");
    }

    // TODO FlinkSQL_PR_8
    @Test
    public void testJobSpecificOptionInBatch() throws Exception {

        // GIVEN
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(false)
        );

        setupDeltaCatalog(tableEnv);

        // CREATE Source TABLE
        tableEnv.executeSql(
            buildSourceTableSql(nonPartitionedLargeTablePath, LARGE_TABLE_SCHEMA)
        );

        // versionAsOf = 1 query hint.
        String versionAsOf_1 = String.format("`%s` = '1'", DeltaSourceOptions.VERSION_AS_OF.key());

        // versionAsOf = 5 query hint.
        String versionAsOf_5 = String.format("`%s` = '5'", DeltaSourceOptions.VERSION_AS_OF.key());

        // WHEN
        TableResult tableResultHint1 = tableEnv.executeSql(
            String.format("SELECT * FROM sourceTable /*+ OPTIONS(%s) */ ORDER BY col1",
                versionAsOf_1)
        );
        TableResult tableResultHint2 = tableEnv.executeSql(
            String.format("SELECT * FROM sourceTable /*+ OPTIONS(%s) */ ORDER BY col1",
                versionAsOf_5)
        );

        // THEN
        assertVersionAsOfResult(tableResultHint1, 200);
        assertVersionAsOfResult(tableResultHint2, 600);
    }

    /**
     * This method assumes that tableResult has sorted data (ORDER BY was used).
     */
    private void assertVersionAsOfResult(TableResult tableResult, int expectedRowCount)
        throws Exception {

        try (CloseableIterator<Row> collect = tableResult.collect()) {
            int rowCount = 0;
            long minCol1Value = 0;
            long maxCol1Value = 0;
            while (collect.hasNext()) {
                rowCount++;
                Row row = collect.next();
                long col1Val = row.getFieldAs("col1");

                if (minCol1Value > col1Val) {
                    minCol1Value = col1Val;
                    continue;
                }

                if (maxCol1Value < col1Val) {
                    maxCol1Value = col1Val;
                }
            }

            assertThat(rowCount)
                .withFailMessage(
                    "Query produced different number of rows than expected."
                        + "\nExpected: %d\nActual: %d", expectedRowCount, rowCount)
                .isEqualTo(expectedRowCount);

            assertThat(minCol1Value)
                .withFailMessage("Query produced different min value for col1."
                    + "\nExpected: %dnActual: %d", 0, minCol1Value)
                .isEqualTo(0);

            // It is expected that col1 for table used in test will have sequence values from 0
            // and + 1 for every row.
            assertThat(maxCol1Value)
                .withFailMessage("Query produced different max value for col1."
                    + "\nExpected: %dnActual: %d", expectedRowCount - 1, maxCol1Value)
                .isEqualTo(expectedRowCount - 1);
        }
    }

    private String buildSourceTableSql(String tablePath, String schemaString) {

        return String.format(
            "CREATE TABLE %s ("
                + schemaString
                + ") "
                + "WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            DeltaSourceTableTestSuite.TEST_SOURCE_TABLE_NAME,
            tablePath
        );
    }

    public abstract void setupDeltaCatalog(TableEnvironment tableEnv);
}
