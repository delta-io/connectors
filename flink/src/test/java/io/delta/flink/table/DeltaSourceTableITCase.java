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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.ExecutionITCaseTestConstants;
import io.delta.flink.utils.FailoverType;
import io.delta.flink.utils.RecordCounterToFail.FailCheck;
import io.delta.flink.utils.TableUpdateDescriptor;
import io.delta.flink.utils.TestDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.StringUtils;
import org.hamcrest.CoreMatchers;
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DeltaSourceTableITCase {

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

    @ParameterizedTest
    @ValueSource(strings = {"", "batch", "BATCH", "baTCH"})
    public void testBatchTableJob(String jobMode) throws Exception {

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(false) // streamingMode = false
        );

        // CREATE Source TABLE
        tableEnv.executeSql(
            buildSourceTableSql(nonPartitionedTablePath, SMALL_TABLE_SCHEMA)
        );

        String connectorModeHint = StringUtils.isNullOrWhitespaceOnly(jobMode) ?
            "" : String.format("/*+ OPTIONS('mode' = '%s') */", jobMode);

        String selectSql = String.format("SELECT * FROM sourceTable %s", connectorModeHint);

        Table resultTable = tableEnv.sqlQuery(selectSql);

        DataStream<Row> rowDataStream = tableEnv.toDataStream(resultTable);

        List<Row> resultData = DeltaTestUtils.testBoundedStream(rowDataStream, miniClusterResource);

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
        assertThat("Source read different number of rows that Delta Table have.",
            resultData.size(),
            equalTo(SMALL_TABLE_COUNT));

        // check for column values
        assertThat("Source produced different values for [name] column",
            readNames,
            equalTo(NAME_COLUMN_VALUES));

        assertThat("Source produced different values for [surname] column",
            readSurnames,
            equalTo(SURNAME_COLUMN_VALUES));

        assertThat("Source produced different values for [age] column", readAge,
            equalTo(AGE_COLUMN_VALUES));

        // Checking that we don't have more columns.
        assertNoMoreColumns(resultData, 3);
    }

    @ParameterizedTest
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
        assertThat("Source read different number of rows that Delta Table have.",
            totalNumberOfRows,
            CoreMatchers.equalTo(initialTableSize + numberOfTableUpdateBulks * rowsPerTableUpdate)
        );

        assertThat("Source Produced Different Rows that were in Delta Table",
            uniqueValues.size(),
            CoreMatchers.equalTo(initialTableSize + numberOfTableUpdateBulks * rowsPerTableUpdate)
        );
    }

    @Test
    public void testSelectWithWhereFilter() throws Exception {

        // GIVEN
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(false) // streamingMode = false
        );

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
        assertThat("SELECT with WHERE read different number of rows that expected.",
            resultData.size(),
            equalTo(599)
        );

        assertThat("SELECT with WHERE read different unique values for column col1.",
            readCol1Values.size(),
            equalTo(599)
        );

        assertThat(readCol1Values.get(0), equalTo(501L));
        assertThat(readCol1Values.get(readCol1Values.size() - 1), equalTo(1099L));

        // Checking that we don't have more columns.
        assertNoMoreColumns(resultData, 3);
    }

    @Test
    public void testSelectComputedColumns() throws Exception {

        // GIVEN
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(false) // streamingMode = false
        );

        String computedColumnsSchema = ""
            + "col1 BIGINT,"
            + "col2 BIGINT,"
            + "col3 VARCHAR,"
            + "col4 AS col1 * col2";

        // CREATE Source TABLE
        tableEnv.executeSql(
            buildSourceTableSql(nonPartitionedLargeTablePath, computedColumnsSchema)
        );

        // WHEN
        String selectSql = "SELECT col1, col2, col4 FROM sourceTable";

        TableResult tableResult = tableEnv.executeSql(selectSql);

        List<Row> results = new ArrayList<>();
        tableResult.await(10, TimeUnit.SECONDS);
        try (CloseableIterator<Row> collect = tableResult.collect()) {
            while (collect.hasNext()) {
                results.add(collect.next());
            }
        }

        assertThat(results.isEmpty(), equalTo(false));
        for (Row row : results) {
            assertThat(row.getField("col4"),
                equalTo((long) row.getField("col1") * (long) row.getField("col2")));
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
            DeltaSourceTableITCase.TEST_SOURCE_TABLE_NAME,
            tablePath
        );
    }
}
