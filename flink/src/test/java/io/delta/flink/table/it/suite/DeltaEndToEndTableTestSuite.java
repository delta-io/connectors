package io.delta.flink.table.it.suite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.rules.TemporaryFolder;
import static io.delta.flink.utils.DeltaTestUtils.buildClusterResourceConfig;
import static io.delta.flink.utils.DeltaTestUtils.getTestStreamEnv;
import static io.delta.flink.utils.DeltaTestUtils.verifyDeltaTable;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.LARGE_TABLE_ALL_COLUMN_NAMES;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.LARGE_TABLE_ALL_COLUMN_TYPES;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class DeltaEndToEndTableTestSuite {

    private static final int PARALLELISM = 2;

    protected static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private String sourceTableDdl;

    @RegisterExtension
    private static final MiniClusterExtension miniClusterResource =  new MiniClusterExtension(
        buildClusterResourceConfig(PARALLELISM)
    );

    private String sinkTablePath;

    @BeforeAll
    public static void beforeAll() throws IOException {
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TEMPORARY_FOLDER.delete();
    }

    @BeforeEach
    public void setUp() {

        // Schema for this table has only
        // {@link ExecutionITCaseTestConstants#LARGE_TABLE_ALL_COLUMN_NAMES} of type
        // {@link ExecutionITCaseTestConstants#LARGE_TABLE_ALL_COLUMN_TYPES} columns.
        // Column types are long, long, String
        String nonPartitionedLargeTablePath;
        try {
            nonPartitionedLargeTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            sinkTablePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
            DeltaTestUtils.initTestForNonPartitionedLargeTable(nonPartitionedLargeTablePath);
            assertThat(sinkTablePath).isNotEqualToIgnoringCase(nonPartitionedLargeTablePath);
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }

        sourceTableDdl = String.format("CREATE TABLE sourceTable ("
                + "col1 BIGINT,"
                + "col2 BIGINT,"
                + "col3 VARCHAR"
                + ") WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            nonPartitionedLargeTablePath);
    }

    @Test
    public void shouldReadAndWriteDeltaTable() throws Exception {

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(false) // streamingMode = false
        );

        setupDeltaCatalog(tableEnv);

        String sinkTableDdl =
            String.format("CREATE TABLE sinkTable ("
                    + "col1 BIGINT,"
                    + "col2 BIGINT,"
                    + "col3 VARCHAR"
                    + ") WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                sinkTablePath);

        readWriteTable(tableEnv, sinkTableDdl);

        // Execute SELECT on sink table and validate TableResult.
        TableResult tableResult = tableEnv.executeSql("SELECT * FROM sinkTable");
        List<Row> result = readRowsFromQuery(tableResult, 1100);
        for (Row row : result) {
            assertThat(row.getField("col1")).isInstanceOf(Long.class);
            assertThat(row.getField("col2")).isInstanceOf(Long.class);
            assertThat(row.getField("col3")).isInstanceOf(String.class);
        }
    }

    /**
     * End-to-End test where Delta sink table is created using Flink's LIKE statement.
     */
    @Test
    public void shouldReadAndWriteDeltaTable_LikeTable() throws Exception {

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(false) // streamingMode = false
        );

        setupDeltaCatalog(tableEnv);

        String sinkTableDdl = String.format(""
                + "CREATE TABLE sinkTable "
                + "WITH ("
                + "'connector' = 'delta',"
                + "'table-path' = '%s'"
                + ") LIKE sourceTable",
            sinkTablePath);

        readWriteTable(tableEnv, sinkTableDdl);

        // Execute SELECT on sink table and validate TableResult.
        TableResult tableResult = tableEnv.executeSql("SELECT * FROM sinkTable");
        List<Row> result = readRowsFromQuery(tableResult, 1100);
        for (Row row : result) {
            assertThat(row.getField("col1")).isInstanceOf(Long.class);
            assertThat(row.getField("col2")).isInstanceOf(Long.class);
            assertThat(row.getField("col3")).isInstanceOf(String.class);
        }
    }

    /**
     * End-to-End test where Delta sin table is created using Flink's AS SELECT statement.
     */
    @Test
    public void shouldReadAndWriteDeltaTable_AsSelect() throws Exception {

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(false) // streamingMode = false
        );

        setupDeltaCatalog(tableEnv);

        String sinkTableDdl = String.format(""
                + "CREATE TABLE sinkTable "
                + "WITH ("
                + "'connector' = 'delta',"
                + "'table-path' = '%s'"
                + ") AS SELECT * FROM sourceTable",
            sinkTablePath);

        tableEnv.executeSql(this.sourceTableDdl).await(10, TimeUnit.SECONDS);
        tableEnv.executeSql(sinkTableDdl).await(10, TimeUnit.SECONDS);

        RowType rowType = RowType.of(LARGE_TABLE_ALL_COLUMN_TYPES, LARGE_TABLE_ALL_COLUMN_NAMES);
        verifyDeltaTable(this.sinkTablePath, rowType, 1100);

        // Execute SELECT on sink table and validate TableResult.
        TableResult tableResult = tableEnv.executeSql("SELECT * FROM sinkTable");
        List<Row> result = readRowsFromQuery(tableResult, 1100);
        for (Row row : result) {
            assertThat(row.getField("col1")).isInstanceOf(Long.class);
            assertThat(row.getField("col2")).isInstanceOf(Long.class);
            assertThat(row.getField("col3")).isInstanceOf(String.class);
        }
    }

    @Test
    public void shouldWriteAndReadNestedStructures() throws Exception {

        String sourceTableDdl = "CREATE TABLE sourceTable ("
            + " col1 INT,"
            + " col2 ROW <a INT, b INT>"
            + ") WITH ("
            + "'connector' = 'datagen',"
            + "'rows-per-second' = '1',"
            + "'fields.col1.kind' = 'sequence',"
            + "'fields.col1.start' = '1',"
            + "'fields.col1.end' = '5'"
            + ")";

        String deltaSinkTableDdl =
            String.format("CREATE TABLE deltaSinkTable ("
                    + " col1 INT,"
                    + " col2 ROW <a INT, b INT>"
                    + ") WITH ("
                    + " 'connector' = 'delta',"
                    + " 'table-path' = '%s'"
                    + ")",
                sinkTablePath);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            getTestStreamEnv(true) // streamingMode = false
        );

        setupDeltaCatalog(tableEnv);

        tableEnv.executeSql(sourceTableDdl).await(10, TimeUnit.SECONDS);
        tableEnv.executeSql(deltaSinkTableDdl).await(10, TimeUnit.SECONDS);

        tableEnv.executeSql("INSERT INTO deltaSinkTable SELECT * FROM sourceTable")
            .await(10, TimeUnit.SECONDS);

        // Execute SELECT on sink table and validate TableResult.
        TableResult tableResult =
            tableEnv.executeSql("SELECT col2.a AS innerA, col2.b AS innerB FROM deltaSinkTable");
        List<Row> result = readRowsFromQuery(tableResult, 5);
        for (Row row : result) {
            assertThat(row.getField("innerA")).isInstanceOf(Integer.class);
            assertThat(row.getField("innerB")).isInstanceOf(Integer.class);
        }
    }

    private void readWriteTable(StreamTableEnvironment tableEnv, String sinkTableDdl)
        throws Exception {

        String selectToInsertSql = "INSERT INTO sinkTable SELECT * FROM sourceTable";
        tableEnv.executeSql(this.sourceTableDdl);
        tableEnv.executeSql(sinkTableDdl);

        tableEnv.executeSql(selectToInsertSql).await(10, TimeUnit.SECONDS);

        RowType rowType = RowType.of(LARGE_TABLE_ALL_COLUMN_TYPES, LARGE_TABLE_ALL_COLUMN_NAMES);
        verifyDeltaTable(this.sinkTablePath, rowType, 1100);
    }

    @NotNull
    private List<Row> readRowsFromQuery(TableResult tableResult, int expectedRowsCount)
        throws Exception {

        List<Row> result = new ArrayList<>();
        try (CloseableIterator<Row> collect = tableResult.collect()) {
            while (collect.hasNext()) {
                result.add(collect.next());
            }
        }

        assertThat(result).hasSize(expectedRowsCount);
        return result;
    }

    public abstract void setupDeltaCatalog(TableEnvironment tableEnv);
}
