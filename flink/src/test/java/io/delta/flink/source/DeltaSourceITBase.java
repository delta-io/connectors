package io.delta.flink.source;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.delta.flink.utils.ContinuousTestDescriptor;
import io.delta.flink.utils.DeltaTestUtils;
import io.delta.flink.utils.ExecutionITCaseTestConstants;
import io.delta.flink.utils.FailoverType;
import io.delta.flink.utils.RecordCounterToFail.FailCheck;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;
import static io.delta.flink.utils.DeltaTestUtils.buildCluster;
import static io.delta.flink.utils.ExecutionITCaseTestConstants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

public abstract class DeltaSourceITBase extends TestLogger {

    protected static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    protected static final int PARALLELISM = 4;

    protected final MiniClusterWithClientResource miniClusterResource = buildCluster(PARALLELISM);

    /**
     * Schema for this table has only {@link ExecutionITCaseTestConstants#DATA_COLUMN_NAMES}
     * of type {@link ExecutionITCaseTestConstants#DATA_COLUMN_TYPES} columns.
     */
    protected String nonPartitionedTablePath;

    /**
     * Schema for this table contains data columns
     * {@link ExecutionITCaseTestConstants#DATA_COLUMN_NAMES} and col1, col2
     * partition columns. Types of data columns are
     * {@link ExecutionITCaseTestConstants#DATA_COLUMN_TYPES}
     */
    protected String partitionedTablePath;

    /**
     * Schema for this table has only
     * {@link ExecutionITCaseTestConstants#LARGE_TABLE_ALL_COLUMN_NAMES} of type
     * {@link ExecutionITCaseTestConstants#LARGE_TABLE_ALL_COLUMN_TYPES} columns.
     * Column types are long, long, String
     */
    protected String nonPartitionedLargeTablePath;

    public static void beforeAll() throws IOException {
        TMP_FOLDER.create();
    }

    public static void afterAll() {
        TMP_FOLDER.delete();
    }

    public void setup() {
        try {
            miniClusterResource.before();

            nonPartitionedTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
            nonPartitionedLargeTablePath = TMP_FOLDER.newFolder().getAbsolutePath();
            partitionedTablePath = TMP_FOLDER.newFolder().getAbsolutePath();

            DeltaTestUtils.initTestForSourcePartitionedTable(partitionedTablePath);
            DeltaTestUtils.initTestForNonPartitionedTable(nonPartitionedTablePath);
            DeltaTestUtils.initTestForNonPartitionedLargeTable(
                nonPartitionedLargeTablePath);
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    public void after() {
        miniClusterResource.after();
    }

    @Test
    public void testReadPartitionedTableSkippingPartitionColumns() throws Exception {

        // GIVEN, the full schema of used table is {name, surname, age} + col1, col2 as a partition
        // columns. The size of version 0 is two rows.
        DeltaSource<RowData> deltaSource = initSourceForColumns(
            partitionedTablePath,
            DATA_COLUMN_NAMES
        );

        // WHEN
        List<RowData> resultData = testWithPartitions(deltaSource);

        List<String> readNames =
            resultData.stream()
                .map(row -> row.getString(0).toString()).collect(Collectors.toList());

        Set<String> readSurnames =
            resultData.stream().map(row -> row.getString(1).toString()).collect(Collectors.toSet());

        Set<Integer> readAge =
            resultData.stream().map(row -> row.getInt(2)).collect(Collectors.toSet());

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
        assertNoMoreColumns(resultData,3);
    }

    @Test
    public void testReadOnlyPartitionColumns() throws Exception {

        // GIVEN, the full schema of used table is {name, surname, age} + col1, col2 as a partition
        // columns. The size of version 0 is two rows.
        DeltaSource<RowData> deltaSource = initSourceForColumns(
            partitionedTablePath,
            new String[]{"col1", "col2"}
        );

        // WHEN
        List<RowData> resultData = testWithPartitions(deltaSource);

        // THEN
        assertThat("Source read different number of rows that Delta Table have.",
            resultData.size(),
            equalTo(SMALL_TABLE_COUNT));

        // check partition column values
        String col1_partitionValue = "val1";
        String col2_partitionValue = "val2";
        assertAll(() ->
            resultData.forEach(rowData -> {
                    assertPartitionValue(rowData, 0, col1_partitionValue);
                    assertPartitionValue(rowData, 1, col2_partitionValue);
                }
            )
        );

        // Checking that we don't have more columns.
        assertNoMoreColumns(resultData,2);
    }

    @Test
    public void testWithOnePartition() throws Exception {

        // GIVEN, the full schema of used table is {name, surname, age} + col1, col2 as a partition
        // columns. The size of version 0 is two rows.
        DeltaSource<RowData> deltaSource = initSourceForColumns(
            partitionedTablePath,
            new String[]{"surname", "age", "col2"} // sipping [name] column
        );

        // WHEN
        List<RowData> resultData = testWithPartitions(deltaSource);

        Set<String> readSurnames =
            resultData.stream().map(row -> row.getString(0).toString()).collect(Collectors.toSet());

        Set<Integer> readAge =
            resultData.stream().map(row -> row.getInt(1)).collect(Collectors.toSet());

        // THEN
        assertThat("Source read different number of rows that Delta Table have.",
            resultData.size(),
            equalTo(SMALL_TABLE_COUNT));

        // check for column values
        assertThat("Source produced different values for [surname] column",
            readSurnames,
            equalTo(SURNAME_COLUMN_VALUES));

        assertThat("Source produced different values for [age] column", readAge,
            equalTo(AGE_COLUMN_VALUES));

        // check partition column value
        String col2_partitionValue = "val2";
        resultData.forEach(rowData -> assertPartitionValue(rowData, 2, col2_partitionValue));

        // Checking that we don't have more columns.
        assertNoMoreColumns(resultData,3);
    }

    @Test
    public void testWithBothPartitions() throws Exception {

        // GIVEN, the full schema of used table is {name, surname, age} + col1, col2 as a partition
        // columns. The size of version 0 is two rows.
        DeltaSource<RowData> deltaSource = initSourceAllColumns(partitionedTablePath);

        // WHEN
        List<RowData> resultData = testWithPartitions(deltaSource);

        List<String> readNames =
            resultData.stream()
                .map(row -> row.getString(0).toString()).collect(Collectors.toList());

        Set<String> readSurnames =
            resultData.stream().map(row -> row.getString(1).toString()).collect(Collectors.toSet());

        Set<Integer> readAge =
            resultData.stream().map(row -> row.getInt(2)).collect(Collectors.toSet());

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

        // check for partition column values
        String col1_partitionValue = "val1";
        String col2_partitionValue = "val2";

        resultData.forEach(rowData -> {
            assertPartitionValue(rowData, 3, col1_partitionValue);
            assertPartitionValue(rowData, 4, col2_partitionValue);
        });

        // Checking that we don't have more columns.
        assertNoMoreColumns(resultData,5);
    }

    protected abstract DeltaSource<RowData> initSourceAllColumns(String tablePath);

    protected abstract DeltaSource<RowData> initSourceForColumns(
        String tablePath,
        String[] columnNames);

    protected abstract List<RowData> testWithPartitions(DeltaSource<RowData> deltaSource)
        throws Exception;

    /**
     * Base method used for testing {@link DeltaSource} in {@link Boundedness#BOUNDED} mode. This
     * method creates a {@link StreamExecutionEnvironment} and uses provided {@code
     * DeltaSource} instance without any failover.
     *
     * @param source The {@link DeltaSource} that should be used in this test.
     * @param <T>    Type of objects produced by source.
     * @return A {@link List} of produced records.
     */
    protected <T> List<T> testBoundedDeltaSource(DeltaSource<T> source)
        throws Exception {

        // Since we don't do any failover here (used FailoverType.NONE) we don't need any
        // actually FailCheck.
        // We do need to pass the check at least once, to call
        // RecordCounterToFail#continueProcessing.get() hence (FailCheck) integer -> true
        return testBoundedDeltaSource(FailoverType.NONE, source, (FailCheck) integer -> true);
    }

    /**
     * Base method used for testing {@link DeltaSource} in {@link Boundedness#BOUNDED} mode. This
     * method creates a {@link StreamExecutionEnvironment} and uses provided {@code DeltaSource}
     * instance.
     * <p>
     * <p>
     * The created environment can perform a failover after condition described by {@link FailCheck}
     * which is evaluated every record produced by {@code DeltaSource}
     *
     * @param failoverType The {@link FailoverType} type that should be performed for given test
     *                     setup.
     * @param source       The {@link DeltaSource} that should be used in this test.
     * @param failCheck    The {@link FailCheck} condition which is evaluated for every row produced
     *                     by source.
     * @param <T>          Type of objects produced by source.
     * @return A {@link List} of produced records.
     * @implNote For Implementation details please refer to
     * {@link DeltaTestUtils#testBoundedStream(FailoverType,
     * FailCheck, DataStream, MiniClusterWithClientResource)} method.
     */
    protected <T> List<T> testBoundedDeltaSource(FailoverType failoverType, DeltaSource<T> source,
        FailCheck failCheck) throws Exception {

        if (source.getBoundedness() != Boundedness.BOUNDED) {
            throw new RuntimeException(
                "Not using Bounded source in Bounded test setup. This will not work properly.");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));

        DataStream<T> stream =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "delta-source");

        return DeltaTestUtils
            .testBoundedStream(failoverType, failCheck, stream, miniClusterResource);
    }

    /**
     * Base method used for testing {@link DeltaSource} in {@link Boundedness#CONTINUOUS_UNBOUNDED}
     * mode. This method creates a {@link StreamExecutionEnvironment} and uses provided {@code
     * DeltaSource} instance.
     * <p>
     * <p>
     * The created environment can perform a failover after condition described by {@link FailCheck}
     * which is evaluated every record produced by {@code DeltaSource}
     *
     * @param failoverType The {@link FailoverType} type that should be performed for given test
     *                     setup.
     * @param source       The {@link DeltaSource} that should be used in this test.
     * @param failCheck    The {@link FailCheck} condition which is evaluated for every row produced
     *                     by source.
     * @param <T>          Type of objects produced by source.
     * @return A {@link List} of produced records.
     * @implNote For Implementation details please refer to
     * {@link DeltaTestUtils#testContinuousStream(FailoverType, ContinuousTestDescriptor,
     * FailCheck, DataStream, MiniClusterWithClientResource)}
     */
    protected <T> List<List<T>> testContinuousDeltaSource(
            FailoverType failoverType,
            DeltaSource<T> source,
            ContinuousTestDescriptor testDescriptor,
            FailCheck failCheck)
        throws Exception {

        StreamExecutionEnvironment env = prepareStreamingEnvironment(source);

        DataStream<T> stream =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "delta-source");

        return DeltaTestUtils.testContinuousStream(
                failoverType,
                testDescriptor,
                failCheck,
                stream,
                miniClusterResource
        );
    }

    protected void assertPartitionValue(
            RowData rowData,
            int partitionColumnPosition,
            String partitionValue) {
        assertThat(
            "Partition column has a wrong value.",
            rowData.getString(partitionColumnPosition).toString(),
            equalTo(partitionValue)
        );
    }

    protected <T> StreamExecutionEnvironment prepareStreamingEnvironment(DeltaSource<T> source) {
        return prepareStreamingEnvironment(source, PARALLELISM);
    }

    protected <T> StreamExecutionEnvironment prepareStreamingEnvironment(
            DeltaSource<T> source,
            int parallelismLevel) {
        if (source.getBoundedness() != Boundedness.CONTINUOUS_UNBOUNDED) {
            throw new RuntimeException(
                "Not using using Continuous source in Continuous test setup. This will not work "
                    + "properly.");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelismLevel);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.enableCheckpointing(200L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));
        return env;
    }

    private void assertNoMoreColumns(List<RowData> resultData, int columnIndex) {
        resultData.forEach(rowData ->
            assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> rowData.getString(columnIndex),
                "Found row with extra column."
            )
        );
    }
}
