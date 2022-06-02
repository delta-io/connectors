package io.delta.flink.source;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.builder.DeltaConfigOption;
import io.delta.flink.source.internal.builder.DeltaSourceBuilderBase;
import io.delta.flink.source.internal.enumerator.supplier.TimestampFormatConverter;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.when;

import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;

@ExtendWith(MockitoExtension.class)
class RowDataBoundedDeltaSourceBuilderTest extends RowDataDeltaSourceBuilderTestBase {

    @AfterEach
    public void afterEach() {
        closeDeltaLogStatic();
    }

    ///////////////////////////////
    //  Bounded-only test cases  //
    ///////////////////////////////

    @Test
    public void shouldCreateSource() {

        when(deltaLog.snapshot()).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        DeltaSource<RowData> boundedSource = DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf())
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.BOUNDED));
    }

    /**
     * Test for versionAsOf passed via builder's versionAsOf(...) method.
     */
    @Test
    public void shouldCreateSourceForVersionAsOfParameter() {
        long versionAsOf = 10L;
        when(deltaLog.getSnapshotForVersionAsOf(versionAsOf)).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);
        DeltaSource<RowData> boundedSource = DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf())
            .versionAsOf(versionAsOf)
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.BOUNDED));
        assertThat(boundedSource.getSourceConfiguration()
            .getValue(DeltaSourceOptions.VERSION_AS_OF), equalTo(versionAsOf));
    }

    /**
     * Test for versionAsOf passed via builder's generic option(...) method.
     * This tests also checks option's value type conversion.
     */
    @Test
    public void shouldCreateSourceForVersionAsOfOption() {
        long versionAsOf = 10L;
        when(deltaLog.getSnapshotForVersionAsOf(versionAsOf)).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);

        String versionAsOfKey = DeltaSourceOptions.VERSION_AS_OF.key();
        List<RowDataBoundedDeltaSourceBuilder> builders = Arrays.asList(
            getBuilderAllColumns().option(versionAsOfKey, 10), // int
            getBuilderAllColumns().option(versionAsOfKey, 10L), // long
            getBuilderAllColumns().option(versionAsOfKey, "10") // string
        );

        assertAll(() -> {
            for (RowDataBoundedDeltaSourceBuilder builder : builders) {
                DeltaSource<RowData> boundedSource = builder.build();

                assertThat(boundedSource, notNullValue());
                assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.BOUNDED));
                assertThat(boundedSource.getSourceConfiguration()
                    .getValue(DeltaSourceOptions.VERSION_AS_OF), equalTo(versionAsOf));
            }
        });
    }

    /**
     * Test for timestampAsOf passed via builder's timestampAsOf(...) method.
     */
    @Test
    public void shouldCreateSourceForTimestampAsOfParameter() {
        String timestamp = "2022-02-24T04:55:00.001";
        long timestampAsOf = TimestampFormatConverter.convertToTimestamp(timestamp);
        when(deltaLog.getSnapshotForTimestampAsOf(timestampAsOf)).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);
        DeltaSource<RowData> boundedSource =DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf())
            .timestampAsOf(timestamp)
            .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.BOUNDED));
        assertThat(boundedSource.getSourceConfiguration()
            .getValue(DeltaSourceOptions.TIMESTAMP_AS_OF), equalTo(timestampAsOf));
    }

    /**
     * Test for timestampAsOf passed via builder's generic option(...) method.
     */
    @Test
    public void shouldCreateSourceForTimestampAsOfOption() {
        long timestampAsOf = TimestampFormatConverter.convertToTimestamp("2022-02-24T04:55:00.001");
        when(deltaLog.getSnapshotForTimestampAsOf(timestampAsOf)).thenReturn(headSnapshot);

        StructField[] schema = {new StructField("col1", new StringType())};
        mockDeltaTableForSchema(schema);
        DeltaSource<RowData> boundedSource =
            getBuilderWithOption(DeltaSourceOptions.TIMESTAMP_AS_OF, timestampAsOf)
                .build();

        assertThat(boundedSource, notNullValue());
        assertThat(boundedSource.getBoundedness(), equalTo(Boundedness.BOUNDED));
        assertThat(boundedSource.getSourceConfiguration()
            .getValue(DeltaSourceOptions.TIMESTAMP_AS_OF), equalTo(timestampAsOf));
    }

    //////////////////////////////////////////////////////////////
    // Overridden parent methods for tests in base parent class //
    //////////////////////////////////////////////////////////////

    @Override
    public Collection<? extends DeltaSourceBuilderBase<?,?>> initBuildersWithInapplicableOptions() {
        return Arrays.asList(
            getBuilderWithOption(DeltaSourceOptions.IGNORE_CHANGES, true),
            getBuilderWithOption(DeltaSourceOptions.IGNORE_DELETES, true),
            getBuilderWithOption(DeltaSourceOptions.UPDATE_CHECK_INTERVAL, 1000L),
            getBuilderWithOption(DeltaSourceOptions.UPDATE_CHECK_INITIAL_DELAY, 1000L),
            getBuilderWithOption(DeltaSourceOptions.STARTING_TIMESTAMP, System.currentTimeMillis()),
            getBuilderWithOption(DeltaSourceOptions.STARTING_VERSION, "Latest")
        );
    }

    @Override
    protected <T> RowDataBoundedDeltaSourceBuilder getBuilderWithOption(
            DeltaConfigOption<T> option,
            T value) {
        RowDataBoundedDeltaSourceBuilder builder =
            DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf()
            );

        return (RowDataBoundedDeltaSourceBuilder) setOptionOnBuilder(option, value, builder);
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderWithNulls() {
        return DeltaSource.forBoundedRowData(
            null,
            null
        );
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderForColumns(String[] columnNames) {
        return DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf()
            )
            .columnNames((columnNames != null) ? Arrays.asList(columnNames) : null);
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderAllColumns() {
        return DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf()
            );
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderWithMutuallyExcludedOptions() {
        return DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf()
            )
            .versionAsOf(10)
            .timestampAsOf("2022-02-24T04:55:00.001");
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder getBuilderWithGenericMutuallyExcludedOptions() {
        return DeltaSource.forBoundedRowData(
                new Path(TABLE_PATH),
                DeltaSinkTestUtils.getHadoopConf()
            )
            .option(DeltaSourceOptions.VERSION_AS_OF.key(), 10)
            .option(
                DeltaSourceOptions.TIMESTAMP_AS_OF.key(),
                TimestampFormatConverter.convertToTimestamp("2022-02-24T04:55:00.001")
            );
    }

    @Override
    protected RowDataBoundedDeltaSourceBuilder
        getBuilderWithNullMandatoryFieldsAndExcludedOption() {
        return DeltaSource.forBoundedRowData(
                null,
                DeltaSinkTestUtils.getHadoopConf()
            )
            .timestampAsOf("2022-02-24T04:55:00.001")
            .option(DeltaSourceOptions.VERSION_AS_OF.key(), 10);
    }


}
