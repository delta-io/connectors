package io.delta.flink.source;

import java.util.List;

import io.delta.flink.source.internal.builder.BoundedDeltaSourceBuilder;
import io.delta.flink.source.internal.builder.DeltaBulkFormat;
import io.delta.flink.source.internal.builder.FormatBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;

/**
 * A builder class for {@link DeltaSource} for a stream of {@link RowData}. Created source instance
 * will operate in {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED} mode.
 * <p>
 * For most common use cases use {@link DeltaSource#forBoundedRowData} utility method to instantiate
 * the source. After instantiation of this builder you can either call {@link
 * RowDataBoundedDeltaSourceBuilder#build()} method to get the instance of a {@link DeltaSource} or
 * configure additional options using builder's API.
 */
public class RowDataBoundedDeltaSourceBuilder
    extends BoundedDeltaSourceBuilder<RowData, RowDataBoundedDeltaSourceBuilder> {

    RowDataBoundedDeltaSourceBuilder(
        Path tablePath,
        FormatBuilder<RowData> formatBuilder,
        Configuration hadoopConfiguration) {
        super(tablePath, formatBuilder, hadoopConfiguration);
    }

    //////////////////////////////////////////////////////////
    ///     We have to override methods from base class    ///
    /// to include them in javadoc generated by sbt-unidoc ///
    //////////////////////////////////////////////////////////

    /**
     * Sets value of "versionAsOf" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED}
     * mode only. With this option we can time travel to given {@link io.delta.standalone.Snapshot}
     * version and read from it.
     *
     * <p>
     * This option is mutually exclusive with {@link #timestampAsOf(String)} option.
     *
     * @param snapshotVersion Delta {@link io.delta.standalone.Snapshot} version to time travel to.
     */
    @Override
    public RowDataBoundedDeltaSourceBuilder versionAsOf(long snapshotVersion) {
        return super.versionAsOf(snapshotVersion);
    }

    /**
     * Sets value of "timestampAsOf" option. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED}
     * mode only. With this option we can time travel to the latest {@link
     * io.delta.standalone.Snapshot} that was generated at or before given timestamp.
     * <p>
     * This option is mutually exclusive with {@link #versionAsOf(long)} option.
     *
     * @param snapshotTimestamp The timestamp we should time travel to. Supported formats are:
     *                          <ul>
     *                                <li>2022-02-24</li>
     *                                <li>2022-02-24 04:55:00</li>
     *                                <li>2022-02-24 04:55:00.001</li>
     *                                <li>2022-02-24T04:55:00</li>
     *                                <li>2022-02-24T04:55:00.001</li>
     *                                <li>2022-02-24T04:55:00.001Z</li>
     *                           </ul>
     */
    @Override
    public RowDataBoundedDeltaSourceBuilder timestampAsOf(String snapshotTimestamp) {
        return super.timestampAsOf(snapshotTimestamp);
    }

    /**
     * Sets list of Delta's partition columns.
     */
    public RowDataBoundedDeltaSourceBuilder partitions(List<String> partitions) {
        return super.partitions(partitions);
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option {@link String} value to set.
     */
    @Override
    public RowDataBoundedDeltaSourceBuilder option(String optionName, String optionValue) {
        return super.option(optionName, optionValue);
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option boolean value to set.
     */
    @Override
    public RowDataBoundedDeltaSourceBuilder option(String optionName, boolean optionValue) {
        return super.option(optionName, optionValue);
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option int value to set.
     */
    @Override
    public RowDataBoundedDeltaSourceBuilder option(String optionName, int optionValue) {
        return super.option(optionName, optionValue);
    }

    /**
     * Sets a configuration option.
     *
     * @param optionName  Option name to set.
     * @param optionValue Option long value to set.
     */
    @Override
    public RowDataBoundedDeltaSourceBuilder option(String optionName, long optionValue) {
        return super.option(optionName, optionValue);
    }

    /**
     * Creates an instance of {@link DeltaSource} for a stream of {@link RowData}. Created source
     * will work in Bounded mode, meaning it will read content of configured Delta's snapshot
     * version ignoring all changes done to this table after starting this source.
     *
     * <p>
     * This method can throw {@code DeltaSourceValidationException} in case of invalid arguments
     * passed to Delta source builder.
     *
     * @return New {@link DeltaSource} instance.
     */
    @Override
    @SuppressWarnings("unchecked")
    public DeltaSource<RowData> build() {

        DeltaBulkFormat<RowData> format = validateSourceAndFormat();

        return new DeltaSource<>(
            tablePath,
            format,
            DEFAULT_BOUNDED_SPLIT_ENUMERATOR_PROVIDER,
            hadoopConfiguration,
            sourceConfiguration);
    }
}
