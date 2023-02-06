package io.delta.flink.internal.table;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DeltaDynamicTableFactoryTest {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaDynamicTableFactoryTest.class);

    public static final ResolvedSchema SCHEMA =
        ResolvedSchema.of(
            Column.physical("a", DataTypes.STRING()),
            Column.physical("b", DataTypes.INT()),
            Column.physical("c", DataTypes.BOOLEAN()));

    private DeltaDynamicTableFactory tableFactory;

    private Map<String, String> originalEnvVariables;

    @BeforeEach
    public void setUp() {
        this.tableFactory = new DeltaDynamicTableFactory();

        originalEnvVariables = System.getenv();
    }

    @AfterEach
    public void afterEach() {
        CommonTestUtils.setEnv(originalEnvVariables, true);
    }

    @Test
    void shouldLoadHadoopConfFromHadoopHomeEnv() {

        String path = "src/test/resources/hadoop-conf";
        File file = new File(path);
        String confDir = file.getAbsolutePath();

        Map<String, String> options = new HashMap<>();
        options.put("table-path", "file://some/path");
        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, options);

        CommonTestUtils.setEnv(Collections.singletonMap("HADOOP_HOME", confDir), true);

        DeltaDynamicTableSink dynamicTableSink =
            (DeltaDynamicTableSink) tableFactory.createDynamicTableSink(tableContext);

        Configuration actualConf = dynamicTableSink.getConf();
        assertThat(actualConf.get("dummy.property1", "noValue_asDefault"), equalTo("false-value"));
        assertThat(actualConf.get("dummy.property2", "noValue_asDefault"), equalTo("11"));
    }

    @Test
    void shouldLoadHadoopConfFromHadoopConfDirEnv() {

        String path = "src/test/resources/hadoop-conf";
        File file = new File(path);
        String confDir = file.getAbsolutePath();

        Map<String, String> options = new HashMap<>();
        options.put("table-path", "file://some/path");
        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, options);

        CommonTestUtils.setEnv(Collections.singletonMap("HADOOP_CONF_DIR", confDir), true);

        DeltaDynamicTableSink dynamicTableSink =
            (DeltaDynamicTableSink) tableFactory.createDynamicTableSink(tableContext);

        Configuration actualConf = dynamicTableSink.getConf();
        assertThat(actualConf.get("dummy.property1", "noValue_asDefault"), equalTo("false"));
        assertThat(actualConf.get("dummy.property2", "noValue_asDefault"), equalTo("1"));
    }

    @Test
    void shouldValidateMissingTablePathOption() {

        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, Collections.emptyMap());

        ValidationException validationException = assertThrows(
            ValidationException.class,
            () -> tableFactory.createDynamicTableSink(tableContext)
        );

        LOG.info(validationException.getMessage());
    }

    @Test
    void shouldValidateUsedUnexpectedOption() {

        Map<String, String> options = new HashMap<>();
        options.put("table-path", "file://some/path");
        options.put("invalid-Option", "MyTarget");
        Context tableContext = DeltaTestUtils.createTableContext(SCHEMA, options);

        ValidationException validationException = assertThrows(
            ValidationException.class,
            () -> tableFactory.createDynamicTableSink(tableContext)
        );

        LOG.info(validationException.getMessage());
    }
}
