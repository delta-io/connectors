package io.delta.flink.internal.table;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.flink.utils.DeltaTestUtils;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.rules.TemporaryFolder;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;

@ExtendWith(MockitoExtension.class)
class DeltaCatalogTest {

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static final boolean ignoreIfExists = false;

    private static final String DATABASE = "default";

    public static final String CATALOG_NAME = "testCatalog";

    private DeltaCatalog deltaCatalog;

    // Resets every test.
    private Map<String, String> ddlOptions;

    @BeforeAll
    public static void beforeAll() throws IOException {
        TEMPORARY_FOLDER.create();
    }

    @AfterAll
    public static void afterAll() {
        TEMPORARY_FOLDER.delete();
    }

    @BeforeEach
    public void setUp() throws IOException {
        Catalog decoratedCatalog = new GenericInMemoryCatalog(CATALOG_NAME, DATABASE);
        decoratedCatalog.open();
        this.deltaCatalog = new DeltaCatalog(CATALOG_NAME, decoratedCatalog, new Configuration());
        this.ddlOptions = new HashMap<>();
        this.ddlOptions.put(
            DeltaTableConnectorOptions.TABLE_PATH.key(),
            TEMPORARY_FOLDER.newFolder().getAbsolutePath()
        );
    }

    @ParameterizedTest
    @NullSource  // pass a null value
    @ValueSource(strings = {"", " "})
    public void shouldThrow_createTable_invalidTablePath(String deltaTablePath) {

        DeltaCatalogBaseTable deltaCatalogTable = setUpCatalogTable(
            (deltaTablePath == null) ? Collections.emptyMap() : Collections.singletonMap(
                DeltaTableConnectorOptions.TABLE_PATH.key(),
                deltaTablePath
            )
        );

        CatalogException exception = assertThrows(CatalogException.class, () ->
            deltaCatalog.createTable(deltaCatalogTable, ignoreIfExists)
        );

        assertThat(exception.getMessage())
            .isEqualTo("Path to Delta table cannot be null or empty.");
    }

    @Test
    public void shouldThrow_createTable_invalidTableOption() {

        Map<String, String> invalidOptions = Stream.of(
                "spark.some.option",
                "delta.logStore",
                "io.delta.storage.S3DynamoDBLogStore.ddb.region",
                "parquet.writer.max-padding"
            )
            .collect(Collectors.toMap(optionName -> optionName, s -> "aValue"));

        String expectedValidationMessage = ""
            + "DDL contains invalid properties. DDL can have only delta table properties or "
            + "arbitrary user options only.\n"
            + "Invalid options used:\n"
            + " - 'spark.some.option'\n"
            + " - 'delta.logStore'\n"
            + " - 'io.delta.storage.S3DynamoDBLogStore.ddb.region'\n"
            + " - 'parquet.writer.max-padding'";

        validateCreateTableOptions(invalidOptions, expectedValidationMessage);
    }

    @Test
    public void shouldThrow_createTable_jobSpecificOption() {

        // This test will not check if options are mutual excluded.
        // This is covered by table Factory and Source builder tests.
        Map<String, String> invalidOptions = Stream.of(
                "startingVersion",
                "startingTimestamp",
                "updateCheckIntervalMillis",
                "updateCheckDelayMillis",
                "ignoreDeletes",
                "ignoreChanges",
                "versionAsOf",
                "timestampAsOf",
                // This will be treated as arbitrary user-defined table property and will not be
                // part of the exception message since we don't
                // do case-sensitive checks.
                "TIMESTAMPASOF"
            )
            .collect(Collectors.toMap(optionName -> optionName, s -> "aValue"));

        String expectedValidationMessage = ""
            + "DDL contains invalid properties. DDL can have only delta table properties or "
            + "arbitrary user options only.\n"
            + "DDL contains job specific options. Job specific options can be used only via Query"
            + " hints.\n"
            + "Used Job specific options:\n"
            + " - 'ignoreDeletes'\n"
            + " - 'startingTimestamp'\n"
            + " - 'updateCheckIntervalMillis'\n"
            + " - 'startingVersion'\n"
            + " - 'ignoreChanges'\n"
            + " - 'versionAsOf'\n"
            + " - 'updateCheckDelayMillis'\n"
            + " - 'timestampAsOf'";

        validateCreateTableOptions(invalidOptions, expectedValidationMessage);
    }

    @Test
    public void shouldThrow_createTable_jobSpecificOption_and_invalidTableOptions() {

        // This test will not check if options are mutual excluded.
        // This is covered by table Factory and Source builder tests.
        Map<String, String> invalidOptions = Stream.of(
                "spark.some.option",
                "delta.logStore",
                "io.delta.storage.S3DynamoDBLogStore.ddb.region",
                "parquet.writer.max-padding",
                "startingVersion",
                "startingTimestamp",
                "updateCheckIntervalMillis",
                "updateCheckDelayMillis",
                "ignoreDeletes",
                "ignoreChanges",
                "versionAsOf",
                "timestampAsOf"
            )
            .collect(Collectors.toMap(optionName -> optionName, s -> "aValue"));

        String expectedValidationMessage = ""
            + "DDL contains invalid properties. DDL can have only delta table properties or "
            + "arbitrary user options only.\n"
            + "Invalid options used:\n"
            + " - 'spark.some.option'\n"
            + " - 'delta.logStore'\n"
            + " - 'io.delta.storage.S3DynamoDBLogStore.ddb.region'\n"
            + " - 'parquet.writer.max-padding'\n"
            + "DDL contains job specific options. Job specific options can be used only via Query"
            + " hints.\n"
            + "Used Job specific options:\n"
            + " - 'startingTimestamp'\n"
            + " - 'ignoreDeletes'\n"
            + " - 'updateCheckIntervalMillis'\n"
            + " - 'startingVersion'\n"
            + " - 'ignoreChanges'\n"
            + " - 'versionAsOf'\n"
            + " - 'updateCheckDelayMillis'\n"
            + " - 'timestampAsOf'";

        validateCreateTableOptions(invalidOptions, expectedValidationMessage);
    }

    @Test
    public void shouldThrow_mismatchedDdlOption_and_deltaTableProperty() {

        String tablePath = this.ddlOptions.get(
            DeltaTableConnectorOptions.TABLE_PATH.key()
        );

        Map<String, String> configuration = Collections.singletonMap("delta.appendOnly", "false");

        DeltaLog deltaLog = DeltaTestUtils.setupDeltaTable(
            tablePath,
            configuration,
            Metadata.builder()
                .schema(new StructType(TestTableData.DELTA_FIELDS))
                .build()
        );

        assertThat(deltaLog.tableExists())
            .withFailMessage(
                "There should be Delta table files in test folder before calling DeltaCatalog.")
            .isTrue();

        Map<String, String> mismatchedOptions =
            Collections.singletonMap("delta.appendOnly", "true");


        String expectedValidationMessage = ""
            + "Invalid DDL options for table [default.testTable]. DDL options for Delta table"
            + " connector cannot override table properties already defined in _delta_log.\n"
            + "DDL option name | DDL option value | Delta option value \n"
            + "delta.appendOnly | true | false";

        validateCreateTableOptions(mismatchedOptions, expectedValidationMessage);
    }

    private void validateCreateTableOptions(
        Map<String, String> invalidOptions,
        String expectedValidationMessage) {
        ddlOptions.putAll(invalidOptions);
        DeltaCatalogBaseTable deltaCatalogTable = setUpCatalogTable(
            ddlOptions
        );

        CatalogException exception = assertThrows(CatalogException.class, () ->
            deltaCatalog.createTable(deltaCatalogTable, ignoreIfExists)
        );

        assertThat(exception.getMessage()).isEqualTo(expectedValidationMessage);
    }

    private DeltaCatalogBaseTable setUpCatalogTable(Map<String, String> options) {

        CatalogTable catalogTable = CatalogTable.of(
            Schema.newBuilder()
                .fromFields(TestTableData.COLUMN_NAMES, TestTableData.COLUMN_TYPES)
                .build(),
            "comment",
            Collections.emptyList(), // partitionKeys
            options // options
        );

        return new DeltaCatalogBaseTable(
            new ObjectPath(DATABASE, "testTable"),
            new ResolvedCatalogTable(
                catalogTable,
                ResolvedSchema.physical(TestTableData.COLUMN_NAMES, TestTableData.COLUMN_TYPES)
            )
        );
    }
}
