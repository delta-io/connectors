package io.delta.flink.internal.table;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.standalone.Operation;
import io.delta.standalone.Operation.Name;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

class DeltaCatalogTableHelperTest {

    @Test
    public void testCreateTableOperation() {

        Metadata metadata = Metadata.builder()
            .schema(
                new StructType(new StructField[]{new StructField("col1", new StringType())})
            )
            .partitionColumns(Collections.singletonList("col1"))
            .configuration(Collections.singletonMap("customKey", "myVal"))
            .description("test description").build();

        Operation operation =
            DeltaCatalogTableHelper.prepareDeltaLogOperation(Name.CREATE_TABLE, metadata);

        Map<String, String> expectedOperationParameters = new HashMap<>();
        expectedOperationParameters.put("partitionBy", "\"[\\\"col1\\\"]\"");
        expectedOperationParameters.put("description", "\"test description\"");
        expectedOperationParameters.put("properties", "\"{\\\"customKey\\\":\\\"myVal\\\"}\"");
        expectedOperationParameters.put("isManaged", "false");

        assertThat(operation.getParameters())
            .containsExactlyInAnyOrderEntriesOf(expectedOperationParameters);
    }

    @Test
    public void testAlterPropertiesTableOperation() {

        Metadata metadata = Metadata.builder()
            .schema(
                new StructType(new StructField[]{new StructField("col1", new StringType())})
            )
            .partitionColumns(Collections.singletonList("col1"))
            .configuration(Collections.singletonMap("customKey", "myVal"))
            .description("test description").build();

        Operation operation =
            DeltaCatalogTableHelper.prepareDeltaLogOperation(Name.SET_TABLE_PROPERTIES, metadata);

        Map<String, String> expectedOperationParameters =
            Collections.singletonMap("properties", "\"{\\\"customKey\\\":\\\"myVal\\\"}\"");

        assertThat(operation.getParameters())
            .containsExactlyInAnyOrderEntriesOf(expectedOperationParameters);
    }

    @Test
    public void testThrowOnNotCreateTableNorSetTblPropOperation() {

        Metadata metadata = Metadata.builder()
            .schema(
                new StructType(new StructField[]{new StructField("col1", new StringType())})
            )
            .partitionColumns(Collections.singletonList("col1"))
            .configuration(Collections.singletonMap("customKey", "myVal"))
            .description("test description").build();

        CatalogException catalogException = assertThrows(CatalogException.class, () ->
            DeltaCatalogTableHelper.prepareDeltaLogOperation(Name.DELETE, metadata));

        assertThat(catalogException.getMessage())
            .isEqualTo("Trying to use unsupported Delta Operation [DELETE]");

    }
}

