package io.delta.flink.internal.table;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.flink.internal.ConnectorUtils;
import io.delta.flink.internal.table.CatalogExceptionHelper.MismatchedDdlOptionAndDeltaTableProperty;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.PhysicalColumn;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

public final class DeltaCatalogTableHelper {

    private DeltaCatalogTableHelper() {}

    public static StructType resolveDeltaSchemaFromDdl(ResolvedCatalogTable table) {

        // contains physical, computed and metadata columns that were defined in DDL
        List<Column> columns = table.getResolvedSchema().getColumns();
        validateNoDuplicateColumns(columns);

        List<String> names = new LinkedList<>();
        List<LogicalType> types = new LinkedList<>();

        for (Column column : columns) {
            // We care only about physical columns. As stated in Flink doc - metadata columns and
            // computed columns are excluded from persisting. Therefore, a computed column cannot
            // be the target of an INSERT INTO statement.
            if (column instanceof PhysicalColumn) {
                names.add(column.getName());
                types.add(column.getDataType().getLogicalType());
            }
        }

        return io.delta.flink.sink.internal.SchemaConverter.toDeltaDataType(
            RowType.of(types.toArray(new LogicalType[0]), names.toArray(new String[0]))
        );
    }

    public static Pair<String[], DataType[]> resolveFlinkTypesFromDelta(StructType tableSchema) {
        StructField[] fields = tableSchema.getFields();
        String[] columnNames = new String[fields.length];
        DataType[] columnTypes = new DataType[fields.length];
        int i = 0;
        for (StructField field : fields) {
            columnNames[i] = field.getName();
            columnTypes[i] = LogicalTypeDataTypeConverter.toDataType(
                io.delta.flink.source.internal.SchemaConverter.toFlinkDataType(field.getDataType(),
                field.isNullable()));
            i++;
        }

        return Pair.of(columnNames, columnTypes);
    }

    public static void validateNoDuplicateColumns(List<Column> columns) {
        final List<String> names =
            columns.stream().map(Column::getName).collect(Collectors.toList());
        final List<String> duplicates =
            names.stream()
                .filter(name -> Collections.frequency(names, name) > 1)
                .distinct()
                .collect(Collectors.toList());
        if (duplicates.size() > 0) {
            throw new CatalogException(
                String.format(
                    "Schema must not contain duplicate column names. Found duplicates: %s",
                    duplicates));
        }
    }

    public static void commitToDeltaLog(
            DeltaLog deltaLog,
            Metadata newMetadata,
            Operation.Name setTableProperties) {

        OptimisticTransaction transaction = deltaLog.startTransaction();
        transaction.updateMetadata(newMetadata);
        Operation opName =
            prepareDeltaLogOperation(setTableProperties, newMetadata);
        transaction.commit(
            Collections.singletonList(newMetadata),
            opName,
            ConnectorUtils.ENGINE_INFO
        );
    }

    /**
     * Prepares {@link Operation} object for current transaction
     *
     * @param opName name of the operation.
     * @param metadata Delta Table Metadata action.
     * @return {@link Operation} object for current transaction.
     */
    public static Operation prepareDeltaLogOperation(Operation.Name opName, Metadata metadata) {
        Map<String, String> operationParameters = new HashMap<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            switch (opName) {
                case CREATE_TABLE:
                    // We need to perform mapping to JSON object twice for partition columns.
                    // First to map the list to string type and then again to make this string
                    // JSON encoded e.g. java array of ["a", "b"] will be mapped as string
                    // "[\"a\",\"c\"]". Delta seems to expect "[]" and "{} rather then [] and {}.
                    operationParameters.put("isManaged", objectMapper.writeValueAsString(false));
                    operationParameters.put("description",
                        objectMapper.writeValueAsString(metadata.getDescription()));
                    operationParameters.put("properties",
                        objectMapper.writeValueAsString(
                            objectMapper.writeValueAsString(metadata.getConfiguration()))
                    );
                    operationParameters.put("partitionBy", objectMapper.writeValueAsString(
                        objectMapper.writeValueAsString(metadata.getPartitionColumns()))
                    );
                    break;
                case SET_TABLE_PROPERTIES:
                    operationParameters.put("properties",
                        objectMapper.writeValueAsString(
                            objectMapper.writeValueAsString(metadata.getConfiguration()))
                    );
                    break;
                default:
                    throw new CatalogException(String.format(
                        "Trying to use unsupported Delta Operation [%s]",
                        opName.name())
                    );
            }

        } catch (JsonProcessingException e) {
            throw new CatalogException("Cannot map object to JSON", e);
        }

        return new Operation(opName, operationParameters, Collections.emptyMap());
    }

    /**
     * Validate DDL Delta table properties if they match properties from _delta_log add new
     * properties to metadata.
     *
     * @param deltaDdlOptions DDL options that should be added to _delta_log.
     * @param tableCatalogPath
     * @param deltaMetadata
     * @return a map of deltaLogProperties that will have same properties than original metadata
     * plus new ones that were defined in DDL.
     */
    public static Map<String, String> prepareDeltaTableProperties(
        Map<String, String> deltaDdlOptions,
        ObjectPath tableCatalogPath,
        Metadata deltaMetadata,
        boolean allowOverride) {

        List<MismatchedDdlOptionAndDeltaTableProperty> invalidDdlOptions = new LinkedList<>();
        Map<String, String> deltaLogProperties = new HashMap<>(deltaMetadata.getConfiguration());
        for (Entry<String, String> ddlOption : deltaDdlOptions.entrySet()) {
            String deltaLogPropertyValue =
                deltaLogProperties.putIfAbsent(ddlOption.getKey(), ddlOption.getValue());

            if (!allowOverride
                && deltaLogPropertyValue != null
                && !deltaLogPropertyValue.equals(ddlOption.getValue())) {
                // _delta_log contains property defined in ddl but with different value.
                invalidDdlOptions.add(
                    new MismatchedDdlOptionAndDeltaTableProperty(
                        ddlOption.getKey(),
                        ddlOption.getValue(),
                        deltaLogPropertyValue
                    )
                );
            }
        }

        if (!invalidDdlOptions.isEmpty()) {
            throw CatalogExceptionHelper.mismatchedDdlOptionAndDeltaTablePropertyException(
                tableCatalogPath,
                invalidDdlOptions
            );
        }
        return deltaLogProperties;
    }

    public static void validateDdlSchemaAndPartitionSpecMatchesDelta(
        String deltaTablePath,
        ObjectPath tableCatalogPath,
        List<String> ddlPartitionColumns,
        StructType ddlDeltaSchema,
        Metadata deltaMetadata) {

        StructType deltaSchema = deltaMetadata.getSchema();
        if (!(ddlDeltaSchema.equals(deltaSchema)
            && ConnectorUtils.listEqualsIgnoreOrder(
            ddlPartitionColumns,
            deltaMetadata.getPartitionColumns()))) {
            throw CatalogExceptionHelper.deltaLogAndDdlSchemaMismatchException(
                tableCatalogPath,
                deltaTablePath,
                deltaMetadata,
                ddlDeltaSchema,
                ddlPartitionColumns
            );
        }
    }

    public static Map<String, String> filterMetastoreDdlOptions(Map<String, String> ddlOptions) {
        return ddlOptions.entrySet().stream()
            .filter(entry ->
                !(entry.getKey().contains(FactoryUtil.CONNECTOR.key())
                    || entry.getKey().contains(DeltaTableConnectorOptions.TABLE_PATH.key()))
            ).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    public static DeltaMetastoreTable prepareMetastoreTable(
            CatalogBaseTable table,
            String deltaTablePath) {

        // Store only path, table name and connector type in metastore.
        // For computed and meta columns are not supported.
        Map<String, String> optionsToStoreInMetastore = new HashMap<>();
        optionsToStoreInMetastore.put(FactoryUtil.CONNECTOR.key(),
            DeltaDynamicTableFactory.IDENTIFIER);
        optionsToStoreInMetastore.put(DeltaTableConnectorOptions.TABLE_PATH.key(),
            deltaTablePath);

        // Prepare catalog table to store in metastore. This table will have only selected
        // options from DDL and an empty schema.
        // Flink's Hive catalog calls CatalogTable::getSchema method (deprecated) and apply null
        // check on the resul.
        // The default implementation for this method returns null, and the DefaultCatalogTable
        // returned by CatalogTable.of( ) does not override it,
        // hence we need to have our own wrapper that will return empty TableSchema when
        // getSchema method is called.
        return new DeltaMetastoreTable(
            CatalogTable.of(
                // by design don't store schema in metastore. Also watermark and primary key will
                // not be stored in metastore and for now it will not be supported by Delta
                // connector SQL.
                Schema.newBuilder().build(),
                table.getComment(),
                Collections.emptyList(),
                optionsToStoreInMetastore
            )
        );
    }

    public static void validateDdlOptions(Map<String, String> ddlOptions) {
        List<String> invalidOptions = new LinkedList<>();
        for (String ddlOption : ddlOptions.keySet()) {

            // validate for Flink Job specific options in DDL
            if (DeltaFlinkJobSpecificOptions.SOURCE_JOB_OPTIONS.contains(ddlOption)) {
                throw CatalogExceptionHelper.jobSpecificOptionInDdlException(ddlOption);
            }

            // validate for Delta log Store config and parquet config.
            if (ddlOption.startsWith("spark.") ||
                ddlOption.startsWith("delta.logStore") ||
                ddlOption.startsWith("io.delta") ||
                ddlOption.startsWith("parquet.")) {
                invalidOptions.add(ddlOption);
            }
        }
        if (!invalidOptions.isEmpty()) {
            throw CatalogExceptionHelper.invalidOptionInDdl(invalidOptions);
        }
    }

    public static class DeltaMetastoreTable implements CatalogTable {

        private final CatalogTable decoratedTable;

        private DeltaMetastoreTable(CatalogTable decoratedTable) {
            this.decoratedTable = decoratedTable;
        }

        @Override
        public boolean isPartitioned() {
            return decoratedTable.isPartitioned();
        }

        @Override
        public List<String> getPartitionKeys() {
            return Collections.emptyList();
        }

        @Override
        public CatalogTable copy(Map<String, String> map) {
            return decoratedTable.copy(map);
        }

        @Override
        public Map<String, String> getOptions() {
            return decoratedTable.getOptions();
        }

        @Override
        public TableSchema getSchema() {
            return TableSchema.builder().build();
        }

        @Override
        public Schema getUnresolvedSchema() {
            return Schema.newBuilder().build();
        }

        @Override
        public String getComment() {
            return decoratedTable.getComment();
        }

        @Override
        public CatalogBaseTable copy() {
            return decoratedTable.copy();
        }

        @Override
        public Optional<String> getDescription() {
            return decoratedTable.getDescription();
        }

        @Override
        public Optional<String> getDetailedDescription() {
            return decoratedTable.getDetailedDescription();
        }
    }

}
