package io.delta.flink.internal.table;

import java.util.List;
import java.util.Map;

import io.delta.flink.internal.table.DeltaCatalogTableHelper.DeltaMetastoreTable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;

/**
 * Delta Catalog implementation. This class executes calls to _delta_log for catalog operations such
 * as createTable, getTable etc. This class also prepares, persists and uses data store in metastore
 * using decorated catalog implementation.
 * <p>
 * Catalog operations that are not in scope of Delta Table or do not require _delta_log operations
 * will be handled by {@link CatalogProxy} and {@link BaseCatalog} classes.
 */
public class DeltaCatalog {

    private final String catalogName;

    /**
     * A Flink's {@link Catalog} implementation to which all Metastore related actions will be
     * redirected. The {@link DeltaCatalog} will not call {@link Catalog#open()} on this instance.
     * If it is required to call this method it should be done before passing this reference to
     * {@link DeltaCatalog}.
     */
    private final Catalog decoratedCatalog;

    private final Configuration hadoopConf;

    /**
     * Creates instance of {@link DeltaCatalog} for given decorated catalog and catalog name.
     *
     * @param catalogName         catalog name.
     * @param decoratedCatalog    A Flink's {@link Catalog} implementation to which all Metastore
     *                            related actions will be redirected. The {@link DeltaCatalog} will
     *                            not call {@link Catalog#open()} on this instance. If it is
     *                            required to call this method it should be done before passing this
     *                            reference to {@link DeltaCatalog}.
     * @param hadoopConf The {@link Configuration} object that will be used for {@link
     *                            DeltaLog} initialization.
     */
    DeltaCatalog(String catalogName, Catalog decoratedCatalog, Configuration hadoopConf) {
        this.catalogName = catalogName;
        this.decoratedCatalog = decoratedCatalog;
        this.hadoopConf = hadoopConf;

        checkArgument(
            !StringUtils.isNullOrWhitespaceOnly(catalogName),
            "Catalog name cannot be null or empty."
        );
        checkArgument(decoratedCatalog != null,
            "The decoratedCatalog cannot be null."
        );
        checkArgument(hadoopConf != null,
            "The Hadoop Configuration object - 'hadoopConfiguration' cannot be null."
        );
    }

    /**
     * Creates a new table in metastore and _delta_log if not already exists under given table Path.
     * The information stored in metastore will contain only catalog path (database.tableName) and
     * connector type. DDL options and table schema will be stored in _delta_log.
     * <p>
     * If _delta_log already exists under DDL's table-path option this method will throw an
     * exception if DDL scheme does not match _delta_log schema or DDL options override existing
     * _delta_log table properties or Partition specification defined in `PARTITION BY` does not
     * match _delta_log partition specification.
     * <p>
     * <p>
     * The framework will make sure to call this method with fully validated ResolvedCatalogTable or
     * ResolvedCatalogView.
     *
     * @param catalogTable   the {@link DeltaCatalogBaseTable} with describing new table that should
     *                       be added to the catalog.
     * @param ignoreIfExists specifies behavior when a table or view already exists at the given
     *                       path: if set to false, it throws a TableAlreadyExistException, if set
     *                       to true, do nothing.
     * @throws TableAlreadyExistException if table already exists and ignoreIfExists is false
     * @throws DatabaseNotExistException  if the database in tablePath doesn't exist
     * @throws CatalogException           in case of any runtime exception
     */
    public void createTable(DeltaCatalogBaseTable catalogTable, boolean ignoreIfExists)
        throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

        checkNotNull(catalogTable);
        ObjectPath tableCatalogPath = catalogTable.getTableCatalogPath();
        // First we need to check if table exists in Metastore and if so, throw exception.
        if (this.decoratedCatalog.tableExists(tableCatalogPath) && !ignoreIfExists) {
            throw new TableAlreadyExistException(this.catalogName, tableCatalogPath);
        }

        if (!databaseExists(catalogTable.getDatabaseName())) {
            throw new DatabaseNotExistException(
                this.catalogName,
                catalogTable.getDatabaseName()
            );
        }

        // These are taken from the DDL OPTIONS.
        Map<String, String> ddlOptions = catalogTable.getOptions();
        String deltaTablePath = ddlOptions.get(DeltaTableConnectorOptions.TABLE_PATH.key());
        if (StringUtils.isNullOrWhitespaceOnly(deltaTablePath)) {
            throw new CatalogException("Path to Delta table cannot be null or empty.");
        }

        // DDL options validation
        DeltaCatalogTableHelper.validateDdlOptions(ddlOptions);

        // At this point what we should have in ddlOptions are only delta table
        // properties, connector type, table path and arbitrary user-defined table properties.
        // We don't want to store connector type or table path in _delta_log, so we will filter
        // those.
        Map<String, String> filteredDdlOptions =
            DeltaCatalogTableHelper.filterMetastoreDdlOptions(ddlOptions);

        CatalogBaseTable table = catalogTable.getCatalogTable();

        // Get Partition columns from DDL;
        List<String> ddlPartitionColumns = ((CatalogTable) table).getPartitionKeys();

        // Get Delta schema from Flink DDL.
        StructType ddlDeltaSchema =
            DeltaCatalogTableHelper.resolveDeltaSchemaFromDdl((ResolvedCatalogTable) table);

        DeltaLog deltaLog = DeltaLog.forTable(hadoopConf, deltaTablePath);
        if (deltaLog.tableExists()) {
            // Table was not present in metastore however it is present on Filesystem, we have to
            // verify if schema, partition spec and properties stored in _delta_log match with DDL.
            Metadata deltaMetadata = deltaLog.update().getMetadata();

            // Validate ddl schema and partition spec matches _delta_log's.
            DeltaCatalogTableHelper.validateDdlSchemaAndPartitionSpecMatchesDelta(
                deltaTablePath,
                tableCatalogPath,
                ddlPartitionColumns,
                ddlDeltaSchema,
                deltaMetadata
            );

            // Add new properties to Delta's metadata.
            // Throw if DDL Delta table properties override previously defined properties from
            // _delta_log.
            Map<String, String> deltaLogProperties =
                DeltaCatalogTableHelper.prepareDeltaTableProperties(
                    filteredDdlOptions,
                    tableCatalogPath,
                    deltaMetadata,
                    false // allowOverride = false
                );

            // deltaLogProperties will have same properties than original metadata + new one,
            // defined in DDL. In that case we want to update _delta_log metadata.
            if (deltaLogProperties.size() != deltaMetadata.getConfiguration().size()) {
                Metadata updatedMetadata = deltaMetadata.copyBuilder()
                    .configuration(deltaLogProperties)
                    .build();

                // add properties to _delta_log
                DeltaCatalogTableHelper
                    .commitToDeltaLog(
                        deltaLog,
                        updatedMetadata,
                        Operation.Name.SET_TABLE_PROPERTIES
                );
            }

            // Add table to metastore
            DeltaMetastoreTable metastoreTable =
                DeltaCatalogTableHelper.prepareMetastoreTable(table, deltaTablePath);
            this.decoratedCatalog.createTable(tableCatalogPath, metastoreTable, ignoreIfExists);
        } else {
            // Table does not exist on filesystem, we have to create a new _delta_log
            Metadata metadata = Metadata.builder()
                .schema(ddlDeltaSchema)
                .partitionColumns(ddlPartitionColumns)
                .configuration(filteredDdlOptions)
                .name(tableCatalogPath.getObjectName())
                .build();

            // create _delta_log
            DeltaCatalogTableHelper.commitToDeltaLog(
                deltaLog,
                metadata,
                Operation.Name.CREATE_TABLE
            );

            DeltaMetastoreTable metastoreTable =
                DeltaCatalogTableHelper.prepareMetastoreTable(table, deltaTablePath);

            // add table to metastore
            this.decoratedCatalog.createTable(tableCatalogPath, metastoreTable, ignoreIfExists);
        }
    }

    /**
     * Returns a {@link CatalogBaseTable} identified by the given
     * {@link DeltaCatalogBaseTable#getCatalogTable()}.
     * This method assumes that provided {@link DeltaCatalogBaseTable#getCatalogTable()} table
     * already exists in metastore hence no extra metastore checks will be executed.
     *
     * @throws TableNotExistException if the target does not exist
     */
    public CatalogBaseTable getTable(DeltaCatalogBaseTable catalogTable)
            throws TableNotExistException {
        CatalogBaseTable metastoreTable = catalogTable.getCatalogTable();

        String tablePath =
            metastoreTable.getOptions().get(DeltaTableConnectorOptions.TABLE_PATH.key());

        DeltaLog deltaLog = DeltaLog.forTable(this.hadoopConf, tablePath);
        if (!deltaLog.tableExists()) {
            // TableNotExistException does not accept custom message, but we would like to meet
            // API contracts from Flink's Catalog::getTable interface and throw
            // TableNotExistException but with information that what was missing was _delta_log.
            throw new TableNotExistException(
                this.catalogName,
                catalogTable.getTableCatalogPath(),
                new CatalogException(
                    String.format(
                        "Table %s exists in metastore but _delta_log was not found under path %s",
                        catalogTable.getTableCatalogPath().getFullName(),
                        tablePath
                    )
                )
            );
        }
        Metadata deltaMetadata = deltaLog.update().getMetadata();
        StructType deltaSchema = deltaMetadata.getSchema();
        if (deltaSchema == null) {
            // This should not happen, but if it did for some reason it mens there is something
            // wong with _delta_log.
            throw new CatalogException(String.format(""
                    + "Delta schema is null for table %s and table path %s. Please contact your "
                    + "administrator.",
                catalogTable.getCatalogTable(),
                tablePath
            ));
        }

        Pair<String[], DataType[]> flinkTypesFromDelta =
            DeltaCatalogTableHelper.resolveFlinkTypesFromDelta(deltaSchema);

        return CatalogTable.of(
            Schema.newBuilder()
                .fromFields(flinkTypesFromDelta.getKey(), flinkTypesFromDelta.getValue())
                .build(), // Table Schema is not stored in metastore, we take it from _delta_log.
            metastoreTable.getComment(),
            deltaMetadata.getPartitionColumns(),
            metastoreTable.getOptions()
        );
    }

    /**
     * Checks if _delta_log folder exists for table described by {@link
     * DeltaCatalogBaseTable#getCatalogTable()} metastore entry. This method assumes that table
     * exists in metastore thus not execute any checks there.
     *
     * @return true if _delta_log exists for given {@link DeltaCatalogBaseTable}, false if not.
     */
    public boolean tableExists(DeltaCatalogBaseTable catalogTable) {
        CatalogBaseTable metastoreTable = catalogTable.getCatalogTable();
        String deltaTablePath =
            metastoreTable.getOptions().get(DeltaTableConnectorOptions.TABLE_PATH.key());
        return DeltaLog.forTable(hadoopConf, deltaTablePath).tableExists();
    }

    public void alterTable(DeltaCatalogBaseTable newCatalogTable) throws CatalogException {

        // TODO FlinkSQL_PR_6
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    private boolean databaseExists(String databaseName) {
        return this.decoratedCatalog.databaseExists(databaseName);
    }
}
