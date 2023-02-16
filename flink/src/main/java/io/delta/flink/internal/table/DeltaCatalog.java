package io.delta.flink.internal.table;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import static org.apache.flink.util.Preconditions.checkArgument;

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

    private final Catalog decoratedCatalog;

    private final Configuration hadoopConfiguration;

    DeltaCatalog(String catalogName, Catalog decoratedCatalog, Configuration hadoopConfiguration) {
        this.catalogName = catalogName;
        this.decoratedCatalog = decoratedCatalog;
        this.hadoopConfiguration = hadoopConfiguration;

        checkArgument(
            !StringUtils.isNullOrWhitespaceOnly(catalogName),
            "Catalog name cannot be null or empty."
        );
        checkArgument(decoratedCatalog != null,
            "The decoratedCatalog cannot be null."
        );
        checkArgument(hadoopConfiguration != null,
            "The Hadoop Configuration object - 'hadoopConfiguration' cannot be null."
        );
    }

    public CatalogBaseTable getTable(DeltaCatalogBaseTable catalogTable) {
        // TODO FlinkSQL_PR_4
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    public boolean tableExists(DeltaCatalogBaseTable catalogTable) throws CatalogException {
        // TODO FlinkSQL_PR_4
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    public void createTable(DeltaCatalogBaseTable catalogTable, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

        // TODO FlinkSQL_PR_4
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    public void alterTable(DeltaCatalogBaseTable newCatalogTable) throws CatalogException {

        // TODO FlinkSQL_PR_4
        throw new UnsupportedOperationException("Not yet implemented.");
    }
}
