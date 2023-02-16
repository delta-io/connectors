package io.delta.flink.internal.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * The catalog factory implementation for Delta Catalog. This factory will be discovered by Flink
 * runtime using Java’s Service Provider Interfaces (SPI) based on
 * resources/META-INF/services/org.apache.flink.table.factories.Factory file.
 * <p>
 * Flink runtime will call {@link #createCatalog(Context)} method that will return new Delta Catalog
 * instance.
 */
public class DeltaCatalogFactory implements CatalogFactory {

    /**
     * Property with default database name used for metastore entries.
     */
    public static final ConfigOption<String> DEFAULT_DATABASE =
        ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
            .stringType()
            .defaultValue("default");

    /**
     * Creates and configures a Catalog using the given context
     *
     * @param context {@link Context} object containing catalog properties.
     * @return new instance of Delta Catalog.
     */
    @Override
    public Catalog createCatalog(Context context) {
        // TODO FlinkSQL_PR_6 - inject proper decorated catalog based on catalog properties
        Catalog decoratedCatalog = new GenericInMemoryCatalog(context.getName(), "default");
        Configuration hadoopConfiguration =
            HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
        return new CatalogProxy(context.getName(), "default", decoratedCatalog,
            hadoopConfiguration);
    }

    @Override
    public String factoryIdentifier() {
        return "delta-catalog";
    }

}
