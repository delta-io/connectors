package io.delta.flink.internal.table;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;

// TODO DC - consider extending CatalogException for more concrete types like
//  "DeltaSchemaMismatchException" etc.
public final class CatalogExceptionHelper {

    private static final String INVALID_PROPERTY_TEMPLATE = " - '%s'";

    private CatalogExceptionHelper() {}

    static CatalogException deltaLogAndDdlSchemaMismatchException(
            ObjectPath catalogTablePath,
            String deltaTablePath,
            Metadata deltaMetadata,
            StructType ddlDeltaSchema,
            List<String> ddlPartitions) {

        String deltaSchemaString = (deltaMetadata.getSchema() == null)
            ? "null"
            : deltaMetadata.getSchema().getTreeString();

        return new CatalogException(
            String.format(
                " Delta table [%s] from filesystem path [%s] has different schema or partition "
                    + "spec that one defined in CREATE TABLE DDL.\n"
                    + "DDL schema:\n[%s],\nDelta table schema:\n[%s]\n"
                    + "DDL partition spec:\n[%s],\nDelta Log partition spec\n[%s]\n",
                catalogTablePath,
                deltaTablePath,
                ddlDeltaSchema.getTreeString(),
                deltaSchemaString,
                ddlPartitions,
                deltaMetadata.getPartitionColumns())
        );
    }

    public static CatalogException mismatchedDdlOptionAndDeltaTablePropertyException(
            ObjectPath catalogTablePath,
            List<MismatchedDdlOptionAndDeltaTableProperty> invalidOptions) {

        StringJoiner invalidOptionsString = new StringJoiner("\n");
        for (MismatchedDdlOptionAndDeltaTableProperty invalidOption : invalidOptions) {
            invalidOptionsString.add(
                String.join(
                    " | ",
                    invalidOption.optionName,
                    invalidOption.ddlOptionValue,
                    invalidOption.deltaLogPropertyValue
                )
            );
        }

        return new CatalogException(
            String.format(
                "Invalid DDL options for table [%s]. "
                    + "DDL options for Delta table connector cannot override table properties "
                    + "already defined in _delta_log.\n"
                    + "DDL option name | DDL option value | Delta option value \n%s",
                catalogTablePath.getFullName(),
                invalidOptionsString
            )
        );
    }

    public static CatalogException unsupportedColumnType(Collection<Column> unsupportedColumns) {
        StringJoiner sj = new StringJoiner("\n");
        for (Column unsupportedColumn : unsupportedColumns) {
            sj.add(
                String.join(
                    " -> ",
                    unsupportedColumn.getName(),
                    unsupportedColumn.getClass().getSimpleName()
                )
            );
        }

        return new CatalogException(String.format(
            "Table definition contains unsupported column types. "
                + "Currently, only physical columns are supported by Delta Flink connector.\n"
                + "Invalid columns and types:\n%s", sj)
        );
    }

    public static CatalogException invalidDdlOptionException(InvalidDdlOptions invalidOptions) {

        String invalidTablePropertiesUsed = invalidOptions.getInvalidTableProperties().stream()
            .map(tableProperty -> String.format(INVALID_PROPERTY_TEMPLATE, tableProperty))
            .collect(Collectors.joining("\n"));

        String usedJobSpecificOptions = invalidOptions.getJobSpecificOptions().stream()
            .map(jobProperty -> String.format(INVALID_PROPERTY_TEMPLATE, jobProperty))
            .collect(Collectors.joining("\n"));

        String exceptionMessage = "DDL contains invalid properties. "
            + "DDL can have only delta table properties or arbitrary user options only.";

        if (invalidTablePropertiesUsed.length() > 0) {
            exceptionMessage = String.join(
                "\n",
                exceptionMessage,
                String.format("Invalid options used:\n%s", invalidTablePropertiesUsed)
            );
        }

        if (usedJobSpecificOptions.length() > 0) {
            exceptionMessage = String.join(
                "\n",
                exceptionMessage,
                String.format(
                    "DDL contains job specific options. Job specific options can be used only via "
                        + "Query hints.\nUsed Job specific options:\n%s", usedJobSpecificOptions)
            );
        }

        return new CatalogException(exceptionMessage);
    }

    /**
     * A container class that contains DDL and _delta_log property values for given DDL option.
     */
    public static class MismatchedDdlOptionAndDeltaTableProperty {

        private final String optionName;

        private final String ddlOptionValue;

        private final String deltaLogPropertyValue;

        public MismatchedDdlOptionAndDeltaTableProperty(
                String optionName,
                String ddlOptionValue,
                String deltaLogPropertyValue) {
            this.optionName = optionName;
            this.ddlOptionValue = ddlOptionValue;
            this.deltaLogPropertyValue = deltaLogPropertyValue;
        }
    }

    public static class InvalidDdlOptions {

        private final Set<String> jobSpecificOptions = new HashSet<>();

        private final Set<String> invalidTableProperties = new HashSet<>();

        public void addJobSpecificOption(String jobSpecificOption) {
            this.jobSpecificOptions.add(jobSpecificOption);
        }

        public void addInvalidTableProperty(String invalidTableProperty) {
            this.invalidTableProperties.add(invalidTableProperty);
        }

        public boolean hasInvalidOptions() {
            return !(jobSpecificOptions.isEmpty() && invalidTableProperties.isEmpty());
        }

        public Collection<String> getJobSpecificOptions() {
            return Collections.unmodifiableSet(this.jobSpecificOptions);
        }

        public Collection<String> getInvalidTableProperties() {
            return Collections.unmodifiableSet(this.invalidTableProperties);
        }
    }

}
