package io.delta.standalone.example;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.types.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.io.FileUtils;

/**
 * To run
 * - cd connectors/examples/standalone-example-convert-to-delta
 * - mvn exec:java -Dexec.mainClass="io.delta.standalone.example.java.ConvertToDelta"
 *
 * Outputs to: target/classes/delta/sales
 */
public class ConvertToDelta {

    private static void convertToDelta(Path sourceTablePath, org.apache.hadoop.fs.Path targetTablePath,
            StructType sourceSchema) throws IOException {

        // ---------------------- Generate Commit Files ----------------------

        // find parquet files
        List<File> files;
        try (Stream<Path> walk = Files.walk(sourceTablePath)) {
            files = walk
                    .filter(Files::isRegularFile)
                    .filter(p -> Pattern.matches(".*\\.parquet", p.toString()))
                    .map(x -> new File(x.toUri()))
                    .collect(Collectors.toList());
        }

        // generate AddFiles
        List<AddFile> addFiles = files.stream().map(file -> {
            return new AddFile(
                    file.toURI().toString(),     // path
                    Collections.emptyMap(),      // partitionValues
                    file.length(),               // size
                    System.currentTimeMillis(),  // modificationTime
                    true,                        // dataChange
                    null,                        // stats
                    null                         // tags
            );
        }).collect(Collectors.toList());

        Metadata metadata = Metadata.builder().schema(sourceSchema).build();

        // ---------------------- Commit To Delta Log ----------------------

        DeltaLog log = DeltaLog.forTable(new Configuration(), targetTablePath);
        OptimisticTransaction txn = log.startTransaction();
        txn.updateMetadata(metadata);
        txn.commit(addFiles, new Operation(Operation.Name.CONVERT), "local");
    }

    public static void main(String[] args) throws IOException, URISyntaxException {

        // ---------------------- User Configuration (Input) ----------------------

        final String sourceTable = "external/sales";

        final String targetTable = "delta/sales";

        final StructType sourceSchema = new StructType()
                .add("year", new IntegerType())
                .add("month", new IntegerType())
                .add("day", new IntegerType())
                .add("sale_id", new StringType())
                .add("customer", new StringType())
                .add("total_cost", new FloatType());

        // ---------------------- Internal File System Configuration ----------------------

        // look for target table
        URL targetURL = ConvertToDelta.class.getClassLoader().getResource(targetTable);
        if (targetURL != null) {
            // target directory exists, empty it
            FileUtils.cleanDirectory(Paths.get(targetURL.toURI()).toFile());
        } else {
            // target directory does not exist, create it (relative to package location)
            Path rootPath = Paths.get(ConvertToDelta.class.getResource("/").toURI());
            FileUtils.forceMkdir(new File(rootPath.toFile(), targetTable));
        }

        final Path sourceTablePath = Paths.get(ConvertToDelta.class.getClassLoader().getResource(sourceTable).toURI());
        final org.apache.hadoop.fs.Path targetTablePath = new org.apache.hadoop.fs.Path(
                Paths.get(ConvertToDelta.class.getClassLoader().getResource(targetTable).toURI()).toUri()
        );

        // -------------------------- Convert Table to Delta ---------------------------
        convertToDelta(sourceTablePath, targetTablePath, sourceSchema);

        // ---------------------------- Verify Commit ----------------------------------

        // read from Delta Log
        DeltaLog log = DeltaLog.forTable(new Configuration(), targetTablePath);
        Snapshot currentSnapshot = log.snapshot();
        StructType schema = currentSnapshot.getMetadata().getSchema();

        System.out.println("current version: " + currentSnapshot.getVersion());

        System.out.println("number data files: " + currentSnapshot.getAllFiles().size());

        System.out.println("data files:");
        currentSnapshot.getAllFiles().forEach(file -> System.out.println(file.getPath()));

        System.out.println("schema: ");
        System.out.println(schema.getTreeString());

        System.out.println("first 5 rows:");
        CloseableIterator<RowRecord> iter = currentSnapshot.open();
        try {
            int i = 0;
            while (iter.hasNext() && i < 5) {
                i++;
                RowRecord row = iter.next();
                int year = row.isNullAt("year") ? null : row.getInt("year");
                int month = row.isNullAt("month") ? null : row.getInt("month");
                int day = row.isNullAt("day") ? null : row.getInt("day");
                String sale_id = row.isNullAt("sale_id") ? null : row.getString("sale_id");
                String customer = row.isNullAt("customer") ? null : row.getString("customer");
                float total_cost = row.isNullAt("total_cost") ? null : row.getFloat("total_cost");
                System.out.println(year + " " + month + " " + day + " " + sale_id + " " + customer + " " + total_cost);
            }
        } finally {
            iter.close();
        }

        System.exit(0); // close inactive threads from Scala.collection.Parallelizable
    }
}
