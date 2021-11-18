package io.delta.standalone.example;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Demonstrates how the Delta Standalone library can be used to perform the CONVERT TO DELTA
 * command on a parquet table.
 *
 * To generate your own parquet files for the example, see resources/generateParquet.py
 *
 * To run this example:
 * - cd connectors/examples/standalone-example-convert-to-delta
 * - mvn exec:java -Dexec.mainClass="io.delta.standalone.example.ConvertToDelta"
 *
 * Find the converted table in: target/classes/external/sales
 */
public class ConvertToDelta {

    private static void convertToDelta(Path dataPath, StructType sourceSchema) throws IOException {

        Configuration conf = new Configuration();
        DeltaLog log = DeltaLog.forTable(conf, dataPath);

        if (log.snapshot().getVersion() > -1) {
            System.out.println("The table you are trying to convert is already a delta table");
            return;
        }

        // ---------------------- Generate Commit Files ----------------------

        FileSystem fs = dataPath.getFileSystem(conf);

        // find parquet files
        List<FileStatus> files = Arrays.stream(fs.listStatus(dataPath))
                .filter(f -> f.isFile() && f.getPath().getName().endsWith(".parquet"))
                .collect(Collectors.toList());

        // generate AddFiles
        List<AddFile> addFiles = files.stream().map(file -> {
            return new AddFile(
                    dataPath.toUri().relativize(file.getPath().toUri()).toString(), // path
                    Collections.emptyMap(),                                         // partitionValues
                    file.getLen(),                                                  // size
                    file.getModificationTime(),                                     // modificationTime
                    true,                                                           // dataChange
                    null,                                                           // stats
                    null                                                            // tags
            );
        }).collect(Collectors.toList());

        Metadata metadata = Metadata.builder().schema(sourceSchema).build();

        // ---------------------- Commit To Delta Log ----------------------

        OptimisticTransaction txn = log.startTransaction();
        txn.updateMetadata(metadata);
        txn.commit(addFiles, new Operation(Operation.Name.CONVERT), "local");
    }

    public static void main(String[] args) throws IOException, URISyntaxException {

        // ---------------------- User Configuration (Input) ----------------------

        final String sourcePath = "external/sales";

        final StructType sourceSchema = new StructType()
                .add("year", new IntegerType())
                .add("month", new IntegerType())
                .add("day", new IntegerType())
                .add("sale_id", new StringType())
                .add("customer", new StringType())
                .add("total_cost", new FloatType());

        // ---------------------- Internal File System Configuration ----------------------

        final Path dataPath = new Path(ConvertToDelta.class.getClassLoader().getResource(sourcePath).toURI());

        // -------------------------- Convert Table to Delta ---------------------------

        convertToDelta(dataPath, sourceSchema);

        // ---------------------------- Verify Commit ----------------------------------

        // read from Delta Log
        DeltaLog log = DeltaLog.forTable(new Configuration(), dataPath);
        Snapshot currentSnapshot = log.snapshot();
        StructType schema = currentSnapshot.getMetadata().getSchema();

        System.out.println("current version: " + currentSnapshot.getVersion());

        System.out.println("number data files: " + currentSnapshot.getAllFiles().size());

        System.out.println("data files:");
        CloseableIterator<AddFile> dataFiles = currentSnapshot.scan().getFiles();
        dataFiles.forEachRemaining(file -> System.out.println(file.getPath()));
        dataFiles.close();

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
