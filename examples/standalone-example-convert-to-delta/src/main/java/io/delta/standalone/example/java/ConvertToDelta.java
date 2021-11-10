package io.delta.standalone.example.java;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.*;

import org.apache.hadoop.conf.Configuration;

/**
 * WIP
 *
 * To run
 * - cd connectors/examples/standalone-example-convert-to-delta
 * - mvn exec:java -Dexec.mainClass="io.delta.standalone.example.java.ConvertToDelta"
 *
 * Outputs to: target/classes/delta/sales
 */
public class ConvertToDelta {

    public static void main(String[] args) throws IOException, URISyntaxException {
        // ---------------------- User Configuration ----------------------

        final String sourceTable = "external/sales";

        final String targetTable = "delta/sales";

        final StructType sourceSchema = new StructType()
                .add("year", new IntegerType())
                .add("month", new IntegerType())
                .add("day", new IntegerType())
                .add("sale_id", new StringType())
                .add("customer", new StringType())
                .add("total_cost", new FloatType());

        // ---------------------- Internal Configuration ----------------------
        final Path sourceTablePath = Paths.get(ConvertToDelta.class.getClassLoader().getResource(sourceTable).toURI());
        final org.apache.hadoop.fs.Path targetTablePath = new org.apache.hadoop.fs.Path(
                Paths.get(ConvertToDelta.class.getClassLoader().getResource(targetTable).toURI()).toUri()
        );

        // TODO make the targetTable folder if it doesn't exist, else clear it
        // org.apache.commons.io.FileUtils;
        // File f = new File("/var/www/html/testFolder1");
        // FileUtils.cleanDirectory(f); //clean out directory (this is optional -- but good know)
        // FileUtils.forceDelete(f); //delete directory
        // FileUtils.forceMkdir(f); //create directory

        // ---------------------- Generate Commit Files ----------------------
        List<File> files;
        try (Stream<Path> walk = Files.walk(sourceTablePath)) {
            files = walk
                    .filter(Files::isRegularFile)
                    .map(x -> new File(x.toUri()))
                    .collect(Collectors.toList());
        }

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

        // ---------------------- Commit ----------------------
        DeltaLog log = DeltaLog.forTable(new Configuration(), targetTablePath);
        OptimisticTransaction txn = log.startTransaction();
        txn.updateMetadata(metadata);
        txn.commit(addFiles, new Operation(Operation.Name.CONVERT), "local");

        // ---------------------- Verify Commit ----------------------
    }
}
