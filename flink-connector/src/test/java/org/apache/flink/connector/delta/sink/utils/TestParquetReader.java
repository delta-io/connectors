package org.apache.flink.connector.delta.sink.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.vector.ParquetColumnarRowSplitReader;
import org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;

public class TestParquetReader {

    /**
     * This test method resolves all parquet files in the current snapshot of DeltaLake table, next
     * it reads those files and try to parse every record back as {@link org.apache.flink.types.Row}
     * object. If the parsing doesn't succeed then an exception will be thrown, otherwise the record
     * counter will be incremented and the validation will skip to the next row till the end of the
     * file.
     *
     * @param deltaLog {@link DeltaLog} instance representing table for which the validation should
     *                 be run
     * @return number of read and successfully validated records in the table
     * @throws IOException Thrown when the data cannot be read or writer cannot be instantiated
     */
    public static int readAndValidateAllTableRecords(DeltaLog deltaLog) throws IOException {
        List<AddFile> deltaTableFiles = deltaLog.snapshot().getAllFiles();
        int cumulatedRecords = 0;
        for (AddFile addedFile : deltaTableFiles) {
            Path parquetFilePath = new Path(deltaLog.getPath().toString(), addedFile.getPath());
            cumulatedRecords += TestParquetReader.readAndParseRecords(
                    parquetFilePath, DeltaSinkTestUtils.TestDeltaLakeTable.TEST_ROW_TYPE);
        }
        return cumulatedRecords;
    }

    static int readAndParseRecords(Path parquetFilepath,
                                   RowType rowType) throws IOException {

        ParquetColumnarRowSplitReader reader = getTestParquetReader(
                parquetFilepath,
                rowType
        );

        int recordsRead = 0;
        while (!reader.reachedEnd()) {
            DeltaSinkTestUtils.TestDeltaLakeTable.CONVERTER.toExternal(reader.nextRecord());
            recordsRead++;
        }
        return recordsRead;
    }

    static ParquetColumnarRowSplitReader getTestParquetReader(Path path,
                                                              RowType rowType) throws IOException {
        return ParquetSplitReaderUtil.genPartColumnarRowReader(
                true,
                true,
                DeltaSinkTestUtils.HadoopConfTest.getHadoopConf(),
                rowType.getFieldNames().toArray(new String[0]),
                rowType.getChildren().stream()
                        .map(TypeConversions::fromLogicalToDataType)
                        .toArray(DataType[]::new),
                new HashMap<>(),
                IntStream.range(0, rowType.getFieldCount()).toArray(),
                50,
                path,
                0,
                Long.MAX_VALUE);
    }

}
