/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.delta.pulsar.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * The parquet file read util.
 */
@Slf4j
public class DeltaParquetReader {
    ParquetFileReader reader;
    int currentRow;
    PageReadStore currentPage;
    RecordReader recordReader;
    MessageType schema;
    List<Type> fields;
    String filePath;

    public DeltaParquetReader() {
        this.currentRow = 0;
        this.reader = null;
        this.currentPage = null;
    }

    /**
     * The parquet file return.
     */
    public static class Parquet {
        private final List<SimpleGroup> data;
        private final List<Type> schema;
        private final MessageType messageType;

        public Parquet(List<SimpleGroup> data, List<Type> schema, MessageType messageType) {
            this.data = data;
            this.schema = schema;
            this.messageType = messageType;
        }

        public MessageType getMessageType() {
            return messageType;
        }

        public List<SimpleGroup> getData() {
            return data;
        }

        public List<Type> getSchema() {
            return schema;
        }
    }

    public void open(String filePath, Configuration conf) throws IOException {
        this.filePath = filePath;
        reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), conf));
    }

    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    public static long getRowNum(String filePath, Configuration conf) throws IOException {
        ParquetFileReader readerNum =
            ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), conf));
        PageReadStore pages;
        long totalRows = 0;
        while ((pages = readerNum.readNextRowGroup()) != null) {
            totalRows += pages.getRowCount();
        }
        readerNum.close();
        return totalRows;
    }

    public static Parquet getTotalParquetData(String filePath, Configuration conf)
        throws IOException {
        List<SimpleGroup> simpleGroups = new ArrayList<>();
        ParquetFileReader reader =
            ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), conf));
        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        List<Type> fields = schema.getFields();
        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
            long rows = pages.getRowCount();
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            RecordReader recordReader =
                columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

            for (int i = 0; i < rows; i++) {
                SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                simpleGroups.add(simpleGroup);
            }
        }
        reader.close();
        return new Parquet(simpleGroups, fields, schema);
    }

    public Parquet readBatch(int maxRowsOneRound) throws IOException {
        List<SimpleGroup> simpleGroups = new ArrayList<SimpleGroup>();
        if (schema == null) {
            schema = reader.getFooter().getFileMetaData().getSchema();
            fields = schema.getFields();
        }

        long totalRows = 0L;
        if (currentPage != null) {
            if (currentRow < currentPage.getRowCount()) {
                for (; currentRow < currentPage.getRowCount() && totalRows < maxRowsOneRound;
                     currentRow++) {
                    SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                    simpleGroups.add(simpleGroup);
                    totalRows++;
                }
                if (totalRows >= maxRowsOneRound) {
                    log.debug("file {} currentpage read total rows {} currentRow {} "
                            + "maxRowsOneRound {} leftNum {}",
                            filePath, totalRows, currentRow, maxRowsOneRound,
                            currentPage.getRowCount() - currentRow);
                    return new Parquet(simpleGroups, fields, schema);
                }
            }
            currentPage = null;
            currentRow = 0;
        }
        while (true) {
            currentPage = reader.readNextRowGroup();

            if (currentPage == null) {
                log.debug("file {} read EOF rows {} maxRowsOneRound {}",
                    filePath, totalRows, maxRowsOneRound);
                if (totalRows == 0) {
                    return null;
                } else {
                    return new Parquet(simpleGroups, fields, schema);
                }
            }
            long rows = currentPage.getRowCount();
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            recordReader =
                columnIO.getRecordReader(currentPage, new GroupRecordConverter(schema));

            for (currentRow = 0;
                 currentRow < rows && totalRows < maxRowsOneRound;
                 currentRow++) {
                SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                simpleGroups.add(simpleGroup);
                totalRows++;
            }
            if (totalRows >= maxRowsOneRound) {
                log.debug("file {} allPage read total rows {} maxRowsOneRound {} leftNum {}",
                        filePath, totalRows, maxRowsOneRound,
                    currentPage.getRowCount() - currentRow);
                return new Parquet(simpleGroups, fields, schema);
            }
        }
    }
}
