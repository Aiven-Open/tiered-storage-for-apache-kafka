/*
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
 *
 * Copyright 2025 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package io.aiven.kafka.tieredstorage.iceberg.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import com.google.common.collect.Lists;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;

/**
 * The structure, definitions and idea of these defines was taken from
 * <a href="https://github.com/apache/iceberg/blob/main/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/IcebergWriter.java">IcebergWriter</a>
 */
public class IcebergWriter {
    private final Table table;
    private final List<DataFile> writerResults;

    private RecordConverter recordConverter;
    private TaskWriter<Record> writer;

    public IcebergWriter(final Table table) {
        this.table = table;
        this.writerResults = Lists.newArrayList();
        initNewWriter();
    }

    private void initNewWriter() {
        this.writer = TableWriterFactory.createTableWriter(table);
        this.recordConverter = new RecordConverter(table.schema());
    }

    public void write(final Object record) throws RemoteStorageException {
        try {
            final Record row = convertToRow(record);
            writer.write(row);
        } catch (final Exception e) {
            throw new RemoteStorageException("Failed to convert Kafka record into Iceberg", e);
        }
    }

    private Record convertToRow(final Object record) {
        return recordConverter.convert(record, null);
    }

    private void flush() {
        final WriteResult writeResult;
        try {
            writeResult = writer.complete();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }

        writerResults.addAll(List.of(writeResult.dataFiles()));
    }

    public List<DataFile> complete() {
        flush();

        final List<DataFile> result = Lists.newArrayList(writerResults);
        writerResults.clear();

        return result;
    }

    public void close() {
        try {
            writer.close();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
