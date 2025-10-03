/*
 * Copyright 2025 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage.iceberg.data;

import java.util.List;
import java.util.Map;

import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.aiven.kafka.tieredstorage.iceberg.IcebergTableManager;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IcebergWriterTest {

    private Table table;
    private IcebergWriter icebergWriter;
    private Schema avroSchema;

    @BeforeEach
    void setUp() {
        avroSchema = SchemaBuilder.record("TestRecord")
                .fields()
                    .name("id").type().longType().noDefault()
                    .name("name").type().unionOf().nullType().and().stringType().endUnion().noDefault()
                    .name("value").type().unionOf().nullType().and().doubleType().endUnion().noDefault()
            .endRecord();

        final InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test-catalog", Map.of());

        final TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");

        table = new IcebergTableManager(catalog).getOrCreateTable(tableId, avroSchema);
    }

    private GenericRecord createAvroRecord(final Long id, final String name, final Double value) {
        final GenericRecord record = new GenericData.Record(avroSchema);
        record.put("id", id);
        record.put("name", name);
        record.put("value", value);
        return record;
    }

    @Test
    void shouldCompleteAndReturnDataFiles() throws RemoteStorageException {
        icebergWriter = new IcebergWriter(table);

        final GenericRecord record1 = createAvroRecord(1L, "record1", 10.0);
        final GenericRecord record2 = createAvroRecord(2L, "record2", 20.0);

        icebergWriter.write(record1);
        icebergWriter.write(record2);

        final List<DataFile> dataFiles = icebergWriter.complete();

        assertThat(dataFiles).hasSize(1);

        final DataFile dataFile = dataFiles.get(0);
        assertThat(dataFile.recordCount()).isEqualTo(2);
        assertThat(dataFile.fileSizeInBytes()).isPositive();
    }

    @Test
    void shouldHandleRecordConversionFailure() {
        icebergWriter = new IcebergWriter(table);

        final Object invalidRecord = "invalid_string_record";

        assertThatThrownBy(() -> icebergWriter.write(invalidRecord))
            .isInstanceOf(RemoteStorageException.class)
            .hasMessage("Failed to convert Kafka record into Iceberg")
            .hasCauseInstanceOf(Exception.class);
    }
}
