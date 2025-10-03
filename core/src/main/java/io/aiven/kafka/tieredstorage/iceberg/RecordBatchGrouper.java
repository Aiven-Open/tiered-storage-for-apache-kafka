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

package io.aiven.kafka.tieredstorage.iceberg;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.common.utils.Utils;

import io.aiven.kafka.tieredstorage.iceberg.data.RowSchema;

import org.apache.avro.generic.GenericData;

/**
 * Groups records in batches based on the {@code kafka.batch_byte_offset} field.
 */
public class RecordBatchGrouper implements Closeable {
    private final MultiFileReader reader;
    private GenericData.Record nextRecordBuffer = null;

    public RecordBatchGrouper(final MultiFileReader reader) {
        this.reader = Objects.requireNonNull(reader, "reader cannot be null");
    }

    List<GenericData.Record> nextBatch() throws IOException {
        if (peekNext() == null) {
            return null;
        }

        final List<GenericData.Record> result = new ArrayList<>();
        final GenericData.Record first = takeNext();
        result.add(first);

        final int currentBatchOffset = batchByteOffset(first);
        while (peekNext() != null && batchByteOffset(peekNext()) == currentBatchOffset) {
            result.add(takeNext());
        }

        return result;
    }

    private int batchByteOffset(final GenericData.Record record) {
        return (int) ((GenericData.Record) record.get(RowSchema.Fields.KAFKA)).get(RowSchema.Fields.BATCH_BYTE_OFFSET);
    }

    GenericData.Record peekNext() throws IOException {
        if (nextRecordBuffer == null) {
            nextRecordBuffer = reader.read();
        }
        return nextRecordBuffer;
    }

    GenericData.Record takeNext() throws IOException {
        final GenericData.Record result = peekNext();
        nextRecordBuffer = null;
        return result;
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(reader, "reader");
    }
}
