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
 * Groups records in batches based on batch identity (base offset, producer ID, and base sequence).
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

        final BatchIdentity currentBatch = batchIdentity(first);
        while (peekNext() != null && batchIdentity(peekNext()).equals(currentBatch)) {
            result.add(takeNext());
        }

        return result;
    }

    /**
     * Extracts the unique identity of a batch from a record.
     * A batch is uniquely identified by its base offset, producer ID, and base sequence.
     */
    private BatchIdentity batchIdentity(final GenericData.Record record) {
        final GenericData.Record kafkaMetadata = (GenericData.Record) record.get(RowSchema.Fields.KAFKA);
        return new BatchIdentity(
            (long) kafkaMetadata.get(RowSchema.Fields.BATCH_BASE_OFFSET),
            (long) kafkaMetadata.get(RowSchema.Fields.BATCH_PRODUCER_ID),
            (int) kafkaMetadata.get(RowSchema.Fields.BATCH_BASE_SEQUENCE)
        );
    }

    /**
     * Represents the unique identity of a Kafka record batch.
     */
    private record BatchIdentity(long baseOffset, long producerId, int baseSequence) {}

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
