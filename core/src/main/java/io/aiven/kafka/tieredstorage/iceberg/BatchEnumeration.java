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

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Utils;

import io.aiven.kafka.tieredstorage.iceberg.data.RowSchema;

import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchEnumeration implements Enumeration<InputStream>, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(BatchEnumeration.class);

    private final RecordBatchGrouper recordBatchGrouper;
    private final StructureProvider structureProvider;
    private final String topic;

    private List<GenericData.Record> nextBatchBuffer = null;

    public BatchEnumeration(final RecordBatchGrouper recordBatchGrouper,
                            final StructureProvider structureProvider,
                            final String topic) {
        this.recordBatchGrouper = Objects.requireNonNull(recordBatchGrouper, "recordBatchGrouper cannot be null");
        this.structureProvider = structureProvider;
        this.topic = topic;
    }

    @Override
    public boolean hasMoreElements() {
        try {
            return peekNextBatchRecords() != null;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream nextElement() {
        final List<GenericData.Record> records;
        try {
            records = takeNextBatchRecords();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        if (records == null) {
            throw new NoSuchElementException();
        }

        // TODO this won't work with service batches, the batch info must be in segment metadata.
        if (records.isEmpty()) {
            throw new RuntimeException("empty batch");
        }

        final GenericData.Record firstRecord = records.get(0);

        try (final ByteBufferOutputStream os = new ByteBufferOutputStream(512 * 1024)) {
            final MemoryRecordsBuilder builder = new MemoryRecordsBuilder(
                os,
                getBatchMagic(firstRecord),
                getBatchCompressionType(firstRecord),
                getBatchTimestampType(firstRecord),
                getBatchBaseOffset(firstRecord),
                getBatchMaxTimestamp(firstRecord),
                getBatchProducerId(firstRecord),
                getBatchProducerEpoch(firstRecord),
                getBaseSequence(firstRecord),
                false, false,  // TODO change when we support them
                getBatchPartitionLeaderEpoch(firstRecord),
                0  // doesn't matter
            );
            for (final GenericData.Record record : records) {
                builder.append(new SimpleRecord(
                    getKafkaTimestamp(record),
                    getKafkaKey(record),
                    getKafkaValue(record),
                    getKafkaHeaders(record)
                ));
            }

            final RecordBatch batch = builder.build().firstBatch();
            final ByteBuffer buffer = ByteBuffer.allocate(batch.sizeInBytes());
            batch.writeTo(buffer);
            return new ByteArrayInputStream(buffer.array());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<GenericData.Record> peekNextBatchRecords() throws IOException {
        if (nextBatchBuffer == null) {
            nextBatchBuffer = recordBatchGrouper.nextBatch();
        }
        return nextBatchBuffer;
    }

    private List<GenericData.Record> takeNextBatchRecords() throws IOException {
        final var result = peekNextBatchRecords();
        nextBatchBuffer = null;
        return result;
    }

    private long getBatchBaseOffset(final GenericData.Record record) {
        return (long) ((GenericData.Record) record.get(RowSchema.Fields.KAFKA))
            .get(RowSchema.Fields.BATCH_BASE_OFFSET);
    }

    private int getBatchPartitionLeaderEpoch(final GenericData.Record record) {
        return (int) ((GenericData.Record) record.get(RowSchema.Fields.KAFKA))
            .get(RowSchema.Fields.BATCH_PARTITION_LEADER_EPOCH);
    }

    private byte getBatchMagic(final GenericData.Record record) {
        return ((Integer) ((GenericData.Record) record.get(RowSchema.Fields.KAFKA))
            .get(RowSchema.Fields.BATCH_MAGIC)).byteValue();
    }

    private TimestampType getBatchTimestampType(final GenericData.Record record) {
        final int id = (int) ((GenericData.Record) record.get(RowSchema.Fields.KAFKA))
            .get(RowSchema.Fields.BATCH_TIMESTAMP_TYPE);
        return switch (id) {
            case -1 -> TimestampType.NO_TIMESTAMP_TYPE;
            case 0 -> TimestampType.CREATE_TIME;
            case 1 -> TimestampType.LOG_APPEND_TIME;
            default -> throw new RuntimeException("Unknown timestamp type " + id);
        };
    }

    private Compression getBatchCompressionType(final GenericData.Record record) {
        final int id = (int) ((GenericData.Record) record.get(RowSchema.Fields.KAFKA))
            .get(RowSchema.Fields.BATCH_COMPRESSION_TYPE);
        return Compression.of(CompressionType.forId(id)).build();
    }

    private long getBatchMaxTimestamp(final GenericData.Record record) {
        return (long) ((GenericData.Record) record.get(RowSchema.Fields.KAFKA))
            .get(RowSchema.Fields.BATCH_MAX_TIMESTAMP);
    }

    private long getBatchProducerId(final GenericData.Record record) {
        return (long) ((GenericData.Record) record.get(RowSchema.Fields.KAFKA))
            .get(RowSchema.Fields.BATCH_PRODUCER_ID);
    }

    private short getBatchProducerEpoch(final GenericData.Record record) {
        return ((Integer) ((GenericData.Record) record.get(RowSchema.Fields.KAFKA))
            .get(RowSchema.Fields.BATCH_PRODUCER_EPOCH)).shortValue();
    }

    private int getBaseSequence(final GenericData.Record record) {
        return (int) ((GenericData.Record) record.get(RowSchema.Fields.KAFKA))
            .get(RowSchema.Fields.BATCH_BASE_SEQUENCE);
    }

    private long getKafkaTimestamp(final GenericData.Record record) {
        // TODO real timestamp may have different type.
        return (long) ((GenericData.Record) record.get(RowSchema.Fields.KAFKA))
            .get(RowSchema.Fields.TIMESTAMP);
    }

    private Header[] getKafkaHeaders(final GenericData.Record record) {
        final List<GenericData.Record> headers = (List<GenericData.Record>) record.get(RowSchema.Fields.HEADERS);
        final Header[] result = new Header[headers.size()];
        for (int i = 0; i < headers.size(); i++) {
            final var r = headers.get(i);
            final String key = (String) r.get(RowSchema.Fields.HEADER_KEY);
            final byte[] value = (byte[]) r.get(RowSchema.Fields.HEADER_VALUE);
            result[i] = new RecordHeader(key, value);
        }
        return result;
    }

    private ByteBuffer getKafkaKey(final GenericData.Record record) {
        final Object key = record.get(RowSchema.Fields.KEY);
        final Object rawKey = record.get(RowSchema.Fields.KEY_RAW);
        if (key == null && rawKey == null) {
            return null;
        } else if (key != null) {
            return structureProvider.serializeKey(topic, null, key);
        } else {
            return ByteBuffer.wrap((byte[]) rawKey);
        }
    }

    private ByteBuffer getKafkaValue(final GenericData.Record record) {
        final Object value = record.get(RowSchema.Fields.VALUE);
        final Object rawValue = record.get(RowSchema.Fields.VALUE_RAW);
        if (value == null && rawValue == null) {
            return null;
        } else if (value != null) {
            return structureProvider.serializeValue(topic, null, value);
        } else {
            return ByteBuffer.wrap((byte[]) rawValue);
        }
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(recordBatchGrouper, "recordBatchGrouper");
    }
}
