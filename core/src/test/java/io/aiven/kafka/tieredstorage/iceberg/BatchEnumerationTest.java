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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;

import io.aiven.kafka.tieredstorage.iceberg.data.RowSchema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BatchEnumerationTest {
    static final String HEADER1_KEY = "header1";
    static final String HEADER2_KEY = "header2";

    @Test
    void test() throws IOException {
        // TODO test nulls

        // Records 0-2 (batch 0) are not "read".

        // Batch 1.
        final byte batch1Magic = RecordBatch.MAGIC_VALUE_V2;
        final int batch1ByteOffset = 1;
        final CompressionType batch1CompressionType = CompressionType.GZIP;
        final TimestampType batch1TimestampType = TimestampType.CREATE_TIME;
        final long batch1BaseOffset = 3L;
        final long batch1LogAppendTime = 1000L;
        final long batch1ProducerId = 123L;
        final short batch1ProducerEpoch = 1;
        final int batch1BaseSequence = 0;
        final int batch1PartitionLeaderEpoch = 0;

        final GenericData.Record record3 = createRecord(
            3L, 1000000003L, batch1ByteOffset, batch1BaseOffset, batch1PartitionLeaderEpoch, batch1Magic,
            batch1TimestampType, batch1CompressionType,
            batch1LogAppendTime,
            batch1ProducerId,
            batch1ProducerEpoch,
            batch1BaseSequence
        );
        final GenericData.Record record4 = createRecord(
            4L, 1000000004L, batch1ByteOffset, batch1BaseOffset, batch1PartitionLeaderEpoch, batch1Magic,
            batch1TimestampType, batch1CompressionType,
            batch1LogAppendTime,
            batch1ProducerId,
            batch1ProducerEpoch,
            batch1BaseSequence
        );
        final GenericData.Record record5 = createRecord(
            5L, 1000000005L, batch1ByteOffset, batch1BaseOffset, batch1PartitionLeaderEpoch, batch1Magic,
            batch1TimestampType, batch1CompressionType,
            batch1LogAppendTime,
            batch1ProducerId,
            batch1ProducerEpoch,
            0
        );

        // Batch 2.
        final int batch2Magic = RecordBatch.MAGIC_VALUE_V2;
        final int batch2ByteOffset = 2;
        final CompressionType batch2CompressionType = CompressionType.SNAPPY;
        final TimestampType batch2TimestampType = TimestampType.LOG_APPEND_TIME;
        final long batch2BaseOffset = 6L;
        final long batch2LogAppendTime = 2000L;
        final long batch2ProducerId = 456L;
        final short batch2ProducerEpoch = 2;
        final int batch2BaseSequence = 4;
        final int batch2PartitionLeaderEpoch = 2;

        final GenericData.Record record6 = createRecord(
            6L, 0L, batch2ByteOffset, batch2BaseOffset, batch2PartitionLeaderEpoch, batch2Magic,
            batch2TimestampType, batch2CompressionType,
            batch2LogAppendTime,
            batch2ProducerId,
            batch2ProducerEpoch,
            batch2BaseSequence
            // doesn't matter, we use log append time
        );
        final GenericData.Record record7 = createRecord(
            7L, 0L, batch2ByteOffset, batch2BaseOffset, batch2PartitionLeaderEpoch, batch2Magic,
            batch2TimestampType, batch2CompressionType,
            batch2LogAppendTime,
            batch2ProducerId,
            batch2ProducerEpoch,
            batch2BaseSequence
            // doesn't matter, we use log append time
        );
        final GenericData.Record record8 = createRecord(
            8L, 0L, batch2ByteOffset, batch2BaseOffset, batch2PartitionLeaderEpoch, batch2Magic,
            batch2TimestampType, batch2CompressionType,
            batch2LogAppendTime,
            batch2ProducerId,
            batch2ProducerEpoch,
            batch2BaseSequence
            // doesn't matter, we use log append time
        );

        final var multiFileReader = mock(MultiFileReader.class);
        when(multiFileReader.read()).thenReturn(
            record3, record4, record5,
            record6, record7, record8,
            null);

        final RecordBatchGrouper recordBatchGrouper = new RecordBatchGrouper(multiFileReader);
        final StructureProvider structureProvider = mock(StructureProvider.class);
        try (final var batchEnumeration = new BatchEnumeration(recordBatchGrouper, structureProvider, "")) {
            // Batch 1.
            assertThat(batchEnumeration.hasMoreElements()).isTrue();
            final var records1 = MemoryRecords.readableRecords(
                ByteBuffer.wrap(batchEnumeration.nextElement().readAllBytes()));
            final Iterator<MutableRecordBatch> batchIterator1 = records1.batches().iterator();
            assertThat(batchIterator1.hasNext()).isTrue();

            final MutableRecordBatch batch1 = batchIterator1.next();
            assertThat(batchIterator1.hasNext()).isFalse();

            final Iterator<Record> recordIterator1 = batch1.iterator();
            checkRecord(recordIterator1, 3L, 1000000003L, 0);
            checkRecord(recordIterator1, 4L, 1000000004L, 1);
            checkRecord(recordIterator1, 5L, 1000000005L, 2);
            assertThat(recordIterator1.hasNext()).isFalse();

            // Batch 2.
            assertThat(batchEnumeration.hasMoreElements()).isTrue();
            final var records2 = MemoryRecords.readableRecords(
                ByteBuffer.wrap(batchEnumeration.nextElement().readAllBytes()));
            final Iterator<MutableRecordBatch> batchIterator2 = records2.batches().iterator();
            assertThat(batchIterator2.hasNext()).isTrue();

            final MutableRecordBatch batch2 = batchIterator2.next();
            assertThat(batchIterator2.hasNext()).isFalse();

            final Iterator<Record> recordIterator2 = batch2.iterator();
            checkRecord(recordIterator2, 6L, batch2LogAppendTime, 4);
            checkRecord(recordIterator2, 7L, batch2LogAppendTime, 5);
            checkRecord(recordIterator2, 8L, batch2LogAppendTime, 6);
            assertThat(recordIterator2.hasNext()).isFalse();

            assertThat(batchEnumeration.hasMoreElements()).isFalse();
        }
    }

    private static GenericData.Record createRecord(
        final long offset,
        final long timestamp,
        final int batchByteOffset,
        final long batchBaseOffset,
        final int batchPartitionLeaderEpoch,
        final int batchMagic,
        final TimestampType batchTimestampType,
        final CompressionType batchCompressionType,
        final long batchMaxTimestamp,
        final long batchProducerId,
        final int batchProducerEpoch,
        final int batchBaseSequence
    ) {
        final GenericData.Record record = new GenericData.Record(RowSchema.createRowSchema(
            Schema.createRecord("k", "", "", false),
            Schema.createRecord("v", "", "", false)
        ));

        final GenericData.Record kafkaPart = new GenericData.Record(RowSchema.KAFKA);
        kafkaPart.put(RowSchema.Fields.OFFSET, offset);
        kafkaPart.put(RowSchema.Fields.TIMESTAMP, timestamp);
        kafkaPart.put(RowSchema.Fields.BATCH_BYTE_OFFSET, batchByteOffset);
        kafkaPart.put(RowSchema.Fields.BATCH_BASE_OFFSET, batchBaseOffset);
        kafkaPart.put(RowSchema.Fields.BATCH_PARTITION_LEADER_EPOCH, batchPartitionLeaderEpoch);
        kafkaPart.put(RowSchema.Fields.BATCH_MAGIC, batchMagic);
        kafkaPart.put(RowSchema.Fields.BATCH_TIMESTAMP_TYPE, batchTimestampType.id);
        kafkaPart.put(RowSchema.Fields.BATCH_COMPRESSION_TYPE, (int) batchCompressionType.id);
        kafkaPart.put(RowSchema.Fields.BATCH_MAX_TIMESTAMP, batchMaxTimestamp);
        kafkaPart.put(RowSchema.Fields.BATCH_PRODUCER_ID, batchProducerId);
        kafkaPart.put(RowSchema.Fields.BATCH_PRODUCER_EPOCH, batchProducerEpoch);
        kafkaPart.put(RowSchema.Fields.BATCH_BASE_SEQUENCE, batchBaseSequence);
        record.put(RowSchema.Fields.KAFKA, kafkaPart);

        final GenericData.Record header1 = new GenericData.Record(RowSchema.HEADER);
        header1.put(RowSchema.Fields.HEADER_KEY, HEADER1_KEY);
        header1.put(RowSchema.Fields.HEADER_VALUE, header1Value(offset));
        final GenericData.Record header2 = new GenericData.Record(RowSchema.HEADER);
        header2.put(RowSchema.Fields.HEADER_KEY, HEADER2_KEY);
        header2.put(RowSchema.Fields.HEADER_VALUE, header2Value(offset));
        record.put(RowSchema.Fields.HEADERS, List.of(header1, header2));

        record.put(RowSchema.Fields.KEY_RAW, key(offset));
        record.put(RowSchema.Fields.VALUE_RAW, value(offset));

        return record;
    }

    private void checkRecord(final Iterator<Record> recordIterator,
                             final long offset,
                             final long timestamp,
                             final int sequence) {
        assertThat(recordIterator.hasNext()).isTrue();
        final Record record = recordIterator.next();
        assertThat(record.offset()).isEqualTo(offset);
        assertThat(record.sequence()).isEqualTo(sequence);
        assertThat(record.timestamp()).isEqualTo(timestamp);
        assertThat(record.hasKey()).isTrue();
        assertThat(record.key()).isEqualByComparingTo(ByteBuffer.wrap(key(offset)));
        assertThat(record.hasValue()).isTrue();
        assertThat(record.value()).isEqualByComparingTo(ByteBuffer.wrap(value(offset)));
        assertThat(record.headers()).containsExactly(
            new RecordHeader(HEADER1_KEY, header1Value(offset)),
            new RecordHeader(HEADER2_KEY, header2Value(offset))
        );

        // hasTimestampType, isCompressed, hasMagic aren't needed to be checked in modern records
    }

    private static byte[] header1Value(final long offset) {
        return String.format("header1 value: %d", offset).getBytes();
    }

    private static byte[] header2Value(final long offset) {
        return String.format("header2 value: %d", offset).getBytes();
    }

    private static byte[] key(final long offset) {
        return String.format("key %d", offset).getBytes();
    }

    private static byte[] value(final long offset) {
        return String.format("value %d", offset).getBytes();
    }
}
